"""
app.py — Myrient Archive Browser TUI
Browse and inspect the file directory at https://myrient.erista.me/files/

Usage:
    python app.py

Key bindings:
    ↑ / ↓         Navigate list
    Enter          Open directory / navigate to search result
    Ctrl+F         Open fuzzy search bar
    Space          Toggle selection on highlighted item
    A              Select / deselect all items in current directory
    D              Download selected items (recursive for dirs; prompts size first)
    S              Calculate size of selected item (recursive)
    G              Calculate GRAND TOTAL of entire archive
    R              Refresh current directory
    C              Copy current URL to clipboard
    Backspace      Go up one directory
    Esc            Close search bar
    Q              Quit

Search bar (Ctrl+F):
    Typing instantly fuzzy-filters the current directory.
    Pressing Enter launches a deep recursive search from the current directory.
    Clicking or pressing Enter on a result navigates to its parent folder.
    Space / D work normally on search results to select+download.

Downloads are written to ./downloads/ (mirroring the remote path) by default.
You may run the program with `--dest /some/path` (or `-d`) to change this root.
Already-downloaded files at the correct size are skipped automatically.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Optional

from textual import work, on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.reactive import reactive
from textual.widgets import (
    DataTable,
    Footer,
    Header,
    Input,
    Label,
    Static,
    Tree,
)
from textual.widgets.tree import TreeNode

import scraper
from scraper import Entry, BASE_URL, format_size, fetch_directory

# ── Activity Panel ────────────────────────────────────────────────────────────

_SPINNER_FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"


def _fuzzy_score(query: str, text: str) -> int:
    """
    Return a match score > 0 if *query* fuzzy-matches *text*, else 0.
    Higher score = better match.

    Strategy (in order):
    1. Exact substring                   → 10 000 – len(text)
    2. ALL space-split words present     → 5 000
    3. ANY word present                  → 1 000 per word
    4. ≥70 % of chars appear in order    → 100 + qi*10  (min query len 3)
    """
    q = query.lower().strip()
    t = text.lower()
    if not q:
        return 1
    if q in t:
        return max(1, 10000 - len(t))
    words = q.split()
    if all(w in t for w in words):
        return 5000
    hits = sum(1 for w in words if w in t)
    if hits:
        return 1000 * hits
    # Character-sequence fallback (only for longer queries)
    if len(q) < 3:
        return 0
    qi = 0
    for ch in t:
        if qi < len(q) and ch == q[qi]:
            qi += 1
    if qi >= max(1, int(len(q) * 0.7)):
        return 100 + qi * 10
    return 0


def _prog_bar(done: int, total: int, width: int = 24) -> str:
    """Render a Unicode block progress bar as Rich markup."""
    if total <= 0:
        return f"[dim]{'░' * width}[/]"
    filled = min(int(done / total * width), width)
    pct = int(done / total * 100)
    bar = f"[green]{'█' * filled}[/][dim]{'░' * (width - filled)}[/]"
    return f"{bar} [bold]{pct}%[/]"


class ActivityPanel(Static):
    """
    Collapsible panel pinned above the footer.  Hidden when idle;
    auto-appears when background tasks register themselves.

    Each task is a dict entry:  task_id -> (spinner_idx, label_text)
    Call add_task / update_task / finish_task from workers.
    """

    # task_id -> {"spin": int, "text": str}
    def __init__(self, *args, **kwargs) -> None:
        super().__init__("", *args, **kwargs)
        self._tasks: dict[str, dict] = {}

    def add_task(self, task_id: str, text: str) -> None:
        self._tasks[task_id] = {"spin": 0, "text": text}
        self.display = True
        self._rebuild()

    def update_task(self, task_id: str, text: str) -> None:
        if task_id not in self._tasks:
            return
        d = self._tasks[task_id]
        d["spin"] = (d["spin"] + 1) % len(_SPINNER_FRAMES)
        d["text"] = text
        self._rebuild()

    def finish_task(self, task_id: str) -> None:
        self._tasks.pop(task_id, None)
        if self._tasks:
            self._rebuild()
        else:
            self.update("")
            self.display = False

    def _rebuild(self) -> None:
        lines: list[str] = []
        n = len(self._tasks)
        lines.append(
            f"[bold yellow]⚡ {n} task{'s' if n != 1 else ''} running[/]"
            "  [dim]— the app stays usable, browse freely[/]"
        )
        for d in self._tasks.values():
            spin = _SPINNER_FRAMES[d["spin"]]
            lines.append(f" [cyan]{spin}[/]  {d['text']}")
        self.update("\n".join(lines))


# ── Download Panel ────────────────────────────────────────────────────────────

class DownloadPanel(Static):
    """
    Dedicated download progress panel.  Hidden when no download is running.
    Shows:
      • Overall file-count bar + bytes bar
      • Per-active-file bars with folder label, filename, bytes done / total
      • Skipped / error counters
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__("", *args, **kwargs)
        self._total_files = 0
        self._total_bytes = 0
        self._done_files = 0
        self._done_bytes = 0
        self._skipped = 0
        self._errors = 0
        self._dest_root = ""
        self._active: dict[str, dict] = {}  # key -> {name, folder, done, total, spin}
        self._spin_global = 0
        self._last_rebuild = 0.0

    def start(self, total_files: int, total_bytes: int, dest_root: str) -> None:
        self._total_files = total_files
        self._total_bytes = total_bytes
        self._done_files = 0
        self._done_bytes = 0
        self._skipped = 0
        self._errors = 0
        self._dest_root = dest_root
        self._active = {}
        self._spin_global = 0
        self._last_rebuild = 0.0
        self.display = True
        self._rebuild(force=True)

    def file_start(self, key: str, name: str, folder: str, total_bytes: int) -> None:
        self._active[key] = {
            "name": name,
            "folder": folder,
            "done": 0,
            "total": total_bytes,
            "spin": 0,
        }
        self._rebuild(force=True)

    def file_progress(self, key: str, done_bytes: int, total_bytes: int) -> None:
        """Called synchronously from the download chunk loop."""
        if key not in self._active:
            return
        d = self._active[key]
        delta = done_bytes - d["done"]
        if delta <= 0:
            return
        d["done"] = done_bytes
        if total_bytes > 0:
            d["total"] = total_bytes
        self._done_bytes += delta
        # Throttle redraws: at most ~12 fps, or when ≥512 KiB moved
        now = time.monotonic()
        if delta >= 512 * 1024 or (now - self._last_rebuild) >= 0.083:
            self._spin_global = (self._spin_global + 1) % len(_SPINNER_FRAMES)
            d["spin"] = self._spin_global
            self._rebuild()

    def file_done(self, key: str, skipped: bool = False, error: bool = False) -> None:
        if key in self._active:
            d = self._active.pop(key)
            # Ensure byte counter is accurate when size was unknown
            if not skipped and not error and d["total"] > d["done"]:
                self._done_bytes += d["total"] - d["done"]
        self._done_files += 1
        if skipped:
            self._skipped += 1
        if error:
            self._errors += 1
        self._rebuild(force=True)

    def finish(self) -> None:
        self.update("")
        self.display = False

    def _rebuild(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and (now - self._last_rebuild) < 0.083:
            return
        self._last_rebuild = now

        W = 28   # bar width for overall bars
        Wf = 22  # bar width for per-file bars
        SEP = "[dim]" + "─" * 72 + "[/]"
        lines: list[str] = []

        # ── Overall header ──────────────────────────────────────────────
        done_f = self._done_files
        total_f = self._total_files
        pct_f = int(done_f / total_f * 100) if total_f else 0
        lines.append(
            f"[bold yellow]⬇  DOWNLOADING[/]  "
            f"[bold]{done_f}[/][dim]/{total_f}[/] files  "
            f"{_prog_bar(done_f, total_f, W)}  [bold]{pct_f}%[/]"
        )
        if self._total_bytes > 0:
            pct_b = int(self._done_bytes / self._total_bytes * 100)
            lines.append(
                f"   [dim]data [/]{_prog_bar(self._done_bytes, self._total_bytes, W)}  "
                f"[dim]{format_size(self._done_bytes)} / {format_size(self._total_bytes)}[/]  "
                f"[bold]{pct_b}%[/]"
            )
        lines.append(f"   [dim]→ {self._dest_root}/[/]")
        lines.append(SEP)

        # ── Active files ───────────────────────────────────────────────
        for d in list(self._active.values()):
            spin = _SPINNER_FRAMES[d["spin"]]
            raw = d["name"]
            name = (raw[:52] + "…") if len(raw) > 53 else raw
            folder = f"[dim cyan]{d['folder']}[/]  " if d["folder"] else ""
            if d["total"] > 0:
                pct = int(d["done"] / d["total"] * 100)
                fbar = _prog_bar(d["done"], d["total"], Wf)
                size_str = f"[dim]{format_size(d['done'])} / {format_size(d['total'])}[/]"
                lines.append(f" [cyan]{spin}[/]  {folder}[bold]{name}[/]")
                lines.append(f"      {fbar}  [bold]{pct}%[/]  {size_str}")
            else:
                lines.append(
                    f" [cyan]{spin}[/]  {folder}[bold]{name}[/]  [dim]downloading…[/]"
                )

        # ── Footer counters ─────────────────────────────────────────────
        parts: list[str] = []
        active_n = len(self._active)
        if active_n:
            parts.append(f"[dim]{active_n} active[/]")
        if self._skipped:
            parts.append(f"[dim]{self._skipped} already downloaded[/]")
        if self._errors:
            parts.append(f"[bold red]{self._errors} errors[/]")
        if parts:
            lines.append("   " + "  ·  ".join(parts))

        self.update("\n".join(lines))

# ── Constants ────────────────────────────────────────────────────────────────

PLACEHOLDER = "__loading__"
SHUTDOWN_DATE = "31 March 2026"

# ── Helper: path label from URL ───────────────────────────────────────────────

def url_to_label(url: str) -> str:
    """Strip BASE_URL prefix from URL to get a short display path."""
    if url.startswith(BASE_URL):
        path = url[len(BASE_URL):]
    else:
        path = url.replace("https://myrient.erista.me", "")
    return path.rstrip("/") or "/files"


# ── Status Bar ────────────────────────────────────────────────────────────────

class StatusBar(Static):
    """Bottom status bar showing current path, file counts, and size totals."""

    current_path: reactive[str] = reactive("/files/")
    file_count: reactive[int] = reactive(0)
    dir_count: reactive[int] = reactive(0)
    selected_size: reactive[str] = reactive("—")
    grand_total: reactive[str] = reactive("not calculated")
    status_msg: reactive[str] = reactive("")

    def _build_markup(self) -> str:
        path_part = f"[bold cyan]{self.current_path}[/]"
        counts = f"[green]{self.dir_count}[/] dirs  [yellow]{self.file_count}[/] files"
        sel = f"Selected size: [magenta]{self.selected_size}[/]"
        grand = f"Grand total: [red]{self.grand_total}[/]"
        msg = f"  [dim]{self.status_msg}[/]" if self.status_msg else ""
        return f"{path_part}  \u2502  {counts}  \u2502  {sel}  \u2502  {grand}{msg}"

    def watch_current_path(self, _: str) -> None:
        self.update(self._build_markup())

    def watch_file_count(self, _: int) -> None:
        self.update(self._build_markup())

    def watch_dir_count(self, _: int) -> None:
        self.update(self._build_markup())

    def watch_selected_size(self, _: str) -> None:
        self.update(self._build_markup())

    def watch_grand_total(self, _: str) -> None:
        self.update(self._build_markup())

    def watch_status_msg(self, _: str) -> None:
        self.update(self._build_markup())


# ── Main App ──────────────────────────────────────────────────────────────────

class MyrientBrowser(App):
    """Interactive TUI browser for the Myrient archive."""

    CSS = """
    Screen {
        layout: vertical;
    }

    #main-area {
        layout: horizontal;
        height: 1fr;
    }

    #tree-panel {
        width: 32;
        min-width: 20;
        max-width: 50;
        height: 100%;
        border-right: solid $primary-darken-3;
        background: $surface;
    }

    #tree-title {
        height: 1;
        background: $primary-darken-2;
        color: $text;
        text-align: center;
        padding: 0 1;
    }

    Tree {
        height: 1fr;
        scrollbar-gutter: stable;
    }

    #files-panel {
        width: 1fr;
        height: 100%;
        background: $background;
    }

    #files-title {
        height: 1;
        background: $primary-darken-2;
        color: $text;
        padding: 0 1;
    }

    DataTable {
        height: 1fr;
    }

    StatusBar {
        height: 1;
        background: $surface-darken-1;
        color: $text-muted;
        padding: 0 1;
        border-top: solid $primary-darken-3;
    }

    #warning-bar {
        height: 1;
        background: darkred;
        color: white;
        text-align: center;
        padding: 0 1;
    }

    ActivityPanel {
        height: auto;
        max-height: 6;
        background: $surface-darken-2;
        border-top: solid $accent;
        padding: 0 1;
        display: none;
    }

    DownloadPanel {
        height: auto;
        max-height: 22;
        background: $surface-darken-2;
        border-top: solid $warning;
        padding: 0 1;
        display: none;
    }

    #search-input {
        display: none;
        height: 3;
        border: solid $accent;
        background: $surface;
        padding: 0 1;
    }

    #search-label {
        height: 1;
        background: $accent-darken-2;
        color: $text;
        padding: 0 1;
        display: none;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("ctrl+f", "open_search", "Search"),
        Binding("escape", "close_search", "Close Search", show=False),
        Binding("r", "refresh", "Refresh"),
        Binding("space", "toggle_select", "Select"),
        Binding("a", "select_all", "Sel All"),
        Binding("d", "download", "Download"),
        Binding("s", "calc_size", "Calc Size"),
        Binding("g", "calc_grand_total", "Grand Total"),
        Binding("c", "copy_url", "Copy URL"),
        Binding("backspace", "go_up", "Go Up"),
        Binding("f2", "focus_tree", "Focus Tree", show=False),
        Binding("f3", "focus_table", "Focus Table", show=False),
    ]

    # ── State ─────────────────────────────────────────────────────────────────

    _current_url: str = BASE_URL
    _current_entries: list[Entry] = []
    _selected_entry: Optional[Entry] = None
    _loading: bool = False
    # Multi-select: row keys (str) of items checked with Space
    _selected_keys: set
    # Stored column key for the "Name" column (set in compose)
    _name_col_key: object
    # Destination root for downloads
    _download_dir: str = "downloads"
    # Search state
    _search_mode: bool = False
    _browse_url_saved: str = BASE_URL
    _browse_entries_saved: list  # list[Entry], saved when entering search
    _result_parent_urls: list   # list[str], parallel to _current_entries in search mode

    # ── Layout ────────────────────────────────────────────────────────────────

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)

        # Warning banner
        yield Static(
            f"\u26a0  Myrient shuts down {SHUTDOWN_DATE} \u2014 archive what you need!",
            id="warning-bar",
        )

        with Horizontal(id="main-area"):
            # Left: directory tree
            with Vertical(id="tree-panel"):
                yield Label("  Directory Tree", id="tree-title")
                tree: Tree[str] = Tree("/files/", id="dir-tree")
                tree.root.data = BASE_URL
                tree.root.expand()
                yield tree

            # Right: file listing
            with Vertical(id="files-panel"):
                yield Label(" /files/", id="files-title")
                yield Input(
                    placeholder="  Type to filter  ·  Enter = deep recursive search  ·  Esc = close",
                    id="search-input",
                )
                yield Label("", id="search-label")
                table = DataTable(id="file-table", cursor_type="row")
                col_keys = table.add_columns("Name", "Size", "Date", "Type")
                self._name_col_key = col_keys[0]
                yield table

        yield StatusBar(id="status-bar")
        yield ActivityPanel(id="activity-panel")
        yield DownloadPanel(id="download-panel")
        yield Footer()

    # ── Startup ───────────────────────────────────────────────────────────────

    def on_mount(self) -> None:
        """Load the root directory when the app starts."""
        self._selected_keys = set()
        self._search_mode = False
        self._browse_url_saved = BASE_URL
        self._browse_entries_saved = []
        self._result_parent_urls = []
        self._load_into_tree(self.query_one("#dir-tree", Tree).root)
        self._load_table(BASE_URL)

    # ── Tree events ───────────────────────────────────────────────────────────

    @on(Tree.NodeExpanded)
    def on_tree_node_expanded(self, event: Tree.NodeExpanded) -> None:
        """Lazy-load children when a tree node is expanded."""
        node = event.node
        url = node.data
        if not url:
            return
        # If the node has a single placeholder child, replace with real contents
        children = list(node.children)
        if len(children) == 1 and children[0].data == PLACEHOLDER:
            children[0].remove()
            self._load_into_tree(node)

    @on(Tree.NodeSelected)
    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        """Navigate to the selected directory."""
        url = event.node.data
        if url and url != PLACEHOLDER:
            self._current_url = url
            self._load_table(url)

    # ── Table events ──────────────────────────────────────────────────────────

    @on(DataTable.RowSelected)
    def on_row_selected(self, event: DataTable.RowSelected) -> None:
        """Navigate into a directory when its row is selected in the table."""
        row_key = event.row_key
        if str(row_key.value) == "__parent__":
            self.action_go_up()
            return
        entries = self._current_entries
        idx = None
        for i, e in enumerate(entries):
            if str(i) == str(row_key.value):
                idx = i
                break
        if idx is not None and idx < len(entries):
            entry = entries[idx]
            self._selected_entry = entry
            self._update_status_selection(entry)

            if self._search_mode:
                # Navigate to the result: dirs open the dir, files open their parent
                target_url = entry.url if entry.is_dir else (
                    self._result_parent_urls[idx]
                    if idx < len(self._result_parent_urls)
                    else self._browse_url_saved
                )
                # Exit search mode
                self.query_one("#search-input", Input).display = False
                self.query_one("#search-label", Label).display = False
                self._search_mode = False
                self._result_parent_urls = []
                self._current_url = target_url
                self._load_table(target_url)
                self._sync_tree_selection(target_url)
                return

            if entry.is_dir:
                # Normal browse navigation
                self._current_url = entry.url
                self._load_table(entry.url)
                self._sync_tree_selection(entry.url)

    @on(DataTable.RowHighlighted)
    def on_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        """Update status bar as the cursor moves through rows."""
        row_key = event.row_key
        if row_key is None or str(row_key.value) == "__parent__":
            return
        for i, e in enumerate(self._current_entries):
            if str(i) == str(row_key.value):
                self._selected_entry = e
                self._update_status_selection(e)
                break

    # ── Search input events ───────────────────────────────────────────────────

    @on(Input.Changed, "#search-input")
    def on_search_input_changed(self, event: Input.Changed) -> None:
        """Live-filter current directory entries as the user types."""
        query = event.value.strip()
        if not query:
            # Restore the saved browse view (cached, instant)
            self._search_mode = False
            self._result_parent_urls = []
            self.query_one("#search-label", Label).display = False
            self._load_table(self._browse_url_saved)
            return
        scored = [
            (e, _fuzzy_score(query, e.name))
            for e in self._browse_entries_saved
        ]
        matches = [
            (e, self._browse_url_saved)
            for e, sc in sorted(scored, key=lambda t: t[1], reverse=True)
            if sc > 0
        ]
        self._show_search_results(matches, query)

    @on(Input.Submitted, "#search-input")
    def on_search_input_submitted(self, event: Input.Submitted) -> None:
        """Launch a deep recursive search when Enter is pressed."""
        query = event.value.strip()
        if not query:
            self.action_close_search()
            return
        self._run_deep_search(query, self._browse_url_saved)

    # ── Key Actions ───────────────────────────────────────────────────────────

    def action_refresh(self) -> None:
        """Reload the current directory (bypass cache)."""
        self._load_table(self._current_url, force=True)

    def action_calc_size(self) -> None:
        """Calculate the total size of the selected item."""
        if self._selected_entry and self._selected_entry.is_dir:
            self._start_size_calc(self._selected_entry.url, self._selected_entry.name)
        elif self._selected_entry and not self._selected_entry.is_dir:
            # Single file — size already known
            sb = self.query_one("#status-bar", StatusBar)
            sb.selected_size = self._selected_entry.display_size
            sb.status_msg = f"File: {self._selected_entry.name}"
        else:
            # Calculate size of current directory
            self._start_size_calc(self._current_url, url_to_label(self._current_url))

    def action_calc_grand_total(self) -> None:
        """Calculate the grand total of the entire archive."""
        sb = self.query_one("#status-bar", StatusBar)
        sb.grand_total = "calculating…"
        sb.status_msg = "Scanning entire archive (this may take a while)…"
        self._run_grand_total()

    def action_copy_url(self) -> None:
        """Copy the current URL (or selected item URL) to clipboard."""
        url = BASE_URL
        if self._selected_entry:
            url = self._selected_entry.url
        else:
            url = self._current_url
        try:
            import subprocess
            subprocess.run(["clip"], input=url.encode(), check=True)
            msg = f"Copied: {url}"
        except Exception:
            try:
                import pyperclip  # type: ignore
                pyperclip.copy(url)
                msg = f"Copied: {url}"
            except Exception:
                msg = f"URL: {url}"
        sb = self.query_one("#status-bar", StatusBar)
        sb.status_msg = msg

    def action_focus_tree(self) -> None:
        self.query_one("#dir-tree").focus()

    def action_focus_table(self) -> None:
        self.query_one("#file-table").focus()

    def action_go_up(self) -> None:
        """Navigate to the parent directory."""
        if self._current_url == BASE_URL:
            return
        parent = self._current_url.rstrip("/").rsplit("/", 1)[0] + "/"
        self._current_url = parent
        self._load_table(parent)
        self._sync_tree_selection(parent)

    def action_toggle_select(self) -> None:
        """Toggle the multi-select checkmark on the highlighted row."""
        table = self.query_one("#file-table", DataTable)
        if table.cursor_row is None:
            return
        cell_key = table.coordinate_to_cell_key(table.cursor_coordinate)
        rk = str(cell_key.row_key.value)
        if rk in ("__parent__", "err"):
            return
        idx = int(rk)
        if idx >= len(self._current_entries):
            return
        entry = self._current_entries[idx]
        if rk in self._selected_keys:
            self._selected_keys.discard(rk)
        else:
            self._selected_keys.add(rk)
        is_sel = rk in self._selected_keys
        if self._search_mode and idx < len(self._result_parent_urls):
            name_val = self._make_search_name_cell(
                entry, self._result_parent_urls[idx], is_sel
            )
        else:
            name_val = self._make_name_cell(entry, is_sel)
        table.update_cell(rk, self._name_col_key, name_val)
        self._update_select_status()

    def action_select_all(self) -> None:
        """Toggle: select all items if not all selected, else deselect all."""
        all_keys = {str(i) for i in range(len(self._current_entries))}
        if all_keys.issubset(self._selected_keys):
            self._selected_keys -= all_keys   # deselect all
        else:
            self._selected_keys |= all_keys   # select all
        self._refresh_name_cells()
        self._update_select_status()

    def action_download(self) -> None:
        """Download all selected items (or the highlighted item if nothing selected)."""
        targets: list[Entry] = []
        if self._selected_keys:
            for rk in sorted(self._selected_keys, key=lambda k: int(k)):
                idx = int(rk)
                if idx < len(self._current_entries):
                    targets.append(self._current_entries[idx])
        elif self._selected_entry is not None:
            targets = [self._selected_entry]
        else:
            # Download entire current directory
            targets = list(self._current_entries)

        if not targets:
            self.query_one("#status-bar", StatusBar).status_msg = "Nothing to download."
            return

        self._run_download(targets, self._download_dir)

    def action_open_search(self) -> None:
        """Open the fuzzy search bar."""
        search_input = self.query_one("#search-input", Input)
        if not search_input.display:
            # Save current browse position before entering search mode
            if not self._search_mode:
                self._browse_url_saved = self._current_url
                self._browse_entries_saved = list(self._current_entries)
            search_input.display = True
        search_input.value = ""
        search_input.focus()
        self.query_one("#files-title", Label).update(
            " \U0001f50d Search  [dim](type to filter  \u00b7  Enter = deep search  \u00b7  Esc = close)[/]"
        )

    def action_close_search(self) -> None:
        """Close the search bar and restore the browse view."""
        search_input = self.query_one("#search-input", Input)
        search_label = self.query_one("#search-label", Label)
        search_input.display = False
        search_label.display = False
        if self._search_mode:
            self._search_mode = False
            self._result_parent_urls = []
            # Restore browse state via cached fetch
            self._load_table(self._browse_url_saved)
        else:
            self.query_one("#file-table", DataTable).focus()

    # ── Workers (async background tasks) ─────────────────────────────────────

    @work(exclusive=False, thread=False)
    async def _load_into_tree(self, node: TreeNode) -> None:
        """Fetch directory contents and populate tree node with sub-directories."""
        url: str = node.data
        if not url or url == PLACEHOLDER:
            return
        try:
            entries = await fetch_directory(url)
        except Exception as exc:
            node.add_leaf(f"[red]Error: {exc}[/]", data=None)
            return

        dirs = [e for e in entries if e.is_dir]
        if not dirs:
            node.add_leaf("[dim](empty)[/]", data=None)
            return

        for entry in dirs:
            label = f"\U0001f4c1 {entry.display_name}"
            child = node.add(label, data=entry.url)
            # Add a placeholder so the node shows the expand arrow
            child.add_leaf(PLACEHOLDER, data=PLACEHOLDER)

    @work(exclusive=True, thread=False)
    async def _load_table(self, url: str, force: bool = False) -> None:
        """Fetch directory and populate the DataTable."""
        table = self.query_one("#file-table", DataTable)
        title_label = self.query_one("#files-title", Label)
        sb = self.query_one("#status-bar", StatusBar)

        # Show loading state
        table.clear()
        sb.status_msg = f"Loading {url_to_label(url)} …"

        try:
            entries = await fetch_directory(url, force_refresh=force)
        except Exception as exc:
            table.add_row("Error", str(exc), "", "", key="err")
            sb.status_msg = f"Error: {exc}"
            return

        self._current_entries = entries
        self._current_url = url
        self._selected_keys = set()  # clear selections on navigation
        label_path = url_to_label(url) or "/files"
        title_label.update(f" {label_path}/")
        sb.current_path = label_path + "/"

        # Update breadcrumb in tree label
        dirs = [e for e in entries if e.is_dir]
        files = [e for e in entries if not e.is_dir]
        sb.dir_count = len(dirs)
        sb.file_count = len(files)
        sb.status_msg = ""

        # Add parent navigation row if not at root
        if url != BASE_URL:
            parent_url = url.rstrip("/").rsplit("/", 1)[0] + "/"  # noqa: F841
            table.add_row(
                "[bold dim]\U0001f4c2 ..[/]", "[dim]\u2014[/]", "[dim]\u2014[/]", "[dim]DIR[/]",
                key="__parent__",
            )

        # Fill table rows
        for i, entry in enumerate(entries):
            name_cell = self._make_name_cell(entry, False)

            if entry.is_dir:
                size_cell = "[dim cyan]—[/]"
                type_cell = "[cyan]DIR[/]"
            else:
                sz = entry.size_bytes
                if sz is None:
                    size_cell = "[dim]?[/]"
                else:
                    size_cell = _size_colored(sz)
                ext = entry.name.rsplit(".", 1)[-1].upper() if "." in entry.name else "—"
                type_cell = f"[dim]{ext}[/]"

            table.add_row(
                name_cell,
                size_cell,
                entry.date or "—",
                type_cell,
                key=str(i),
            )

        # Total known file size
        known_bytes = sum(e.size_bytes for e in files if e.size_bytes is not None)
        if known_bytes > 0:
            sb.selected_size = f"{format_size(known_bytes)} (dir total, files only)"
        else:
            sb.selected_size = "—"

        table.focus()

    @work(exclusive=False, thread=False)
    async def _start_size_calc(self, url: str, label: str) -> None:
        """Recursively calculate the total size of a directory."""
        sb = self.query_one("#status-bar", StatusBar)
        panel = self.query_one("#activity-panel", ActivityPanel)
        task_id = f"size_{id(url)}"
        sb.selected_size = "calculating…"
        panel.add_task(task_id, f"[bold]Size calc:[/] {label}  scanning…")

        dirs_scanned = [0]

        async def _progress(scanned_url: str, _bytes: int) -> None:
            dirs_scanned[0] += 1
            panel.update_task(
                task_id,
                f"[bold]Size calc:[/] {label}  "
                f"[dim]{dirs_scanned[0]} dir{'s' if dirs_scanned[0] != 1 else ''} scanned…[/]",
            )

        try:
            total = await scraper.calculate_dir_size(url, progress_callback=_progress)
            sb.selected_size = format_size(total)
            sb.status_msg = f"Size of {label}: {format_size(total)}"
            self.notify(
                f"{label}\n{format_size(total)}",
                title="Size calculated",
                severity="information",
            )
        except Exception as exc:
            sb.selected_size = "error"
            sb.status_msg = f"Size calc error: {exc}"
            self.notify(str(exc), title="Size calc error", severity="error")
        finally:
            panel.finish_task(task_id)

    @work(exclusive=False, thread=False)
    async def _run_grand_total(self) -> None:
        """Calculate the grand total of the entire archive."""
        sb = self.query_one("#status-bar", StatusBar)
        panel = self.query_one("#activity-panel", ActivityPanel)
        task_id = "grand_total"
        panel.add_task(task_id, "[bold]Grand total:[/] scanning entire archive…")

        dirs_scanned = [0]

        async def _progress(scanned_url: str, _bytes: int) -> None:
            dirs_scanned[0] += 1
            panel.update_task(
                task_id,
                f"[bold]Grand total:[/] {dirs_scanned[0]} directories scanned…",
            )

        try:
            total = await scraper.calculate_dir_size(BASE_URL, progress_callback=_progress)
            sb.grand_total = format_size(total)
            sb.status_msg = f"Grand total: {format_size(total)}"
            self.notify(
                f"Total archive size: {format_size(total)}",
                title="Grand total",
                severity="information",
            )
        except Exception as exc:
            sb.grand_total = "error"
            sb.status_msg = f"Grand total error: {exc}"
            self.notify(str(exc), title="Grand total error", severity="error")
        finally:
            panel.finish_task(task_id)

    @work(exclusive=False, thread=False)
    async def _run_download(self, targets: list[Entry], dest_root: str) -> None:
        """Collect files, then stream-download with per-file byte progress."""
        from urllib.parse import unquote, urlparse

        sb = self.query_one("#status-bar", StatusBar)
        panel = self.query_one("#activity-panel", ActivityPanel)
        dl_panel = self.query_one("#download-panel", DownloadPanel)
        task_id = "download"

        panel.add_task(task_id, "[bold]Download:[/] collecting file list…")

        # Expand all targeted dirs into individual file entries
        all_files: list[Entry] = []
        for entry in targets:
            if entry.is_dir:
                panel.update_task(
                    task_id,
                    f"[bold]Download:[/] scanning [italic]{entry.display_name}[/]…",
                )
                sub = await scraper.collect_files(entry.url)
                all_files.extend(sub)
            else:
                all_files.append(entry)

        panel.finish_task(task_id)

        if not all_files:
            sb.status_msg = "No files found to download."
            return

        total_count = len(all_files)
        total_bytes = sum(e.size_bytes for e in all_files if e.size_bytes is not None)
        dl_panel.start(total_count, total_bytes, dest_root)
        sb.status_msg = (
            f"Downloading {total_count} files  "
            f"({format_size(total_bytes)})  → {dest_root}/"
        )

        def _folder_label(url: str) -> str:
            """Return the last 1–2 folder segments as a breadcrumb label."""
            parts = [
                unquote(p)
                for p in urlparse(url).path.split("/")
                if p and p != "files"
            ]
            # parts[-1] is the filename; parts[:-1] are the directories
            dirs = parts[:-1]
            return " › ".join(dirs[-2:]) if dirs else ""

        sem = asyncio.Semaphore(3)  # 3 concurrent downloads
        counters = {"skipped": 0, "errors": 0}

        async def _dl(entry: Entry) -> None:
            key = entry.url
            folder = _folder_label(key)
            async with sem:
                dl_panel.file_start(key, entry.name, folder, entry.size_bytes or 0)
                try:
                    def _progress(done_bytes: int, total_bytes: int) -> None:
                        dl_panel.file_progress(key, done_bytes, total_bytes)

                    was_skipped = await scraper.download_entry(
                        entry, dest_root, progress_callback=_progress
                    )
                    counters["skipped"] += int(was_skipped)
                    dl_panel.file_done(key, skipped=was_skipped, error=False)
                except Exception as exc:
                    counters["errors"] += 1
                    dl_panel.file_done(key, skipped=False, error=True)
                    self.notify(
                        f"Failed: {entry.name}\n{exc}",
                        title="Download error",
                        severity="warning",
                    )

        await asyncio.gather(*[_dl(e) for e in all_files])

        dl_panel.finish()

        n_err = counters["errors"]
        n_skip = counters["skipped"]
        n_new = total_count - n_err - n_skip
        summary = f"{n_new} new"
        if n_skip:
            summary += f"  +  {n_skip} skipped"
        if n_err:
            summary += f"  +  {n_err} errors"
        self.notify(
            f"{summary}\n→ {dest_root}/",
            title="Download complete" if not n_err else "Download finished with errors",
            severity="information" if not n_err else "warning",
        )
        sb.status_msg = f"Done — {summary}  → {dest_root}/"

    @work(exclusive=False, thread=False)
    async def _run_deep_search(self, query: str, base_url: str) -> None:
        """
        Recursively crawl the archive from *base_url* and collect every entry
        whose name fuzzy-matches *query*.  Results are streamed into the table
        via _show_search_results() once the crawl is done.
        """
        panel = self.query_one("#activity-panel", ActivityPanel)
        search_label = self.query_one("#search-label", Label)
        task_id = "search"
        panel.add_task(task_id, f"[bold]Search:[/] scanning for [italic]{query}[/]\u2026")

        results: list[tuple] = []   # (Entry, parent_url)
        visited: set[str] = set()
        dirs_scanned = [0]
        sem = asyncio.Semaphore(8)  # up to 8 concurrent directory fetches

        async def _scan(url: str) -> None:
            if url in visited:
                return
            visited.add(url)
            async with sem:
                dirs_scanned[0] += 1
                panel.update_task(
                    task_id,
                    f"[bold]Search:[/] [italic]{query}[/]  \u2014  "
                    f"{dirs_scanned[0]} dirs, {len(results)} matches\u2026",
                )
                try:
                    entries = await fetch_directory(url)
                except Exception:
                    return
            sub: list = []
            for entry in entries:
                if _fuzzy_score(query, entry.name) > 0:
                    results.append((entry, url))
                if entry.is_dir:
                    sub.append(_scan(entry.url))
            if sub:
                await asyncio.gather(*sub)

        await _scan(base_url)
        panel.finish_task(task_id)

        if not self._search_mode:
            # User pressed Esc before the crawl finished — discard results
            return

        # Sort: files first (ROMs), then directories; within each group by score desc
        results.sort(
            key=lambda t: (t[0].is_dir, -_fuzzy_score(query, t[0].name))
        )
        results = results[:300]  # cap at 300 results

        if results:
            self._show_search_results(results, query)
            self.notify(
                f"{len(results)} result{'s' if len(results) != 1 else ''} for {query!r}",
                severity="information",
            )
        else:
            search_label.update(
                f"[dim]No results for [/][bold]{query!r}[/][dim]  \u2014  Esc to go back[/]"
            )
            search_label.display = True
            self.notify(f"No results for {query!r}", severity="warning")

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _make_name_cell(self, entry: Entry, selected: bool) -> str:
        """Build the Name cell string with optional selection checkmark."""
        icon = "\U0001f4c1" if entry.is_dir else _file_icon(entry.name)
        check = "[bold green]✓[/] " if selected else "  "
        return f"{check}{icon} {entry.display_name}"

    def _make_search_name_cell(self, entry: Entry, parent_url: str, selected: bool) -> str:
        """Build a Name cell for a search result with its directory path prefix."""
        from urllib.parse import urlparse
        icon = "\U0001f4c1" if entry.is_dir else _file_icon(entry.name)
        check = "[bold green]✓[/] " if selected else "  "
        try:
            path = urlparse(parent_url).path.lstrip("/")
            if path.startswith("files/"):
                path = path[len("files/"):]
            path = path.rstrip("/")
        except Exception:
            path = ""
        path_str = f"[dim cyan]{path}/[/] " if path else ""
        return f"{check}{icon} {path_str}{entry.display_name}"

    def _show_search_results(
        self,
        results: list,
        query: str,
    ) -> None:
        """Populate the DataTable with (Entry, parent_url) search results."""
        table = self.query_one("#file-table", DataTable)
        title_label = self.query_one("#files-title", Label)
        search_label = self.query_one("#search-label", Label)
        sb = self.query_one("#status-bar", StatusBar)

        table.clear()
        self._search_mode = True
        self._current_entries = [r[0] for r in results]
        self._result_parent_urls = [r[1] for r in results]
        self._selected_keys = set()

        n = len(results)
        title_label.update(f" \U0001f50d {query!r}  —  {n} result{'s' if n != 1 else ''}")
        search_label.update(
            "[dim]Enter[/]=deep search  [dim]Esc[/]=back  "
            "[dim]Space[/]=select  [dim]D[/]=download  "
            "[dim]Enter on row[/]=navigate to folder"
        )
        search_label.display = True
        sb.current_path = f"\U0001f50d {query!r}"
        sb.file_count = sum(1 for e, _ in results if not e.is_dir)
        sb.dir_count = sum(1 for e, _ in results if e.is_dir)
        sb.status_msg = f"{n} result{'s' if n != 1 else ''} for {query!r}"

        for i, (entry, parent_url) in enumerate(results):
            name_cell = self._make_search_name_cell(entry, parent_url, False)
            if entry.is_dir:
                size_cell = "[dim cyan]—[/]"
                type_cell = "[cyan]DIR[/]"
            else:
                sz = entry.size_bytes
                size_cell = _size_colored(sz) if sz is not None else "[dim]?[/]"
                ext = entry.name.rsplit(".", 1)[-1].upper() if "." in entry.name else "—"
                type_cell = f"[dim]{ext}[/]"
            table.add_row(name_cell, size_cell, entry.date or "—", type_cell, key=str(i))

        table.focus()

    def _refresh_name_cells(self) -> None:
        """Redraw the Name column for every entry row to reflect current selections."""
        table = self.query_one("#file-table", DataTable)
        for i, entry in enumerate(self._current_entries):
            rk = str(i)
            is_sel = rk in self._selected_keys
            if self._search_mode and i < len(self._result_parent_urls):
                name_val = self._make_search_name_cell(
                    entry, self._result_parent_urls[i], is_sel
                )
            else:
                name_val = self._make_name_cell(entry, is_sel)
            try:
                table.update_cell(rk, self._name_col_key, name_val)
            except Exception:
                pass

    def _update_select_status(self) -> None:
        sb = self.query_one("#status-bar", StatusBar)
        n = len(self._selected_keys)
        if n == 0:
            sb.status_msg = ""
        else:
            # Show total known size of selected items
            sel_entries = [
                self._current_entries[int(k)]
                for k in self._selected_keys
                if int(k) < len(self._current_entries)
            ]
            known = sum(e.size_bytes for e in sel_entries if e.size_bytes)
            dirs = sum(1 for e in sel_entries if e.is_dir)
            files = sum(1 for e in sel_entries if not e.is_dir)
            parts = []
            if dirs:
                parts.append(f"{dirs} dir{'s' if dirs != 1 else ''}")
            if files:
                parts.append(f"{files} file{'s' if files != 1 else ''}")
            size_note = f"  ~{format_size(known)}" if known else ""
            sb.status_msg = f"{n} selected ({', '.join(parts)}){size_note}  — press D to download"

    def _update_status_selection(self, entry: Entry) -> None:
        sb = self.query_one("#status-bar", StatusBar)
        if entry.is_dir:
            sb.selected_size = "— (press S to calculate)"
            sb.status_msg = f"Selected: {entry.display_name}/"
        else:
            sb.selected_size = entry.display_size
            sb.status_msg = f"Selected: {entry.name}"

    def _sync_tree_selection(self, url: str) -> None:
        """Try to expand and highlight the tree node matching the navigated URL."""
        # Best-effort: walk the visible tree and expand the matching node
        tree = self.query_one("#dir-tree", Tree)
        self._expand_tree_to(tree.root, url)

    def _expand_tree_to(self, node: TreeNode, target_url: str) -> bool:
        """Recursively expand tree nodes to reach target_url."""
        if node.data == target_url:
            node.expand()
            return True
        for child in node.children:
            if child.data and isinstance(child.data, str):
                if target_url.startswith(child.data):
                    child.expand()
                    if child.data == target_url:
                        return True
                    return self._expand_tree_to(child, target_url)
        return False

    async def on_unmount(self) -> None:
        await scraper.close()


# ── File type icon helpers ────────────────────────────────────────────────────

_ICON_MAP = {
    ".zip": "🗜", ".7z": "🗜", ".rar": "🗜", ".tar": "🗜", ".gz": "🗜",
    ".iso": "💿", ".img": "💿", ".bin": "💿", ".cue": "💿", ".nrg": "💿",
    ".chd": "💿", ".cso": "💿", ".rvz": "💿", ".wbfs": "💿",
    ".rom": "🎮", ".sfc": "🎮", ".smc": "🎮", ".nes": "🎮", ".gba": "🎮",
    ".nds": "🎮", ".3ds": "🎮", ".gb": "🎮", ".gbc": "🎮", ".n64": "🎮",
    ".z64": "🎮", ".v64": "🎮", ".cdi": "🎮",
    ".pdf": "📄", ".txt": "📄", ".nfo": "📄", ".xml": "📄",
    ".mp3": "🎵", ".flac": "🎵", ".wav": "🎵", ".ogg": "🎵", ".m4a": "🎵",
    ".mp4": "🎬", ".mkv": "🎬", ".avi": "🎬",
    ".apk": "📱",
    ".dat": "🗂", ".log": "🗂",
}

def _file_icon(name: str) -> str:
    ext = name.lower().rsplit(".", 1)[-1] if "." in name else ""
    return _ICON_MAP.get(f".{ext}", "📄")


def _size_colored(size: int) -> str:
    """Return a colored markup string for a file size."""
    s = format_size(size)
    if size < 1024 ** 2:                  # < 1 MiB — green
        return f"[green]{s}[/]"
    elif size < 512 * 1024 ** 2:          # < 512 MiB — yellow
        return f"[yellow]{s}[/]"
    elif size < 5 * 1024 ** 3:            # < 5 GiB — orange (bright red)
        return f"[bold yellow]{s}[/]"
    else:                                  # ≥ 5 GiB — red
        return f"[bold red]{s}[/]"


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Browse and download from the Myrient archive TUI",
    )
    parser.add_argument(
        "--dest",
        "-d",
        metavar="DIR",
        default="downloads",
        help="root directory to write downloads into (default: %(default)s)",
    )
    args = parser.parse_args()

    app = MyrientBrowser()
    # allow overriding the default destination
    app._download_dir = args.dest
    app.run()
