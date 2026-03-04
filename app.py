"""
app.py — Myrient Archive Browser TUI
Browse and inspect the file directory at https://myrient.erista.me/files/

Usage:
    python app.py

Key bindings:
    ↑ / ↓         Navigate list
    Enter          Open directory
    Space          Toggle selection on highlighted item
    A              Select / deselect all items in current directory
    D              Download selected items (recursive for dirs; prompts size first)
    S              Calculate size of selected item (recursive)
    G              Calculate GRAND TOTAL of entire archive
    R              Refresh current directory
    C              Copy current URL to clipboard
    Backspace      Go up one directory
    Q              Quit

Downloads are written to ./downloads/ (mirroring the remote path).
Already-downloaded files at the correct size are skipped automatically.
"""

from __future__ import annotations

import asyncio
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
    Label,
    ProgressBar,
    Static,
    Tree,
)
from textual.widgets.tree import TreeNode

import scraper
from scraper import Entry, BASE_URL, format_size, fetch_directory

# ── Activity Panel ────────────────────────────────────────────────────────────

_SPINNER_FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"


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
    def __init__(self) -> None:
        super().__init__("")
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
        max-height: 10;
        background: $surface-darken-2;
        border-top: solid $accent;
        padding: 0 1;
        display: none;
    }

    #download-progress {
        height: 1;
        background: $surface-darken-2;
        padding: 0 2;
        display: none;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
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
                table = DataTable(id="file-table", cursor_type="row")
                col_keys = table.add_columns("Name", "Size", "Date", "Type")
                self._name_col_key = col_keys[0]
                yield table

        yield StatusBar(id="status-bar")
        yield ActivityPanel(id="activity-panel")
        yield ProgressBar(total=100, id="download-progress", show_eta=False)
        yield Footer()

    # ── Startup ───────────────────────────────────────────────────────────────

    def on_mount(self) -> None:
        """Load the root directory when the app starts."""
        self._selected_keys = set()
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
            if entry.is_dir:
                # Navigate into the directory
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
        name_val = self._make_name_cell(entry, rk in self._selected_keys)
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
        """Collect files from targets (expanding dirs recursively), check size, then download."""
        sb = self.query_one("#status-bar", StatusBar)
        panel = self.query_one("#activity-panel", ActivityPanel)
        pbar = self.query_one("#download-progress", ProgressBar)
        task_id = "download"

        panel.add_task(task_id, "[bold]Download:[/] collecting file list…")

        # Expand all targeted dirs into individual file entries
        all_files: list[Entry] = []
        for entry in targets:
            if entry.is_dir:
                panel.update_task(task_id, f"[bold]Download:[/] scanning [italic]{entry.display_name}[/]…")
                sub = await scraper.collect_files(entry.url)
                all_files.extend(sub)
            else:
                all_files.append(entry)

        if not all_files:
            sb.status_msg = "No files found to download."
            panel.finish_task(task_id)
            return

        total_count = len(all_files)
        total_bytes = sum(e.size_bytes for e in all_files if e.size_bytes is not None)
        unknown = sum(1 for e in all_files if e.size_bytes is None)
        size_label = format_size(total_bytes) + (f" + {unknown} unknown" if unknown else "")

        # Show the progress bar
        pbar.update(total=total_count, progress=0)
        pbar.display = True

        sb.status_msg = f"Downloading {total_count} files  ({size_label})  → {dest_root}/"

        # Download with limited concurrency
        sem = asyncio.Semaphore(4)
        done = 0
        errors = 0

        async def _dl(entry: Entry) -> None:
            nonlocal done, errors
            async with sem:
                try:
                    await scraper.download_entry(entry, dest_root)
                except Exception as exc:
                    errors += 1
                    self.notify(
                        f"Failed to download {entry.name}:\n{exc}",
                        title="Download error",
                        severity="warning",
                    )
                finally:
                    done += 1
                    pbar.update(progress=done)
                    err_note = f"  [red]({errors} errors)[/]" if errors else ""
                    panel.update_task(
                        task_id,
                        f"[bold]Download:[/] {done}/{total_count} files"
                        f"  {_prog_bar(done, total_count, 20)}"
                        f"  [dim]→ {dest_root}/[/]{err_note}",
                    )

        await asyncio.gather(*[_dl(e) for e in all_files])

        pbar.display = False
        panel.finish_task(task_id)

        saved = done - errors
        self.notify(
            f"{saved}/{total_count} files saved to {dest_root}/"
            + (f"\n{errors} errors" if errors else ""),
            title="Download complete" if not errors else "Download finished with errors",
            severity="information" if not errors else "warning",
        )
        sb.status_msg = (
            f"Done: {saved}/{total_count} files saved to {dest_root}/"
            + (f"  ({errors} errors)" if errors else "")
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _make_name_cell(self, entry: Entry, selected: bool) -> str:
        """Build the Name cell string with optional selection checkmark."""
        icon = "\U0001f4c1" if entry.is_dir else _file_icon(entry.name)
        check = "[bold green]✓[/] " if selected else "  "
        return f"{check}{icon} {entry.display_name}"

    def _refresh_name_cells(self) -> None:
        """Redraw the Name column for every entry row to reflect current selections."""
        table = self.query_one("#file-table", DataTable)
        for i, entry in enumerate(self._current_entries):
            rk = str(i)
            name_val = self._make_name_cell(entry, rk in self._selected_keys)
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
    app = MyrientBrowser()
    app.run()
