"""Quick sanity check for the scraper module."""
import sys
sys.path.insert(0, ".")
import scraper
from scraper import parse_size_str, format_size

print("Testing parse_size_str...")
assert parse_size_str("35.9 KiB") == 36762, f"Got {parse_size_str('35.9 KiB')}"
assert parse_size_str("1.0 MiB") == 1048576, f"Got {parse_size_str('1.0 MiB')}"
assert parse_size_str("471 B") == 471, f"Got {parse_size_str('471 B')}"
assert parse_size_str("-") is None
assert parse_size_str("992.2 KiB") is not None
print("  parse_size_str: OK")

print("Testing format_size...")
assert format_size(0) == "0 B"
assert format_size(471) == "471 B"
assert format_size(1024) == "1.0 KiB"
assert format_size(1048576) == "1.0 MiB"
assert format_size(1073741824) == "1.0 GiB"
print("  format_size: OK")

print("Testing Textual imports...")
from textual.app import App
from textual.widgets import Tree, DataTable, Footer, Header, Label, Static
from textual.containers import Horizontal, Vertical
from textual.binding import Binding
print("  Textual imports: OK")

print("\nAll checks passed!")
