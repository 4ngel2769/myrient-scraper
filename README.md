# Myrient Scraper

Simple terminal-based browser and downloader for the Myrient archive.

## Setup

```bash
python -m venv .venv
. .venv/bin/activate        # or .venv\Scripts\Activate.ps1 on Windows
pip install -r requirements.txt
```

## Usage

Run the TUI and navigate:

```bash
python app.py [--dest DIR]
```

- `Ctrl+F` opens search; type to filter indexed entries.
- `Space` selects, `D` downloads, `A` selects all, `Backspace` goes up a directory.
- Downloads go to `./downloads` by default or directory supplied via `--dest`.

The program automatically indexes the full site in the background.
