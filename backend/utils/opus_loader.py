import os
import sys
from pathlib import Path


def add_bundled_opus_dll_directory() -> None:
    """Make bundled Windows libopus DLLs visible to opuslib.

    The opuslib package uses ctypes.util.find_library('opus') at import time.
    On Windows that lookup only succeeds when opus.dll is discoverable via PATH.
    PyOgg ships a compatible opus.dll in this venv, so expose that directory
    before importing opuslib instead of requiring a machine-wide Opus install.
    """
    if os.name != 'nt':
        return

    candidates = [
        Path(sys.prefix) / 'Lib' / 'site-packages' / 'pyogg' / 'libs' / 'win_amd64',
    ]

    for dll_dir in candidates:
        opus_dll = dll_dir / 'opus.dll'
        if not opus_dll.exists():
            continue

        dll_dir_str = str(dll_dir)
        path_parts = os.environ.get('PATH', '').split(os.pathsep)
        if dll_dir_str not in path_parts:
            os.environ['PATH'] = dll_dir_str + os.pathsep + os.environ.get('PATH', '')

        add_dll_directory = getattr(os, 'add_dll_directory', None)
        if add_dll_directory is not None:
            add_dll_directory(dll_dir_str)
        return
