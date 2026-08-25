#!/usr/bin/env python3
"""Fast, dependency-free CI gate for versioned operations scripts."""
from __future__ import annotations

import compileall
import hashlib
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = {
    "operations/scripts/compass_tuesday_prepare.py",
    "operations/scripts/hermes_workflow_reconciler.py",
    "operations/scripts/tests/test_compass_tuesday_prepare.py",
    "operations/scripts/tests/test_hermes_workflow_reconciler.py",
    "operations/tests/test_compass_tuesday_prepare_integration.py",
}


def expected_snapshots() -> dict[str, str]:
    expected: dict[str, str] = {}
    manifest = ROOT / "operations/provenance/SHA256SUMS"
    for line in manifest.read_text(encoding="utf-8").splitlines():
        digest, relative = line.split(maxsplit=1)
        if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise RuntimeError(f"invalid SHA-256 in {manifest}: {digest}")
        relative = relative.strip()
        path = Path(relative)
        if path.is_absolute() or ".." in path.parts or relative in expected:
            raise RuntimeError(f"unsafe or duplicate manifest path: {relative}")
        expected[relative] = digest
    if set(expected) != REQUIRED:
        raise RuntimeError(f"manifest paths differ: {set(expected) ^ REQUIRED}")
    return expected


def verify_snapshots() -> None:
    for relative, expected in expected_snapshots().items():
        path = ROOT / relative
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != expected:
            raise RuntimeError(f"snapshot mismatch: {relative}: {actual} != {expected}")


def run_tests() -> bool:
    loader = unittest.TestLoader()
    suite = unittest.TestSuite(
        [
            loader.discover(str(ROOT / "operations/scripts/tests"), pattern="test_*.py"),
            loader.discover(str(ROOT / "operations/tests"), pattern="test_*.py"),
        ]
    )
    return unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful()


def main() -> int:
    verify_snapshots()
    if not compileall.compile_dir(ROOT / "operations", quiet=1):
        return 1
    if not run_tests():
        return 1
    for script in (
        ROOT / "operations/scripts/compass_tuesday_prepare.py",
        ROOT / "operations/scripts/hermes_workflow_reconciler.py",
    ):
        subprocess.run(
            [sys.executable, str(script), "--help"],
            check=True,
            stdout=subprocess.DEVNULL,
            timeout=30,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
