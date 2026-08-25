# Versioned Hermes operations scripts

This directory is the maintained source for persistent control-plane scripts that are installed under `/opt/data/scripts` in the StartOS Hermes data volume. The package source repository is used because these scripts operate the Hermes package and host scheduler together; `/opt/hermes` remains immutable.

## Installed mapping

- `operations/scripts/compass_tuesday_prepare.py` → `/opt/data/scripts/compass_tuesday_prepare.py`
- `operations/scripts/hermes_workflow_reconciler.py` → `/opt/data/scripts/hermes_workflow_reconciler.py`
- `operations/scripts/tests/*.py` → `/opt/data/scripts/tests/*.py`

`operations/provenance/SHA256SUMS` pins the exact maintained snapshots. The integration test under `operations/tests` is a portable copy of the installed-contract test; only its source path is repository-relative so CI cannot accidentally test the host installation.

## Verification

```sh
python3 operations/ci_fast.py
```

Deployments must follow `/opt/data/HERMES_AUTONOMY_OPERATIONS.md`: pre/post performance-watchdog verification, exact-target backup, focused and fast tests, package-visible installation/readback, scheduler canary, and health audits. Never deploy these files into `/opt/hermes`.
