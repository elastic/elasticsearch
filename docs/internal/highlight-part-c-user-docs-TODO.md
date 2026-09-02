# HIGHLIGHT Part C — user-facing docs TODO

**Status:** placeholder. The user-facing reference documentation for the ES|QL
`HIGHLIGHT` Part C behavior is **not yet written**. This file tracks that gap so
it is not lost; it is intentionally the only deliverable of this documentation
layer of the stack.

## What still needs documenting

Part C makes the `HIGHLIGHT` query and `ON` clause optional. The three new forms
below need to be documented once the command graduates from snapshot-only:

- **Bare `HIGHLIGHT`** — no query, no `ON`. The query is derived from the
  full-text predicates of the upstream `WHERE` commands that still describe the
  documents reaching the command, and the fields are derived from what that query
  names (falling back to every text/keyword field).
- **`HIGHLIGHT ON *`** — highlight every text/keyword field reaching the command.
- **Implicit query with an explicit `ON`** — e.g.
  `... | WHERE MATCH(title, "x") | HIGHLIGHT ON title`.

## Where the real docs should live

The user-facing ES|QL command reference (alongside the other ES|QL processing
commands). The current `HIGHLIGHT` reference entry must be extended to cover the
optional-query / optional-`ON` / `ON *` forms and the derivation rules.

## Context for the writer

- `docs/internal/highlight-part-c-implementation.md` — full implementation guide:
  grammar, derivation rules, transport gating, and the `DocPreserving` walk.
- `docs/internal/highlight-demo.console` — runnable Console demo of every new form.
