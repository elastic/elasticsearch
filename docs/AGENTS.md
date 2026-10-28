# Docs

## Building docs

Always run `docs-builder` from the **root of the checkout**, not from a subdirectory.
Running from a subdirectory will only validate that subdirectory's links and may silently miss errors in cross-document references.

```bash
# From /path/to/elasticsearch (repo root):
docs-builder
```
