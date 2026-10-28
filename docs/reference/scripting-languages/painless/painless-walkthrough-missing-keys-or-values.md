---
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Missing keys or values [_missing_keys]

When you access document values, using `doc['myfield'].value` throws an exception if the specified field is missing in a document.

For more dynamic index mappings, you can write a catch equation:

```
if (!doc.containsKey('myfield') || doc['myfield'].empty) { return "unavailable" } else { return doc['myfield'].value }
```

This expression tests for the existence of `myfield`, returning its value only if the key exists.

To check if a document is missing a value, call `doc['myfield'].size() == 0`.