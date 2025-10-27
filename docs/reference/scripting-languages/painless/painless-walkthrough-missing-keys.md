---
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Missing keys [_missing_keys]

`doc['myfield'].value` throws an exception if the field is missing in a document.

For more dynamic index mappings, you may consider writing a catch equation

```
if (!doc.containsKey('myfield') || doc['myfield'].empty) { return "unavailable" } else { return doc['myfield'].value }
```
