---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-identifiers.html
---

# Identifiers [painless-identifiers]

Use an identifier as a named token to specify a [variable](/reference/scripting-languages/painless/painless-variables.md), [type](/reference/scripting-languages/painless/painless-types.md), [field](/reference/scripting-languages/painless/painless-operators-reference.md#field-access-operator), [method](/reference/scripting-languages/painless/painless-operators-reference.md#method-call-operator), or [function](/reference/scripting-languages/painless/painless-functions.md).

**Errors**

If a [keyword](/reference/scripting-languages/painless/painless-keywords.md) is used as an identifier.

**Grammar**

```text
ID: [_a-zA-Z] [_a-zA-Z-0-9]*;
```

**Examples**

* Variations of identifiers.

    ```painless
    a
    Z
    id
    list
    list0
    MAP25
    _map25
    Map_25
    ```


