---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-functions.html
products:
  - id: painless
---

# Functions [painless-functions]

A function is a named piece of code comprised of one-to-many statements to perform a specific task. A function is called multiple times in a single script to repeat its specific task. A parameter is a named type value available as a [variable](/reference/scripting-languages/painless/painless-variables.md) within the statement(s) of a function. A function specifies zero-to-many parameters, and when a function is called a value is specified per parameter. An argument is a value passed into a function at the point of call. A function specifies a return type value, though if the type is [void](/reference/scripting-languages/painless/painless-types.md#void-type) then no value is returned. Any non-void type return value is available for use within an [operation](/reference/scripting-languages/painless/painless-operators.md) or is discarded otherwise.

You can declare functions at the beginning of a Painless script, for example:

```painless
boolean isNegative(def x) { x < 0 }
...
if (isNegative(someVar)) {
  ...
}
```

