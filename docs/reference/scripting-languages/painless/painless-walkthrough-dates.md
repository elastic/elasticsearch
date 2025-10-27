---
products:
  - id: painless
---

# Dates [modules-scripting-painless-dates]

Date fields are exposed as `ZonedDateTime`, so they support methods like `getYear`, `getDayOfWeek` or e.g. getting milliseconds since epoch with `getMillis`. To use these in a script, leave out the `get` prefix and continue with lowercasing the rest of the method name. For example, the following returns every hockey player’s birth year:

```console
GET hockey/_search
{
  "script_fields": {
    "birth_year": {
      "script": {
        "source": "doc.born.value.year"
      }
    }
  }
}
```