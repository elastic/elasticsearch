
Matching the exact characters `*` and `.` will require escaping.
The escape character is backslash `\`. Since also backslash is a special character in string literals,
it will require further escaping.

```esql
ROW message = "foo * bar"
| WHERE message LIKE "foo \\* bar"
```


To reduce the overhead of escaping, we suggest using triple quotes strings `"""`

```esql
ROW message = "foo * bar"
| WHERE message LIKE """foo \* bar"""
```


