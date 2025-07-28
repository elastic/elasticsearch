---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-lambdas.html
---

# Lambdas [painless-lambdas]

Lambda expressions and method references work the same as in [Java](https://docs.oracle.com/javase/tutorial/java/javaOO/lambdaexpressions.md).

```painless
list.removeIf(item -> item == 2);
list.removeIf((int item) -> item == 2);
list.removeIf((int item) -> { item == 2 });
list.sort((x, y) -> x - y);
list.sort(Integer::compare);
```

You can make method references to functions within the script with `this`, for example `list.sort(this::mycompare)`.

