---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-statements.html
products:
  - id: painless
---

# Statements [painless-statements]

Painless supports all of Java’s [ control flow statements](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/flow.md) except the `switch` statement.

## Conditional statements [_conditional_statements]

### If / Else [_if_else]

```painless
if (doc[item].size() == 0) {
  // do something if "item" is missing
} else if (doc[item].value == 'something') {
  // do something if "item" value is: something
} else {
  // do something else
}
```



## Loop statements [_loop_statements]

### For [_for]

Painless also supports the `for in` syntax:

```painless
for (def item : list) {
  // do something
}
```

```painless
for (item in list) {
  // do something
}
```


### While [_while]

```painless
while (ctx._source.item < condition) {
  // do something
}
```


### Do-While [_do_while]

```painless
do {
  // do something
}
while (ctx._source.item < condition)
```



