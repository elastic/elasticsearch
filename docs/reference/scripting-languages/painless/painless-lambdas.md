---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-lambdas.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Lambdas [painless-lambdas]

Lambda expressions are anonymous functions that provide a concise way to write short, inline functions without declaring them explicitly. Lambdas are particularly useful for functional programming operations such as filtering, mapping, and sorting collections.

Lambda expressions and method references work the same as in [Java](https://docs.oracle.com/javase/tutorial/java/javaOO/lambdaexpressions.html), providing familiar syntax for developers. They allow you to write more compact and expressive code when working with collections and functional interfaces.

## Lambda syntax

Lambdas use the arrow syntax (`->`) to separate parameters from the function body. You can use lambdas with or without explicit type declarations.

### Examples

Basic lambda expressions:

```java
// Removes all elements equal to 2
list.removeIf(item -> item == 2);              
list.removeIf((int item) -> item == 2);       
list.removeIf((int item) -> { item == 2 }); 

// Sorts list in ascending order
list.sort((x, y) -> x - y);       
// Sorts list in ascending order using method reference             
list.sort(Integer::compare);   
```

## Method references

You can make method references to functions within the script with `this`, for example `list.sort(this::mycompare)`. Method references provide a shorthand notation for lambdas that call a specific method.

## Common use cases

Lambdas are commonly used for:

* **Filtering collections:** Remove elements that meet specific criteria.  
* **Sorting data:** Define custom comparison logic.  
* **Transforming values:** Apply operations to collection elements.  
* **Functional operations:** Work with streams and functional interfaces.






