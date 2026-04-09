---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-comments.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Comments [painless-comments]

Comments are used to explain or annotate code and make it more readable. They are ignored when the script is executed, allowing you to add notes or temporarily disable code sections without affecting the functionality.

Painless supports two types of comments: single-line comments and multi-line comments.

## Single-line comments

Single-line comments start with `//` and continue to the end of the line. Everything after `//` on that line is ignored by the compiler.

### Grammar

```
SINGLE_LINE_COMMENT: '//' .*? [\n\r];
```

### Example

```
// This is a single-line comment 
int value = 10; 
int price = 100; // Comment at the end of a line
```

## Multi-line comments

Multi-line comments start with `/*` and end with `*/`. Everything between these markers is ignored, even if it spans multiple lines.

### Grammar

```
MULTI_LINE_COMMENT: '/*' .*? '*/';
```

### Example

```
/* This is a 
   multi-line comment
   spanning several lines */
   
int total = price * quantity;

/* You can also use multi-line comments 
   to temporarily disable code blocks:
   
int debugValue = 0;
debugValue = calculateDebug();
*/

int result = /* inline comment */ calculateTotal();

```

## Best practices

Use comments to:

* Explain complex logic or business rules  
* Document function parameters and return values  
* Provide context data transformations  
* Temporarily disable code during development


:::{tip}
Good comments explain _why_ something is done, not just _what_ is being done. The code itself should be clear enough to show what it does.
::: 
