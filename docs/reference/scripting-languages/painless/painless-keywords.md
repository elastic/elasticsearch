---
navigation_title: Keywords
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-keywords.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Keywords (reserved terms) [painless-keywords]

Keywords are reserved tokens for built-in language features in Painless. These special words have predefined meanings and cannot be used as [identifiers](/reference/scripting-languages/painless/painless-identifiers.md), such as variable names, function names, or field names. 

:::{important}
In Painless documentation, "keywords" refers to reserved words in the scripting language itself. They are different from the {{es}} [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md#keyword-field-type) field type, which is used for exact-value searches and aggregations in your data mappings.
:::

When you write Painless scripts, keywords provide the fundamental building blocks for creating logic, defining data types, and controlling program flow. Since these words have special significance to the Painless compiler, attempting to use them for other purposes results in compilation errors.

### List of keywords:

| if | else | while | do | for |
| :---- | :---- | :---- | :---- | :---- |
| in | continue | break | return | new |
| try | catch | throw | this | instanceof |

## Understanding keyword restrictions

Examples of restricted terms include `if`, which tells the compiler to create a conditional statement, and `int`, which declares an integer variable type.

### Examples of valid keyword usage:

```
// Keywords used correctly for their intended purpose
int count = 0;           // `int' declares integer type
boolean isActive = true; // 'boolean' declares boolean type, 'true' is literal
if (count > 0) {         // 'if' creates conditional logic
    return count;        // 'return' exits with value
}

```

### Errors

If a keyword is used as an identifier Painless generates a compilation error:

```
// These will cause compilation errors
int if = 10;             // Cannot use 'if' as variable name
String return = "value"; // Cannot use 'return' as variable name  
boolean int = false;     // Cannot use 'int' as variable name

// Use descriptive names instead
int count = 10;
String result = "value";
boolean isEnabled = false;

```

These restrictions ensure that your scripts remain readable and that the Painless compiler can correctly parse your code without ambiguity.  
