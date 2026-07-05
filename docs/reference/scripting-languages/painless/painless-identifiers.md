---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-identifiers.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Identifiers [painless-identifiers]

Use an identifier as a named token to specify a [variable](/reference/scripting-languages/painless/painless-variables.md), [type](/reference/scripting-languages/painless/painless-types.md), [field](/reference/scripting-languages/painless/painless-operators-reference.md#field-access-operator), [method](/reference/scripting-languages/painless/painless-operators-reference.md#method-call-operator), or [function](/reference/scripting-languages/painless/painless-functions.md).

:::{important}
Identifiers cannot be [keywords](/reference/scripting-languages/painless/painless-keywords.md). Using a keyword as an identifier will result in a compilation error.
:::

Identifiers are the names you give to elements in your code. They must follow specific naming rules and provide meaningful names that clearly describe their purpose. 

## Grammar

```java
ID: [_a-zA-Z] [_a-zA-Z-0-9]*;
```

## Naming rules

* Must start with a letter (a-z, A-Z) or underscore (\_)  
* Can contain letters, numbers (0-9), and underscores  
* Case sensitive (`myVar` and `MyVar` are different)  
* Cannot be a [keyword](/reference/scripting-languages/painless/painless-keywords.md)

### Examples

* Variations of identifiers

    ```text
     a      <1>
     Z      <2>
     id     <3>
     list   <4>
     list0  <5>
     MAP25  <6>
     _map25 <7>
     Map_25 <8>
    ```

    1. int a = 10;
    2. String Z = "Z";
    3. String id = "user_123";
    4. boolean list = true;
    5. float list0 = 3.14f;
    6. long MAP25 = 123456789L;
    7. double _map25 = 99.99;
    8. byte Map_25 = 25;

## Best practices

Choose meaningful identifier names that clearly describe their purpose:

```
// Good: descriptive and clear
String customerEmail = "user@example.com";
int totalPrice = calculateTotal();
boolean isProductAvailable = checkInventory();

// Avoid: unclear or too short
String s = "user@example.com";
int x = calculateTotal();
boolean b = checkInventory();

```
