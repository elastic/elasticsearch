---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/modules-scripting-painless-dispatch.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Understanding method dispatching in Painless [modules-scripting-painless-dispatch]

Painless uses a function dispatch mechanism based on the receiver, method name, and [arity](https://en.wikipedia.org/wiki/Arity) (number of parameters). This approach differs from Java, which dispatches based on compile-time types, and Groovy, which uses [runtime types](https://en.wikipedia.org/wiki/Multiple_dispatch). Understanding this mechanism is fundamental when migrating scripts or interacting with Java standard library APIs from Painless, as it helps you avoid common errors and write more efficient, secure scripts.

## Key terms

Before diving into the dispatch process, here a brief definition of the main concepts:

* **Receiver:** The object or class on which the method is called.

    In `s.foo(a, b)` the type `s` is the receiver. The method `foo` that will be called depends on the type that the variable `s` is. For example, if `s` is a `List` then `foo` will be called on the `List` type.
* **Name:** The name of the method being invoked, such as `foo` in `s.foo(a, b)`  
* **Arity:** The number of parameters the method accepts. In `s.foo(a, b)` the arity is 2  
* **Dispatch:** The process of determining which method implementation to execute based on the receiver, name, and arity

```mermaid
flowchart TD
    A[s.foo(a, b)] --> B[Receiver: type of 's']
    B --> C[Name: method 'foo']
    C --> D[Arity: 2 parameters]
    D --> E[Execute method]
    
    style A fill:#0A52B3,color:#fff
    style B fill:#F5F7FA,stroke:#101C3F,color:#101C3F
    style C fill:#F5F7FA,stroke:#101C3F,color:#101C3F
    style D fill:#F5F7FA,stroke:#101C3F,color:#101C3F
    style E fill:#02BCB7,color:#fff
```

## Why method dispatch matters

This fundamental difference affects how you work with Java APIs in your scripts. When translating Java code to Painless, methods you expect from the standard library might have different names or behave differently. Understanding method dispatch helps you avoid common errors and write more efficient scripts, particularly when working with `def` types that benefit from this optimized resolution mechanism.

## Impact on Java standard library usage

The consequence of the different approach used by Painless is that Painless doesn’t support overloaded methods like Java, leading to some trouble when it allows classes from the Java standard library. For example, in Java and Groovy, `Matcher` has two methods:

* `group(int)`  
* `group(string)`

Painless can’t allow both of these methods because they have the same name and the same number of parameters. Instead, it has `group(int)` and `namedGroup(String)`. If you try to call a method that is not exposed in Painless, you will get a compilation error. 

This renaming pattern occurs throughout the Painless API when adapting Java standard library classes. Any methods that would conflict due to identical names and parameter counts receive distinct names in Painless to ensure unambiguous method resolution.

## Justification for this approach

We have a few justifications for this different way of dispatching methods:

1. It makes operating on `def` types simpler and, presumably, faster. Using receiver, name, and arity means that when Painless sees a call on a `def` object it can dispatch the appropriate method without having to do expensive comparisons of the types of the parameters. The same is true for invocation with `def` typed parameters.  
2. It keeps things consistent. It would be genuinely weird for Painless to behave like Groovy if any `def` typed parameters were involved and Java otherwise. It’d be slow for Painless to behave like Groovy all the time.   
3. It keeps Painless maintainable. Adding the Java or Groovy like method dispatch would add significant complexity, making maintenance and other improvements much more difficult.

## Next steps

For more details, view the [Painless language specification](/reference/scripting-languages/painless/painless-language-specification.md) and the [Painless API examples](/reference/scripting-languages/painless/painless-api-examples.md).

