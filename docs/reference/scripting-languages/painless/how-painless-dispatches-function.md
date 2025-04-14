---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/modules-scripting-painless-dispatch.html
---

# How painless dispatches function [modules-scripting-painless-dispatch]

Painless uses receiver, name, and [arity](https://en.wikipedia.org/wiki/Arity) for method dispatch. For example, `s.foo(a, b)` is resolved by first getting the class of `s` and then looking up the method `foo` with two parameters. This is different from Groovy which uses the [runtime types](https://en.wikipedia.org/wiki/Multiple_dispatch) of the parameters and Java which uses the compile time types of the parameters.

The consequence of this that Painless doesn’t support overloaded methods like Java, leading to some trouble when it allows classes from the Java standard library. For example, in Java and Groovy, `Matcher` has two methods: `group(int)` and `group(String)`. Painless can’t allow both of these methods because they have the same name and the same number of parameters. So instead it has `group(int)` and `namedGroup(String)`.

We have a few justifications for this different way of dispatching methods:

1. It makes operating on `def` types simpler and, presumably, faster. Using receiver, name, and arity means that when Painless sees a call on a `def` object it can dispatch the appropriate method without having to do expensive comparisons of the types of the parameters. The same is true for invocations with `def` typed parameters.
2. It keeps things consistent. It would be genuinely weird for Painless to behave like Groovy if any `def` typed parameters were involved and Java otherwise. It’d be slow for it to behave like Groovy all the time.
3. It keeps Painless maintainable. Adding the Java or Groovy like method dispatch **feels** like it’d add a ton of complexity which’d make maintenance and other improvements much more difficult.

