---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-lang-spec.html
products:
  - id: painless
---

# Painless language specification [painless-lang-spec]

Painless is a scripting language designed for security and performance. Painless syntax is similar to Java syntax along with some additional features such as dynamic typing, Map and List accessor shortcuts, and array initializers. As a direct comparison to Java, there are some important differences, especially related to the casting model. For more detailed conceptual information about the basic constructs that Painless and Java share, refer to the corresponding topics in the [Java Language Specification](https://docs.oracle.com/javase/specs/jls/se8/html/index.md).

Painless scripts are parsed and compiled using the [ANTLR4](https://www.antlr.org/) and [ASM](https://asm.ow2.org/) libraries. Scripts are compiled directly into Java Virtual Machine (JVM) byte code and executed against a standard JVM. This specification uses ANTLR4 grammar notation to describe the allowed syntax. However, the actual Painless grammar is more compact than what is shown here.



















