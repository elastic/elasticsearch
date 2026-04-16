---
navigation_title: Painless
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/index.html
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-guide.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Painless scripting language [painless-guide]

:::{tip}
This section introduces Painless syntax and advanced features. If you're new to Painless scripting, start with our [introduction](docs-content://explore-analyze/scripting/modules-scripting-painless.md) for fundamentals and examples.  
:::

Painless is the default scripting language for {{es}}, designed for security, performance, and flexibility. Built on the [Java Virtual Machine (JVM)](https://docs.oracle.com/en/java/javase/24/vm/java-virtual-machine-technology-overview.html), Painless provides Java-like syntax with direct compilation, a sandbox environment with fine-grained allowlists, and context-aware scripting across the {{stack}}.

## Language overview

Painless compiles directly to JVM bytecode, enabling native performance while maintaining security boundaries through fine-grained allowlists and a sandbox environment. The language implements a subset of Java syntax with {{es}} extensions, supporting field access patterns, datetime operations, and nested data manipulation across context-aware execution workloads.  
Scripts execute within specific contexts that control available variables, allowed operations, and execution environments. Different contexts provide APIs for their specific use case.

## Core benefits

Painless provides three core benefits:

* **Security:** Fine-grained allowlists that prevent access to restricted Java APIs and enforce security layers through a sandbox environment and JVM-level protection  
* **Performance:** Native compilation eliminates interpretation overhead and leverages JVM optimization, delivering native execution speed for production environments  
* **Flexibility:** Scripting syntax and execution contexts span the {{es}} stack, from search scoring and data processing to operational processing

## Reference resources

The reference documentation includes the following resources:

* [**Language specification:**](/reference/scripting-languages/painless/painless-language-specification.md) syntax, operators, data types, and compilation semantics
* [**Contexts:**](/reference/scripting-languages/painless/painless-contexts.md) Execution environments, available variables, and context-specific APIs  
* [**API examples:**](/reference/scripting-languages/painless/painless-api-examples.md) Examples of how to run the Painless execute API to build and test scripts
* [**Debugging:**](docs-content://explore-analyze/scripting/painless-debugging.md) Debugging techniques, common errors, and solutions

For step-by-step tutorials and real-world examples, refer to [How to write Painless scripts](docs-content://explore-analyze/scripting/modules-scripting-using.md) and [Painless script tutorials](docs-content://explore-analyze/scripting/common-script-uses.md) in the Explore and Analyze section.
