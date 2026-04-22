This module uses the ASM library to implement various things, including bytecode instrumentation.
It is loaded using the Embedded Provider Gradle plugin.

The core instrumenter (`InstrumenterImpl`) supports hierarchy-aware rule lookup: when instrumenting
a class, if a method has no direct rule, it performs a BFS over the supertype hierarchy (superclasses
and interfaces) to find an inherited rule. At most one inherited rule may exist in the hierarchy;
this invariant is enforced by the registry's `validate()` method at startup. This allows rules
defined on a supertype to automatically apply to all subtypes without duplication.
