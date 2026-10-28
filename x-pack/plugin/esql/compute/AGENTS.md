# ES|QL Compute Engine

## Code generation

Most classes in this module are generated via two mechanisms:

1. **StringTemplate** (`*.java.st` files)
2. **Annotation processors** (`compute/gen`) — classes annotated with `@Evaluator`,
   `@Aggregator`, `@GroupingAggregator`, etc. are generated at compile time automatically.

Do not hand-edit generated files. Edit the template or annotated source instead.

## Hand-written classes

Some classes are hand-written across multiple type variants instead of generated.
Two rules apply:

- When changing one variant, keep all variants in sync.
- When changing annotation processors or generated output, update hand-written classes
  to stay consistent with the new generated code.

Hand-written families:

- `Rate*GroupingAggregatorFunction` — `IncreaseExponentialHistogramGroupingAggregatorFunction` also belongs to this family.
