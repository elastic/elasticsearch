# Plan: Add dense_vector support for COALESCE (issue #139928)

## Summary

Add support for the `dense_vector` field type to the ES|QL `COALESCE` function so that `COALESCE(vec1, vec2, ...)` returns the first non-null dense_vector among its arguments.

## Context

- **Issue**: [elastic/elasticsearch#139928](https://github.com/elastic/elasticsearch/issues/139928)
- **Current state**: `Coalesce.java` explicitly throws `UnsupportedOperationException` for `DENSE_VECTOR` in `toEvaluator()` (line 219). The `@FunctionInfo` and `@Param` type lists do not include `dense_vector`.
- **Runtime representation**: In the compute engine, `DataType.DENSE_VECTOR` maps to `ElementType.FLOAT` (see `PlannerUtils.toElementType()`). Dense vectors are stored as `FloatBlock` where each position can hold multiple float values (one vector per row). So adding COALESCE for dense_vector means reusing the same pattern as other types but with **FloatBlock** (same as the existing CoalesceDoubleEvaluator pattern, but for Float).

## Implementation plan

### 1. Add CoalesceFloatEvaluator (codegen + wiring)

- **1.1** In `x-pack/plugin/esql/build.gradle`:
  - Define `floatProperties` (same pattern as `doubleProperties`):  
    `var floatProperties = prop("Float", "Float", "float", "FLOAT", "Float.BYTES", "FloatArray")`  
    If the string template or other code expects a `"float"` key in the prop map, add it in the `prop()` helper (e.g. `"float" : type == "float" ? "true" : ""`).
  - Add a new `template { ... }` block for Coalesce that uses `floatProperties` and outputs  
    `org/elasticsearch/xpack/esql/expression/function/scalar/nulls/CoalesceFloatEvaluator.java`.

- **1.2** Run the string-template codegen so that `CoalesceFloatEvaluator.java` is generated under `src/main/generated-src/` (e.g. `./gradlew :x-pack:plugin:esql:compileJava` or the task that runs stringTemplates). The existing `X-CoalesceEvaluator.java.st` template is generic (uses `$Type$Block`, `copyFrom(block, position)`); FloatBlock supports `copyFrom(FloatBlock, int position)` for full position entries, so no template change should be required.

- **1.3** In `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/function/scalar/nulls/Coalesce.java`:
  - Add `"dense_vector"` to the `returnType` and to both `type` arrays in `@FunctionInfo` and in the two `@Param` annotations (for `first` and `rest`).
  - In `toEvaluator()`: add a case for `DENSE_VECTOR` that delegates to `CoalesceFloatEvaluator.toEvaluator(toEvaluator, children())`.
  - Remove `DENSE_VECTOR` from the `throw new UnsupportedOperationException(...)` case so it is no longer listed as unsupported.

### 2. Update CoalesceTests

- **2.1** In `x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/expression/function/scalar/nulls/CoalesceTests.java`:
  - Add test case suppliers for `DataType.DENSE_VECTOR`, following the same pattern as existing types (e.g. `DataType.DATETIME`, `DataType.TDIGEST`):
    - At least one supplier for two dense_vector arguments (first non-null wins).
    - Use the same evaluator string expectation pattern: `"CoalesceFloatEagerEvaluator[values=[Attribute[channel=0], Attribute[channel=1]]]"`.
  - Dense vector test data can follow the approach used elsewhere: e.g. use `MultiRowTestCaseSupplier`-style helpers if available for DENSE_VECTOR, or construct `TestCaseSupplier.TypedData` with `float[]` or the appropriate block/value type that the test infrastructure expects for `DataType.DENSE_VECTOR` (see `MultiRowTestCaseSupplier` around line 701 for DENSE_VECTOR).
  - Add the same “nulls up to a point” and “force null type” variants as for other types (loop over `noNullsSuppliers` and add null-case suppliers) so that the new DENSE_VECTOR cases are covered by the parameterized tests.

- **2.2** Run the unit tests to ensure the new cases pass:  
  `./gradlew :x-pack:plugin:esql:test --tests "org.elasticsearch.xpack.esql.expression.function.scalar.nulls.CoalesceTests"`.

### 3. Add CSV-SPEC tests for COALESCE with dense_vector

- **3.1** Add tests in the main dense_vector csv-spec file  
  `x-pack/plugin/esql/qa/testFixtures/src/main/resources/dense_vector.csv-spec`:
  - At least one test that uses `COALESCE` with columns from the `dense_vector` dataset (e.g. `float_vector` and/or literals), e.g.:
    - `COALESCE(float_vector, <literal dense_vector>)` or `COALESCE(null_column, float_vector)`.
  - Include the same `required_capability` lines used by other tests in that file (e.g. `dense_vector_field_type_released`, `dense_vector_agg_metric_double_if_version`, `l2_norm_vector_similarity_function` as applicable) so tests are skipped on clusters that don’t support dense_vector.

- **3.2** Optionally add a COALESCE test in other `dense_vector-*.csv-spec` files (e.g. `dense_vector-arithmetic.csv-spec`, `dense_vector-byte.csv-spec`, etc.) if we want to cover different element types or datasets; the issue calls out “dense-vector*.csv-spec” so at least the main `dense_vector.csv-spec` should be updated.

- **3.3** Run the CSV-SPEC tests (e.g. via `CsvTests` or the relevant EsqlSpecIT) to confirm they pass.

### 4. Docs and capabilities (if required)

- **4.1** If the change is gated by a new capability (e.g. for backwards compatibility), add a new entry in `EsqlCapabilities` and use the corresponding `required_capability` in the new csv-spec tests. The issue does not mandate a new capability; if the team prefers to ship without a new capability because dense_vector is already GA, this step can be skipped.

- **4.2** Regenerate ES|QL docs if function metadata is generated from source (e.g. `@FunctionInfo`): run the usual doc-generation steps (see `docs/reference/query-languages/esql/README.md`) so that the reference lists COALESCE as supporting `dense_vector`.

### 5. Sanity checks

- Run the full ESQL test suite for the plugin:  
  `./gradlew :x-pack:plugin:esql:test`
- Run the relevant csv-spec integration tests (e.g. EsqlSpecIT with the dense_vector dataset).
- Manually run a query like:  
  `FROM dense_vector | EVAL v = COALESCE(float_vector, to_dense_vector([0,0,0])) | KEEP id, v`  
  and confirm the result shape and values.

## Files to touch (checklist)

| Area | File | Change |
|------|------|--------|
| Codegen | `x-pack/plugin/esql/build.gradle` | Add `floatProperties`, add Coalesce template for CoalesceFloatEvaluator |
| Core | `Coalesce.java` | Add dense_vector to annotations and toEvaluator switch |
| Tests | `CoalesceTests.java` | Add DENSE_VECTOR test case suppliers |
| CSV-SPEC | `dense_vector.csv-spec` | Add COALESCE tests with dense_vector |
| Optional | Other `dense_vector-*.csv-spec` | Add COALESCE tests if desired |
| Optional | `EsqlCapabilities.java` | New capability only if required by process |
| Generated | `CoalesceFloatEvaluator.java` | Produced by build; commit under generated-src |

## Risks / notes

- **FloatBlock and multi-value**: Dense vectors are stored as multi-value FloatBlocks (one position = one vector). The existing Coalesce template uses `copyFrom(block, position)`, which for FloatBlock copies the whole position entry. No change to the template should be needed.
- **Backwards compatibility**: If any cluster might not support dense_vector in COALESCE, gate the new behavior or tests with a capability; otherwise no capability change is needed.
- **Docs**: The public ES|QL docs list supported types for COALESCE; after adding `dense_vector` to `@FunctionInfo` / `@Param`, regenerate the snippets so the docs show dense_vector support.
