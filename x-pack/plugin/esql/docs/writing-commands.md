# Writing ES|QL Commands

This guide lists the touch points for adding a new ES|QL **command** (a pipe or source clause like `LIMIT`, `WHERE`, `STATS`, `FROM`). Use a simple existing command as a template—e.g. **LIMIT** or **SAMPLE**—and follow this checklist.

## Overview

A command flows through: **Parse → Logical plan → Analyze (optional) → Map to physical → PlanWritables → LocalExecutionPlanner → Tests**. Each layer must know about the new command.

## Checklist (in order)

### 1. Grammar & Lexer

- **Parser** (`x-pack/plugin/esql/src/main/antlr/EsqlBaseParser.g4`):
  - Add a rule for your command (e.g. `myCommand : MY_KEYWORD ... ;`).
  - Add it to `sourceCommand` (if it starts a query) or `processingCommand` (if it appears after `|`).
- **Lexer**: Ensure the keyword exists. If needed, add or extend a lexer grammar (see e.g. `Limit.g4`, `ChangePoint.g4`) and import it in `EsqlBaseLexer.g4`.

### 2. Logical plan builder

- **LogicalPlanBuilder** (`parser/LogicalPlanBuilder.java`):
  - Implement `visitMyCommand(EsqlBaseParser.MyCommandContext ctx)`.
  - Return a `PlanFactory` (e.g. `input -> new MyCommand(source(ctx), input, ...)`).

### 3. Logical plan node

- New class under `plan/logical/` (e.g. `MyCommand.java`):
  - Extend `UnaryPlan` (or the appropriate plan type).
  - Implement `NamedWriteable`: `getWriteableName()`, `writeTo(StreamOutput)`, and a constructor from `StreamInput`.
  - Add a static `NamedWriteableRegistry.Entry ENTRY` and use a unique string for the writeable name.

### 4. Analyzer (optional)

- If the command has expressions that need resolution or validation, add or extend rules in `analysis/` (e.g. `Analyzer`, `AnalyzerRules`, or rule classes that match your logical node).

### 5. Mapper (logical → physical)

- **Mapper** (`planner/mapper/Mapper.java`):
  - In `mapUnary()` (or the appropriate `map*` method), add a branch for your logical node.
  - Create the physical plan node (e.g. `new MyCommandExec(...)`).
  - If the command is a pipeline breaker or affects fragments, follow the same patterns as `Limit`/`TopN`/`Aggregate` (e.g. `addExchangeForFragment`, `FragmentExec`).

### 6. Physical plan node

- New class under `plan/physical/` (e.g. `MyCommandExec.java`):
  - Same serialization pattern as logical: `getWriteableName()`, `writeTo`, constructor from `StreamInput`, and static `ENTRY`.

### 7. PlanWritables

- **PlanWritables** (`plan/PlanWritables.java`):
  - Add your logical plan `ENTRY` to `logical()`.
  - Add your physical plan `ENTRY` to `physical()`.

### 8. LocalExecutionPlanner

- **LocalExecutionPlanner** (`planner/LocalExecutionPlanner.java`):
  - In the main `plan(PhysicalPlan, ...)` dispatch, add a branch for your physical node (e.g. `else if (node instanceof MyCommandExec)`).
  - Implement a method that builds a `PhysicalOperation` using the appropriate compute operator (e.g. from `org.elasticsearch.compute.operator` or a custom factory).

### 9. Optimizer rules (optional)

- If the command is a pipeline breaker, participates in pushdown, or needs logical/physical rewrites:
  - Add or extend rules in `optimizer/` (e.g. `LogicalPlanOptimizer`, `PhysicalPlanOptimizer`, or local optimizers).
  - Reference existing rules for `Limit`, `TopN`, or `Filter` for patterns.

### 10. Tests

- **Parser**: Add cases in `StatementParserTests` or similar.
- **Logical plan**: Serialization test (e.g. `plan/logical/MyCommandSerializationTests.java`).
- **Physical plan**: Serialization test (e.g. `plan/physical/MyCommandExecSerializationTests.java`).
- **Integration**: Add at least one csv-spec test under `qa/testFixtures/src/main/resources/` or a yaml rest test under `src/yamlRestTest/`.

### 11. Generative tests (optional)

- Implement a **CommandGenerator** in `qa/testFixtures/.../generator/command/pipe/` (or `source/`).
  - Implement `generate()` and `validateOutput()`.
  - Register it in `EsqlQueryGenerator.PIPE_COMMANDS` or `SOURCE_COMMANDS`.

### 12. Capabilities (if feature-flagged)

- Add a capability in `EsqlCapabilities` and use it in rest/yaml tests (e.g. `required_capability: my_command`).

## Reference commands

| Command   | Use case                          | Notes                                    |
|-----------|-----------------------------------|------------------------------------------|
| **LIMIT** | Simple unary, one expression      | Parser, Limit, LimitExec, LimitOperator |
| **SAMPLE**| Simple unary, one expression      | Similar to LIMIT                         |
| **WHERE** | Filter (expression + child)       | Filter → FilterExec → FilterOperator     |
| **STATS** | Pipeline breaker, aggregation     | Fragment/Exchange and aggregator mapping |

## Related docs

- **Scalar functions**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/function/scalar/package-info.java`
- **Query planner steps**: `x-pack/plugin/esql/package-info.java` (Query Planner Steps)
- **Generative command generator**: `x-pack/plugin/esql/qa/testFixtures/src/main/java/.../generator/README.asciidoc`
