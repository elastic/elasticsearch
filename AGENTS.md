# Agents

## ES|QL

ES|QL (Elasticsearch Query Language) is a piped query language for filtering, transforming, and analyzing data in Elasticsearch.

### External Resources

- **ES|QL public documentation**: https://www.elastic.co/docs/reference/query-languages/esql
- **ES|QL internal documentation & tutorials**: https://github.com/elastic/elasticsearch-team/tree/master/esql
  - [Home / Overview](https://github.com/elastic/elasticsearch-team/blob/master/esql/esTmESQLHome.mdx) — Introduction to ES|QL, what it is, and links to further resources (brown bag recordings, presentations)
  - [Architecture](https://github.com/elastic/elasticsearch-team/blob/master/esql/esTmESQLArchitecture.mdx) — The life of an ES|QL query: parsing, semantic analysis, logical plan optimization, physical planning, and distributed execution
  - [Getting Started](https://github.com/elastic/elasticsearch-team/blob/master/esql/esTmESQLContributingGettingStarted.mdx) — Recommended first steps for new contributors
  - [Important Concepts](https://github.com/elastic/elasticsearch-team/blob/master/esql/esTmESQLContributingImportantConcepts.mdx) — Plans, expressions, attributes, distributed execution, pipeline breakers, blocks and pages
  - [Testing](https://github.com/elastic/elasticsearch-team/blob/master/esql/esTmESQLContributingTesting.mdx) — How to write and debug tests (CsvTests, csv-spec files, EsqlCapabilities, unit tests, integration tests)
  - [Debugging](https://github.com/elastic/elasticsearch-team/blob/master/esql/esTmESQLContributingDebugging.mdx) — How to debug query plans, optimizer rules (TRACE logs, breakpoints), and individual operators
  - [Tutorials](https://github.com/elastic/elasticsearch-team/blob/master/esql/esTmESQLContributingTutorials.mdx) — Step-by-step tutorials (inspecting LogicalPlans, running with TRACE output, etc.)
  - [GitHub Workflow](https://github.com/elastic/elasticsearch-team/blob/master/esql/esTmESQLContributingGithub.mdx) — PR/issue labels and conventions (`ES|QL-ui`, `:Analytics/ES|QL`, `:Analytics/Compute Engine`, etc.)

### In-Repo Resources

#### Documentation

- `docs/reference/query-languages/esql/` — The ES|QL documentation source (static + generated content). See its [README](docs/reference/query-languages/esql/README.md) for how docs are structured and generated, including tutorials on adding new commands and examples.
  - `docs/reference/query-languages/esql/esql-getting-started.md` — Getting started guide
  - `docs/reference/query-languages/esql/esql-syntax.md` — Syntax reference
  - `docs/reference/query-languages/esql/esql-examples.md` — Example queries
  - `docs/reference/query-languages/esql/limitations.md` — Known limitations
  - `docs/reference/query-languages/esql/commands/` — Documentation for each ES|QL command (FROM, WHERE, EVAL, STATS, etc.)
  - `docs/reference/query-languages/esql/functions-operators/` — Documentation for functions and operators (aggregation, string, math, date-time, etc.)
  - `docs/reference/query-languages/esql/_snippets/` — Mix of static and generated content included in docs (function descriptions, examples, parameter tables, syntax diagrams)
  - `docs/reference/query-languages/esql/kibana/` — Generated function definitions and inline docs for Kibana's ES|QL editor

#### Source Code

- `x-pack/plugin/esql/` — Main ES|QL plugin
  - `src/main/antlr/` — ANTLR grammar files (`EsqlBaseLexer.g4`, `EsqlBaseParser.g4`)
  - `src/main/java/org/elasticsearch/xpack/esql/parser/` — Parser (`EsqlParser.java` and generated ANTLR classes)
  - `src/main/java/org/elasticsearch/xpack/esql/action/` — Query request/response handling (`EsqlQueryRequest.java`, `EsqlQueryResponse.java`, `EsqlCapabilities.java`)
  - `src/main/java/org/elasticsearch/xpack/esql/session/` — Query session and execution (`EsqlSession.java`)
  - `src/main/java/org/elasticsearch/xpack/esql/optimizer/` — Logical and physical plan optimizers
  - `src/main/java/org/elasticsearch/xpack/esql/planner/` — Plan-to-execution translation
  - `src/main/java/org/elasticsearch/xpack/esql/expression/function/` — Function registry and implementations (`EsqlFunctionRegistry.java`)
  - `src/main/java/org/elasticsearch/xpack/esql/plugin/` — Plugin entry point and transport actions (`EsqlPlugin.java`, `TransportEsqlQueryAction.java`)
- `x-pack/plugin/esql/compute/` — Compute engine (block-based execution)

#### Developer guides (in-repo)

Step-by-step guides for adding new ES|QL functions; the Javadoc in these packages is the source of truth:

- **Scalar functions** — [x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/function/scalar/package-info.java](x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/function/scalar/package-info.java): Guide to adding a new scalar function (csv-spec test first, copy a similar function, base classes, `@Evaluator` codegen, `EsqlFunctionRegistry`, serialization, unit tests, docs generation, `EsqlCapabilities`, snapshot functions, PR).
- **Aggregate functions** — [x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/function/aggregate/package-info.java](x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/function/aggregate/package-info.java): Guide to adding a new aggregation (builds on the scalar guide; base classes `AggregateFunction`, `NumericAggregate`, `SpatialAggregateFunction`; implementing `ToAggregator`; creating aggregators with `@Aggregator` / `@GroupingAggregator` in `org.elasticsearch.compute.aggregation`; AggregatorState, combine/evaluateFinal; StringTemplates for multi-type aggregators).

#### Testing

- `x-pack/plugin/esql/qa/testFixtures/src/main/resources/` — CSV-SPEC integration tests. See its [README](x-pack/plugin/esql/qa/testFixtures/src/main/resources/README.md) for organization and how to write/embed test examples in docs.
- `x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/optimizer/GoldenTestsReadme.MD` — Golden (characterization) tests for plan optimization. Documents how to add, run, and update golden tests.
- `x-pack/plugin/esql/qa/testFixtures/src/main/java/org/elasticsearch/xpack/esql/generator/` — Random ES|QL query generator for generative testing. See its [README](x-pack/plugin/esql/qa/testFixtures/src/main/java/org/elasticsearch/xpack/esql/generator/README.asciidoc).
- `x-pack/plugin/esql/qa/server/src/main/java/org/elasticsearch/xpack/esql/qa/rest/generative/` — Generative REST tests. See its [README](x-pack/plugin/esql/qa/server/src/main/java/org/elasticsearch/xpack/esql/qa/rest/generative/README.asciidoc).
