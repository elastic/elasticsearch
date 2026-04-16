# ES|QL documentation

> [!NOTE]
> This directory contains the source files for the [ES|QL documentation](https://www.elastic.co/docs/reference/query-languages/esql).

This README covers how the ES|QL docs are structured, what's hand-written vs. auto-generated, and how to add or update commands and functions correctly.

**Adding a new command or function:**
- [Add a new command](#add-a-new-command)
- [Add a new function](#add-a-new-function)
- [Add version metadata](#add-version-metadata)
- [Publish your docs](#publish-your-docs)

**Promoting an existing command or function to GA:**
- [Promote a feature to GA](#promote-a-feature-to-ga)
- [Update list files](#update-list-files)

**Reference:**
- [Regenerate content](#regenerate-content)
- [Understand how generated content works](#understand-how-generated-content-works)
- [PromQL docs](#generate-promql-definitions)

**Learn about the docs system**
- [Elastic docs resources](#elastic-docs-resources)

## Add a new command

Each command has its own standalone page under [`commands/`](https://github.com/elastic/elasticsearch/tree/main/docs/reference/query-languages/esql/commands) that includes a layout snippet from [`_snippets/commands/layout/`](https://github.com/elastic/elasticsearch/tree/main/docs/reference/query-languages/esql/_snippets/commands/layout).

To add a new processing command called `<my_command>`:

1. Create the layout snippet `_snippets/commands/layout/<my_command>.md`
   - Write the command's content here
   - See existing files in that directory for examples
   - Add version metadata: see [Add version metadata](#add-version-metadata)
2. Add the command to [`_snippets/lists/processing-commands.md`](https://github.com/elastic/elasticsearch/blob/main/docs/reference/query-languages/esql/_snippets/lists/processing-commands.md) or [`_snippets/lists/source-commands.md`](https://github.com/elastic/elasticsearch/blob/main/docs/reference/query-languages/esql/_snippets/lists/source-commands.md)
   - Link to the new standalone page path
   - Add [version tags](#update-list-files)
3. Create the standalone wrapper page `commands/<my-command>.md`
   - Add frontmatter with `navigation_title`
   - Add an H1 heading with an anchor
   - Add an `:::{include}` of the layout snippet
   - See [`commands/change-point.md`](https://github.com/elastic/elasticsearch/blob/main/docs/reference/query-languages/esql/commands/change-point.md) as a template
4. Add the page to [`docs/reference/query-languages/toc.yml`](https://github.com/elastic/elasticsearch/blob/main/docs/reference/query-languages/toc.yml)
   - As a child of `processing-commands.md` or `source-commands.md`
   - See [Publish your docs](#publish-your-docs) if the page isn't ready to go live yet
5. Add tested examples
   - See [Add examples to a command](#add-examples-to-a-command) below

## Add examples to a command

When adding tested examples to a command, for example adding an example to the `CHANGE_POINT` command, do the following:
* Make sure you have an example in an appropriate csv-spec file in the [`x-pack/plugin/esql/qa/testFixtures/src/main/resources/`](https://github.com/elastic/elasticsearch/tree/main/x-pack/plugin/esql/qa/testFixtures/src/main/resources) directory.
* Make sure the example has a tag that is unique in that file, and matches the intent of the test, or the docs reason for including that test.
* If you only want to show the query, and no results, then do not tag the results table,
  otherwise tag the results table with a tag that has the same name as the query tag, but with the suffix `-result`.
* Create a file with the name of the tag in a subdirectory with the name of the csv-spec file
  in the [`_snippets/commands/examples`](https://github.com/elastic/elasticsearch/tree/main/docs/reference/query-languages/esql/_snippets/commands/examples) directory. While you could add the content to that file, it is not necessary, merely that the file exists
* Run the test `CommandDocsTests` in the `x-pack/plugin/esql` module to generate the content.

For example, we tag the following test in change_point.csv-spec:

```
example for docs
required_capability: change_point

// tag::changePointForDocs[]
ROW key=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25]
| MV_EXPAND key
| EVAL value = CASE(key<13, 0, 42)
| CHANGE_POINT value ON key
| WHERE type IS NOT NULL
// end::changePointForDocs[]
;

// tag::changePointForDocs-result[]
key:integer | value:integer | type:keyword | pvalue:double
13          | 42            | step_change  | 0.0
// end::changePointForDocs-result[]
;
```

Then we create the file [`_snippets/commands/examples/change_point.csv-spec/changePointForDocs.md`](https://github.com/elastic/elasticsearch/blob/main/docs/reference/query-languages/esql/_snippets/commands/examples/change_point.csv-spec/changePointForDocs.md) with the content:
```
This should be overwritten
```

Then we run the test `CommandDocsTests` in the `x-pack/plugin/esql` module to generate the content.

Now the content of the changePointForDocs.md file should have been updated:

```
% This is generated by ESQL's AbstractFunctionTestCase. Do no edit it. See ../README.md for how to regenerate it.

\```esql
ROW key=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25]
| MV_EXPAND key
| EVAL value = CASE(key<13, 0, 42)
| CHANGE_POINT value ON key
| WHERE type IS NOT NULL
\```

| key:integer | value:integer | type:keyword | pvalue:double |
| --- | --- | --- | --- |
| 13 | 42 | step_change | 0.0 |
```

Finally include this file in the `CHANGE_POINT` command file [`_snippets/commands/layout/change_point.md`](https://github.com/elastic/elasticsearch/blob/main/docs/reference/query-languages/esql/_snippets/commands/layout/change_point.md):

```
**Examples**

The following example shows the detection of a step change:

:::{include} ../examples/change_point.csv-spec/changePointForDocs.md
:::
```

## Add a new function

Each function has its own standalone page under `functions-operators/<group>/` that includes a generated layout snippet from [`_snippets/functions/layout/`](https://github.com/elastic/elasticsearch/tree/main/docs/reference/query-languages/esql/_snippets/functions/layout).

To add a new function called `<my_func>` to the `<group>` group (e.g. `string-functions`):

> [!TIP]
> For a full walkthrough of the Java implementation, see the [guide in `package-info.java`](https://github.com/elastic/elasticsearch/blob/main/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/function/scalar/package-info.java).

1. Implement the function's Java class
   - Add `@FunctionInfo` and `@Param` annotations
   - Add version metadata: see [Add version metadata](#add-version-metadata)
   - If the function name doesn't start with a known prefix (`mv_`, `st_`, `to_`, `date_`, `is_`),
     add it to the switch statement in `DocsV3Support.functionGroupFor()`.
     This maps function names to groups for cross-reference link generation.
      > [!WARNING]
      > Without this, the test in the next step will fail with `Docs Generation Error: Unknown function group`.
2. Create a test class extending `AbstractFunctionTestCase`
3. Run the test to generate all snippets
   - `./gradlew :x-pack:plugin:esql:test -Dtests.class='<MyFunc>Tests'`
   - This generates files in [`_snippets/functions/`](https://github.com/elastic/elasticsearch/tree/main/docs/reference/query-languages/esql/_snippets/functions) (layout, description, parameters, types, examples) and [`images/functions/`](https://github.com/elastic/elasticsearch/tree/main/docs/reference/query-languages/esql/images/functions)
4. Add the function to `_snippets/lists/<group>.md`
   - Link to the individual page path: `functions-operators/<group>/<my_func>.md`
   - Add [version tags](#update-list-files)
5. Create the standalone wrapper page `functions-operators/<group>/<my_func>.md`
   - Add frontmatter with `navigation_title`
   - Add an H1 heading with an anchor
   - Add an `:::{include}` of the layout snippet
   - See [`functions-operators/aggregation-functions/avg.md`](https://github.com/elastic/elasticsearch/blob/main/docs/reference/query-languages/esql/functions-operators/aggregation-functions/avg.md) as a template
6. Add the page to [`docs/reference/query-languages/toc.yml`](https://github.com/elastic/elasticsearch/blob/main/docs/reference/query-languages/toc.yml)
   - As a child of the group page entry
   - See [Publish your docs](#publish-your-docs) if the page isn't ready to go live yet


## Add version metadata

> [!IMPORTANT]
> Starting with 9.0, we no longer publish separate documentation branches for every minor release (`9.0`, `9.1`, `9.2`, etc.).
> This means there won't be a different page for `9.1`, `9.2`, and so on. Instead, all changes landing in subsequent minor releases **will appear on the same page**.

Because we now publish just one docs set off of the `main` branch, we use the [`applies_to` metadata](https://elastic.github.io/docs-builder/syntax/applies/) to differentiate features and their availability across different versions. This is a [cumulative approach](https://elastic.github.io/docs-builder/contribute/#cumulative-docs): instead of creating separate pages for each product and release, we update a **single page** with product- and version-specific details over time.

`applies_to` allows us to clearly communicate when features are introduced, when they transition from preview to GA, and which versions support specific functionality.

This metadata accepts a lifecycle and an optional version.

### Annotate functions and operators

Use the `@FunctionAppliesTo` annotation within the `@FunctionInfo` annotation on function and operator classes to specify the lifecycle and version for functions and operators.

For example, to indicate that a function is in technical preview and applies to version 9.0.0, you would use:

```java
@FunctionInfo(
    returnType = "boolean",
    preview = true, // this marks the feature as preview in serverless
    appliesTo = {
        @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.0.0")
    },
    ...
)
```

When a feature evolves from preview in `9.0` to GA in `9.2`, add a new entry alongside the existing preview entry and remove the `preview = true` boolean:

```java
@FunctionInfo(
    returnType = "boolean",
    preview = false, //  the preview boolean can be removed (or flipped to false) when the function becomes GA
    appliesTo = {
        @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.0.0"),
        @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.2.0")
    },
    ...
)
```

You can also use `applies_to` for `Param`, `MapParam`, and `MapParamEntry`. In this case `applies_to` is a plain text field that accepts the same format used in [inline](https://elastic.github.io/docs-builder/syntax/applies/#inline-examples-by-product) `applies_to` metadata.
For example, to mark a specific parameter as preview:

```java
@Param(name = "query", type = { "keyword" }, description = "The query.", applies_to = "stack: preview 9.4.0")
```

To mark a whole map parameter as preview:

```java
@MapParam(
    name = "options",
    description = "Optional parameters.",
    optional = true,
    applies_to = "stack: preview 9.5.0",
    params = { ... }
)
```

To mark a specific map entry as preview:

```java
@MapParam(
    name = "options",
    description = "Optional parameters.",
    params = {
        @MapParamEntry(
            name = "my_option",
            type = "keyword",
            description = "Description of my_option.",
            applies_to = "stack: preview 9.2"
        )
    }
)
```

We updated [`DocsV3Support.java`](https://github.com/elastic/elasticsearch/blob/main/x-pack/plugin/esql/qa/testFixtures/src/main/java/org/elasticsearch/xpack/esql/expression/function/DocsV3Support.java) to generate the `applies_to` metadata correctly for functions and operators.

### Use inline applies_to metadata

Use [inline annotations](https://elastic.github.io/docs-builder/syntax/applies/#inline-annotations) to specify `applies_to` metadata in descriptions, parameter lists, etc.

For example, the second item in this list is in technical preview as of version 9.2:

```markdown
- Item 1
- Item 2 {applies_to}`stack: preview 9.2.`
```

### Follow these rules

1. **Use the `preview = true` boolean** for any tech preview feature - this is required for the Kibana inline docs
   - **Remove `preview = true`** only when the feature becomes GA on serverless and is _definitely_ going GA in the next minor release
2. **Never delete `appliesTo` entries** - only add new ones as features evolve from preview to GA
3. **Use specific versions** (`9.0.0`, `9.1.0`) when known, or just `PREVIEW` without a version if timing is uncertain
4. **Add `applies_to` to examples** where necessary

> [!IMPORTANT]
> We don't use `applies_to` in the legacy asciidoc system for 8.x and earlier versions.

### Choose a lifecycle value

- `PREVIEW` - Feature is in technical preview
- `GA` - Feature is generally available
- `DEPRECATED` - Feature is deprecated and will be removed in a future release
- `UNAVAILABLE` - Feature is not available in the current version, but may be available in future releases

> [!NOTE]
> Unreleased version information is automatically sanitized in the docs build output. For example, say you specify `preview 9.3.0`:
> - Before `9.3.0` is released, the live documentation will display "Planned for a future release" instead of the specific version number.
>  - This will be updated automatically when the version is released.

### Update list files

List entries in [`_snippets/lists/`](https://github.com/elastic/elasticsearch/tree/main/docs/reference/query-languages/esql/_snippets/lists) are always maintained by hand — they are never auto-generated. Each entry can carry inline `{applies_to}` tags.

> [!IMPORTANT]
> `stack` tags are cumulative — never remove a `stack: preview` tag when adding a `stack: ga` tag. The `serverless: preview` tag is different: drop it when the feature becomes GA.

A few formatting rules:
- Use `9.x` format, not `9.x.0` — strip the patch version
- Omit `serverless: ga` — GA is the default on serverless
- Drop `serverless: preview` only when the feature is GA, then add `stack: ga X.Y`
- No tag needed for anything that was GA before 9.0

#### Promote a feature to GA

**Functions and operators:**

1. In the Java class, add a `GA` entry to `appliesTo` and remove (or set to `false`) `preview = true`:
   ```diff
   @FunctionInfo(
   -   preview = true,
       appliesTo = {
           @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.0.0"),
   +       @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.2.0")
       },
   )
   ```
2. Run the function's tests to regenerate its docs snippets:
   ```
   ./gradlew :x-pack:plugin:esql:test -Dtests.class='MyFuncTests'
   ```
3. Update the list entry: add the `stack: ga X.Y` tag and drop `serverless: preview`:
   ```diff
   - * [`MY_FUNC`](path/to/my-func.md) {applies_to}`stack: preview 9.0` {applies_to}`serverless: preview`
   + * [`MY_FUNC`](path/to/my-func.md) {applies_to}`stack: preview 9.0` {applies_to}`stack: ga 9.2`
   ```

**Commands:**

1. Update the `applies_to` front matter in `_snippets/commands/layout/<my_command>.md`:
   ```diff
   - stack: preview 9.1.0
   + stack: preview 9.1.0, ga 9.3.0
   ```
2. Update the list entry in [`_snippets/lists/processing-commands.md`](https://github.com/elastic/elasticsearch/blob/main/docs/reference/query-languages/esql/_snippets/lists/processing-commands.md) (or [`source-commands.md`](https://github.com/elastic/elasticsearch/blob/main/docs/reference/query-languages/esql/_snippets/lists/source-commands.md)):
   ```diff
   - * [`MY_COMMAND`](/reference/.../my-command.md) {applies_to}`stack: preview 9.1` {applies_to}`serverless: preview`
   + * [`MY_COMMAND`](/reference/.../my-command.md) {applies_to}`stack: preview 9.1` {applies_to}`stack: ga 9.3`
   ```

## Publish your docs

[`docs/reference/query-languages/toc.yml`](https://github.com/elastic/elasticsearch/blob/main/docs/reference/query-languages/toc.yml) is the source of truth for what gets published. A page that exists on disk but isn't listed there won't appear in the navigation or be published.

### Hide a page before it's ready

To merge docs before a feature is publicly available, use `hidden` instead of `file` in `toc.yml`:

```yaml
- hidden: commands/my-command.md
```

Also comment out the corresponding entry in [`_snippets/lists/`](https://github.com/elastic/elasticsearch/tree/main/docs/reference/query-languages/esql/_snippets/lists) with a `%` prefix:

```markdown
% * [My Command](../commands/my-command.md)
```

> [!NOTE]
> The page stays accessible via direct URL but won't appear in navigation or search. You don't need to change any `.md` content files. See [#142859](https://github.com/elastic/elasticsearch/pull/142859/changes) for an example.

When you're ready to publish, flip both:

```diff
# toc.yml
- - hidden: commands/my-command.md
+ - file: commands/my-command.md

# _snippets/lists/
- % * [My Command](../commands/my-command.md)
+ * [My Command](../commands/my-command.md)
```

> [!TIP]
> If the feature isn't yet available on stack, use `applies_to` version metadata rather than hiding the page. See [Add version metadata](#add-version-metadata).

## Regenerate content

All generated content is produced by running ESQL tests. The section below covers what to run and when.

### Functions and operators

Run a single function's tests to regenerate its snippets (layout, description, parameters, types, examples, syntax diagrams, and Kibana definitions):

```
./gradlew :x-pack:plugin:esql:test -Dtests.class='CaseTests'
```

To regenerate everything for all functions and operators:

```
./gradlew :x-pack:plugin:esql:test
```

### Settings

Query settings (see [SET](commands/set.md)) are documented in [`_snippets/commands/settings/`](https://github.com/elastic/elasticsearch/tree/main/docs/reference/query-languages/esql/_snippets/commands/settings). To regenerate, run `QuerySettingsTests` in the `x-pack/plugin/esql` module. Only settings with `snapshot=false` are included.

## Understand how generated content works

There are three overlapping mechanisms for generating the content:
* The `AbstractFunctionTestCase` class generates the content for all the functions and most operators.
  This class makes use of the `DocsV3Support` class to generate the content.
  It uses the `@FunctionInfo` and `@Param` annotations on function and operator classes to know what content should be generated.
  All tests that extend this class will automatically generate the content for the functions they test.
* Some operators do not have a clear class or test class, and so the content is generated by custom
  tests that do not extend the `AbstractOperatorTestCase` class. See, for example, operators such as `Cast ::`,
  which uses `CastOperatorTests` to call directly into the `DocsV3Support` class to generate the content.
* Commands do not have dedicated classes or test classes with annotation that can be used.
  For this reason, the command documentation is generated by the `CommandDocsTests` class.
  Currently, this only covers tested examples used in the documentation, and all other commands
  content is static.
  Since there are no annotations to mark which examples to use, the command documentation
  relies on the docs author providing the knowledge of which examples to use by creating subdirectories
  and examples files that match the csv-spec files and tags to include.

To help differentiate between the static and generated content, the generated content is prefixed with a comment:
```
% This is generated by ESQL's AbstractFunctionTestCase. Do no edit it. See ../README.md for how to regenerate it.
```

## Generate PromQL definitions

PromQL documentation is generated separately and stored in:
```
docs/reference/query-languages/promql/kibana/definitions/*.json
```
For PromQL function documentation, see: https://prometheus.io/docs/prometheus/latest/querying/functions/

To generate the PromQL definition files, run:

```bash
./gradlew :x-pack:plugin:esql:test --tests "PromqlKibanaDefinitionGeneratorTests"
```
The result will be in `x-pack/plugin/esql/build/testrun/test/temp/promql/kibana/definitions/`.

## Elastic docs resources

- [How to contribute to Elastic docs](https://www.elastic.co/docs/contribute-docs)
- [Elastic markdown syntax reference](https://www.elastic.co/docs/contribute-docs/syntax-quick-reference)
- [Docs tools](https://www.elastic.co/docs/contribute-docs/tools)
