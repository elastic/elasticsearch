/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class DocsV3SupportTests extends ESTestCase {
    private static DocsV3Support docs = DocsV3Support.forFunctions("test", DocsV3SupportTests.class);
    private static final String ESQL = "/reference/query-languages/esql";

    public void testFunctionLink() {
        String text = "The value that is greater than half of all values and less than half of all values, "
            + "also known as the 50% <<esql-percentile>>.";
        String expected = "The value that is greater than half of all values and less than half of all values, also known as the 50% "
            + "[`PERCENTILE`]("
            + ESQL
            + "/functions-operators/aggregation-functions.md#esql-percentile).";
        assertThat(docs.replaceLinks(text), equalTo(expected));
    }

    public void testFunctionHeaderLink() {
        String text = "Combine `BUCKET` with an <<esql-aggregation-functions,aggregation>> to create";
        String expected = "Combine `BUCKET` with an [aggregation](" + ESQL + "/functions-operators/aggregation-functions.md) to create";
        assertThat(docs.replaceLinks(text), equalTo(expected));
    }

    public void testSpatialFunctionLink() {
        String text = "This is the inverse of the <<esql-st_disjoint,ST_DISJOINT>> function";
        String expected = "This is the inverse of the [ST_DISJOINT]("
            + ESQL
            + "/functions-operators/spatial-functions.md#esql-st_disjoint) function";
        assertThat(docs.replaceLinks(text), equalTo(expected));
    }

    public void testStringFunctionLink() {
        String text = "a known order like <<esql-split>>.";
        String expected = "a known order like [`SPLIT`](" + ESQL + "/functions-operators/string-functions.md#esql-split).";
        assertThat(docs.replaceLinks(text), equalTo(expected));
    }

    public void testOperatorLink() {
        String text = "If you need floating point division, <<esql-cast-operator>> one of the arguments to a `DOUBLE`.";
        String expected = """
            If you need floating point division,
            [`Cast (::)`](ESQL/functions-operators/operators.md#esql-cast-operator)
            one of the arguments to a `DOUBLE`.""".replaceAll("ESQL", ESQL).replaceAll("\n", " ");
        assertThat(docs.replaceLinks(text), equalTo(expected));
    }

    public void testCommandLink() {
        String text = "use a <<esql-where>> command to remove rows";
        String expected = "use a [`WHERE`](" + ESQL + "/commands/processing-commands.md#esql-where) command to remove rows";
        assertThat(docs.replaceLinks(text), equalTo(expected));
    }

    public void testStatsCommandLink() {
        String text = "Combine `DATE_TRUNC` with <<esql-stats-by>>";
        String expected = "Combine `DATE_TRUNC` with [`STATS`](" + ESQL + "/commands/processing-commands.md#esql-stats-by)";
        assertThat(docs.replaceLinks(text), equalTo(expected));
    }

    public void testFunctionAndHeaderLinks() {
        String text = "Like <<esql-percentile>>, `MEDIAN` is <<esql-percentile-approximate,usually approximate>>.";
        String expected = "Like [`PERCENTILE`]("
            + ESQL
            + "/functions-operators/aggregation-functions.md#esql-percentile), "
            + "`MEDIAN` is [usually approximate]("
            + ESQL
            + "/functions-operators/aggregation-functions.md#esql-percentile-approximate).";
        assertThat(docs.replaceLinks(text), equalTo(expected));
    }

    public void testWikipediaMacro() {
        String text = "Returns the {wikipedia}/Inverse_trigonometric_functions[arccosine] of `n` as an angle, expressed in radians.";
        String expected = "Returns the [arccosine](https://en.wikipedia.org/wiki/Inverse_trigonometric_functions) "
            + "of `n` as an angle, expressed in radians.";
        assertThat(docs.replaceLinks(text), equalTo(expected));
    }

    public void testWikipediaMacro2() {
        String text = "This builds on the three-valued logic ({wikipedia}/Three-valued_logic[3VL]) of the language.";
        String expected =
            "This builds on the three-valued logic ([3VL](https://en.wikipedia.org/wiki/Three-valued_logic)) of the language.";
        assertThat(docs.replaceLinks(text), equalTo(expected));
    }

    public void testJavadocMacro() {
        String text = "This is a noop for `long` (including unsigned) and `integer`.\n"
            + "For `double` this picks the closest `double` value to the integer similar to\n"
            + "{javadoc}/java.base/java/lang/Math.html#ceil(double)[Math.ceil].";
        String expected = "This is a noop for `long` (including unsigned) and `integer`.\n"
            + "For `double` this picks the closest `double` value to the integer similar to\n"
            + "[Math.ceil](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Math.html#ceil(double)).";
        assertThat(docs.replaceLinks(text), equalTo(expected));
    }

    public void testJavadoc8Macro() {
        String text = "Refer to {javadoc8}/java/time/temporal/ChronoField.html[java.time.temporal.ChronoField]";
        String expected =
            "Refer to [java.time.temporal.ChronoField](https://docs.oracle.com/javase/8/docs/api/java/time/temporal/ChronoField.html)";
        assertThat(docs.replaceLinks(text), equalTo(expected));
    }

    public void testJavadoc8MacroLongText() {
        String text = """
            Part of the date to extract.\n
            Can be: `aligned_day_of_week_in_month`, `aligned_day_of_week_in_year`, `aligned_week_of_month`, `aligned_week_of_year`,
            `ampm_of_day`, `clock_hour_of_ampm`, `clock_hour_of_day`, `day_of_month`, `day_of_week`, `day_of_year`, `epoch_day`,
            `era`, `hour_of_ampm`, `hour_of_day`, `instant_seconds`, `micro_of_day`, `micro_of_second`, `milli_of_day`,
            `milli_of_second`, `minute_of_day`, `minute_of_hour`, `month_of_year`, `nano_of_day`, `nano_of_second`,
            `offset_seconds`, `proleptic_month`, `second_of_day`, `second_of_minute`, `year`, or `year_of_era`.
            Refer to {javadoc8}/java/time/temporal/ChronoField.html[java.time.temporal.ChronoField]
            for a description of these values.\n
            If `null`, the function returns `null`.""";
        String expected = """
            Part of the date to extract.\n
            Can be: `aligned_day_of_week_in_month`, `aligned_day_of_week_in_year`, `aligned_week_of_month`, `aligned_week_of_year`,
            `ampm_of_day`, `clock_hour_of_ampm`, `clock_hour_of_day`, `day_of_month`, `day_of_week`, `day_of_year`, `epoch_day`,
            `era`, `hour_of_ampm`, `hour_of_day`, `instant_seconds`, `micro_of_day`, `micro_of_second`, `milli_of_day`,
            `milli_of_second`, `minute_of_day`, `minute_of_hour`, `month_of_year`, `nano_of_day`, `nano_of_second`,
            `offset_seconds`, `proleptic_month`, `second_of_day`, `second_of_minute`, `year`, or `year_of_era`.
            Refer to [java.time.temporal.ChronoField](https://docs.oracle.com/javase/8/docs/api/java/time/temporal/ChronoField.html)
            for a description of these values.\n
            If `null`, the function returns `null`.""";
        assertThat(docs.replaceLinks(text), equalTo(expected));
    }

    public void testExampleLoadMacro() {
        String text = "<<load-esql-example, file=string tag=rlikeEscapingSingleQuotes>>";
        String expected = """
            ```esql
            ROW message = "foo ( bar"
            | WHERE message RLIKE "foo \\\\( bar"
            ```
            """;
        assertThat(docs.replaceLinks(text), equalTo(expected));
    }

    public void testKnownRootEsqlFiles() {
        String text = """
            The order that <<esql-multivalued-fields, multivalued fields>>
            are read from underlying storage is not guaranteed""";
        String expected = """
            The order that [multivalued fields](/reference/query-languages/esql/esql-multivalued-fields.md)
            are read from underlying storage is not guaranteed""";
        assertThat(docs.replaceLinks(text), equalTo(expected));
    }

    public void testKnownRootWithTag() {
        String text = """
            Match can use <<esql-function-named-params,function named parameters>>
            to specify additional options for the match query.""";
        String expected = """
            Match can use [function named parameters](/reference/query-languages/esql/esql-syntax.md#esql-function-named-params)
            to specify additional options for the match query.""";
        assertThat(docs.replaceLinks(text), equalTo(expected));
    }

    public void testRenderingExampleRaw() throws IOException {
        String expectedExample = """
            ROW wkt = "POINT(42.97109630194 14.7552534413725)"
            | EVAL pt = TO_GEOPOINT(wkt)""";
        String example = docs.loadExample("spatial.csv-spec", "to_geopoint-str");
        assertThat(example, equalTo(expectedExample));
    }

    public void testRenderingExampleResultRaw() throws IOException {
        String expectedResults = """
            | wkt:keyword | pt:geo_point |
            | --- | --- |
            | "POINT(42.97109630194 14.7552534413725)" | POINT(42.97109630194 14.7552534413725) |
            """;
        String results = docs.loadExample("spatial.csv-spec", "to_geopoint-str-result");
        assertThat(results, equalTo(expectedResults));
    }

    public void testRenderingExampleRaw2() throws IOException {
        String expectedExample = """
            ROW n=1
            | STATS COUNT(n > 0 OR NULL), COUNT(n < 0 OR NULL)""";
        String example = docs.loadExample("stats", "count-or-null");
        assertThat(example, equalTo(expectedExample));
    }

    public void testRenderingExampleResultRaw2() throws IOException {
        String expectedResults = """
            | COUNT(n > 0 OR NULL):long | COUNT(n < 0 OR NULL):long |
            | --- | --- |
            | 1 | 0 |
            """;
        String results = docs.loadExample("stats", "count-or-null-result");
        assertThat(results, equalTo(expectedResults));
    }

    public void testRenderingExampleEmojis() throws IOException {
        String expectedResults = "ROW bending_arts = \"💧🪨🔥💨\" | EVAL bending_arts_reversed = REVERSE(bending_arts);";
        String results = docs.loadExample("string", "reverseEmoji");
        assertThat(results, equalTo(expectedResults));
    }

    public void testRenderingExampleResultEmojis() throws IOException {
        String expectedResults = """
            | bending_arts:keyword | bending_arts_reversed:keyword |
            | --- | --- |
            | 💧🪨🔥💨 | 💨🔥🪨💧 |
            """;
        String results = docs.loadExample("string", "reverseEmoji-result");
        assertThat(results, equalTo(expectedResults));
    }

    public void testRenderingExampleFromClass() throws Exception {
        String expected = """
            % This is generated by ESQL's AbstractFunctionTestCase. Do no edit it. See ../README.md for how to regenerate it.

            **Examples**

            ```esql
            FROM employees
            | STATS COUNT(height)
            ```

            | COUNT(height):long |
            | --- |
            | 100 |

            To count the number of rows, use `COUNT()` or `COUNT(*)`

            ```esql
            FROM employees
            | STATS count = COUNT(*) BY languages
            | SORT languages DESC
            ```

            | count:long | languages:integer |
            | --- | --- |
            | 10 | null |
            | 21 | 5 |
            | 18 | 4 |
            | 17 | 3 |
            | 19 | 2 |
            | 15 | 1 |

            The expression can use inline functions. This example splits a string into multiple values
            using the `SPLIT` function and counts the values

            ```esql
            ROW words="foo;bar;baz;qux;quux;foo"
            | STATS word_count = COUNT(SPLIT(words, ";"))
            ```

            | word_count:long |
            | --- |
            | 6 |

            To count the number of times an expression returns `TRUE` use a
            [`WHERE`](/reference/query-languages/esql/commands/processing-commands.md#esql-where) command
            to remove rows that shouldn't be included

            ```esql
            ROW n=1
            | WHERE n < 0
            | STATS COUNT(n)
            ```

            | COUNT(n):long |
            | --- |
            | 0 |

            To count the same stream of data based on two different expressions use the pattern
            `COUNT(<expression> OR NULL)`. This builds on the three-valued logic
            ([3VL](https://en.wikipedia.org/wiki/Three-valued_logic)) of the language:
            `TRUE OR NULL` is `TRUE`, but `FALSE OR NULL` is `NULL`, plus the way COUNT handles
            `NULL`s: `COUNT(TRUE)` and `COUNT(FALSE)` are both 1, but `COUNT(NULL)` is 0.

            ```esql
            ROW n=1
            | STATS COUNT(n > 0 OR NULL), COUNT(n < 0 OR NULL)
            ```

            | COUNT(n > 0 OR NULL):long | COUNT(n < 0 OR NULL):long |
            | --- | --- |
            | 1 | 0 |
            """;
        TestDocsFileWriter tempFileWriter = renderTestClassDocs();
        String rendered = tempFileWriter.rendered.get("examples/count.md");
        assertThat(rendered.trim(), equalTo(expected.trim()));
    }

    public void testRenderingLayoutFromClass() throws Exception {
        String expected = """
            % This is generated by ESQL's AbstractFunctionTestCase. Do no edit it. See ../README.md for how to regenerate it.

            ## `COUNT` [esql-count]
            ```{applies_to}
            stack: coming 9.1.0
            ```

            **Syntax**

            :::{image} ../../../images/functions/count.svg
            :alt: Embedded
            :class: text-center
            :::


            :::{include} ../parameters/count.md
            :::

            :::{include} ../description/count.md
            :::

            :::{include} ../types/count.md
            :::

            :::{include} ../examples/count.md
            :::
            """;
        TestDocsFileWriter tempFileWriter = renderTestClassDocs();
        String rendered = tempFileWriter.rendered.get("layout/count.md");
        assertThat(rendered.trim(), equalTo(expected.trim()));
    }

    private TestDocsFileWriter renderTestClassDocs() throws Exception {
        FunctionInfo info = functionInfo(TestClass.class);
        assert info != null;
        FunctionDefinition definition = EsqlFunctionRegistry.def(TestClass.class, TestClass::new, "count");
        var docs = new DocsV3Support.FunctionDocsSupport("count", TestClass.class, definition, TestClass::signatures);
        TestDocsFileWriter tempFileWriter = new TestDocsFileWriter("count");
        docs.setTempFileWriter(tempFileWriter);
        docs.renderDocs();
        return tempFileWriter;
    }

    private class TestDocsFileWriter implements DocsV3Support.TempFileWriter {
        private final String name;
        private final Map<String, String> rendered = new HashMap<>();

        TestDocsFileWriter(String name) {
            this.name = name;
        }

        @Override
        public void writeToTempDir(Path dir, String extension, String str) throws IOException {
            String file = dir.getFileName() + "/" + name + "." + extension;
            rendered.put(file, str);
            logger.info("Wrote to file: {}", file);
        }
    }

    private static FunctionInfo functionInfo(Class<?> clazz) {
        Constructor<?> constructor = constructorFor(clazz);
        if (constructor == null) {
            return null;
        }
        return constructor.getAnnotation(FunctionInfo.class);
    }

    private static Constructor<?> constructorFor(Class<?> clazz) {
        Constructor<?>[] constructors = clazz.getConstructors();
        if (constructors.length == 0) {
            return null;
        }
        if (constructors.length > 1) {
            for (Constructor<?> constructor : constructors) {
                if (constructor.getAnnotation(FunctionInfo.class) != null) {
                    return constructor;
                }
            }
        }
        return constructors[0];
    }

    public static class TestClass extends Function {
        @FunctionInfo(
            returnType = "long",
            description = "Returns the total number (count) of input values.",
            type = FunctionType.AGGREGATE,
            examples = {
                @Example(file = "stats", tag = "count"),
                @Example(description = "To count the number of rows, use `COUNT()` or `COUNT(*)`", file = "docs", tag = "countAll"),
                @Example(description = """
                    The expression can use inline functions. This example splits a string into multiple values
                    using the `SPLIT` function and counts the values""", file = "stats", tag = "docsCountWithExpression"),
                @Example(description = """
                    To count the number of times an expression returns `TRUE` use a
                    <<esql-where>> command
                    to remove rows that shouldn't be included""", file = "stats", tag = "count-where"),
                @Example(
                    description = """
                        To count the same stream of data based on two different expressions use the pattern
                        `COUNT(<expression> OR NULL)`. This builds on the three-valued logic
                        ({wikipedia}/Three-valued_logic[3VL]) of the language:
                        `TRUE OR NULL` is `TRUE`, but `FALSE OR NULL` is `NULL`, plus the way COUNT handles
                        `NULL`s: `COUNT(TRUE)` and `COUNT(FALSE)` are both 1, but `COUNT(NULL)` is 0.""",
                    file = "stats",
                    tag = "count-or-null"
                ) },
            appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.COMING, version = "9.1.0") }
        )
        public TestClass(Source source, @Param(name = "str", type = { "keyword", "text" }, description = """
            String expression. If `null`, the function returns `null`.
            The input can be a single- or multi-valued column or an expression.""") Expression field) {
            super(source, List.of(field));
        }

        public static Map<List<DataType>, DataType> signatures() {
            return Map.of(List.of(DataType.KEYWORD), DataType.LONG);
        }

        @Override
        public DataType dataType() {
            return DataType.LONG;
        }

        @Override
        public Expression replaceChildren(List<Expression> newChildren) {
            return new TestClass(source(), newChildren.getFirst());
        }

        @Override
        protected NodeInfo<? extends Expression> info() {
            return NodeInfo.create(this, TestClass::new, children().getFirst());
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }
    }
}
