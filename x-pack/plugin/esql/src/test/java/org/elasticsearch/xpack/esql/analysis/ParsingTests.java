/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.LoadMapping;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.ParserUtils;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.QueryParam;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyPolicyResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.testAnalyzerContext;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexResolutions;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 * Parses a plan, builds an AST for it, and then runs logical analysis on it.
 * So if we don't error out in the process,  all references were resolved correctly.
 * Use this class if you want to test parsing and resolution of a query
 *  and especially if you expect to get a ParsingException
 */
public class ParsingTests extends ESTestCase {
    private static final String INDEX_NAME = "test";
    private static final EsqlParser parser = new EsqlParser();

    private final IndexResolution defaultIndex = loadIndexResolution("mapping-basic.json");
    private final Analyzer defaultAnalyzer = new Analyzer(
        testAnalyzerContext(
            TEST_CFG,
            new EsqlFunctionRegistry(),
            indexResolutions(defaultIndex),
            emptyPolicyResolution(),
            emptyInferenceResolution()
        ),
        TEST_VERIFIER
    );

    public void testCaseFunctionInvalidInputs() {
        assertEquals("1:22: error building [case]: expects at least two arguments", error("row a = 1 | eval x = case()"));
        assertEquals("1:22: error building [case]: expects at least two arguments", error("row a = 1 | eval x = case(a)"));
        assertEquals("1:22: error building [case]: expects at least two arguments", error("row a = 1 | eval x = case(1)"));
    }

    public void testConcatFunctionInvalidInputs() {
        assertEquals("1:22: error building [concat]: expects at least two arguments", error("row a = 1 | eval x = concat()"));
        assertEquals("1:22: error building [concat]: expects at least two arguments", error("row a = 1 | eval x = concat(a)"));
        assertEquals("1:22: error building [concat]: expects at least two arguments", error("row a = 1 | eval x = concat(1)"));
    }

    public void testCoalesceFunctionInvalidInputs() {
        assertEquals("1:22: error building [coalesce]: expects at least one argument", error("row a = 1 | eval x = coalesce()"));
    }

    public void testGreatestFunctionInvalidInputs() {
        assertEquals("1:22: error building [greatest]: expects at least one argument", error("row a = 1 | eval x = greatest()"));
    }

    public void testLeastFunctionInvalidInputs() {
        assertEquals("1:22: error building [least]: expects at least one argument", error("row a = 1 | eval x = least()"));
    }

    /**
     * Tests the inline cast syntax {@code <value>::<type>} for all supported types and
     * builds a little json report of the valid types.
     */
    public void testInlineCast() throws IOException {
        EsqlFunctionRegistry registry = new EsqlFunctionRegistry();
        Path dir = PathUtils.get(System.getProperty("java.io.tmpdir"))
            .resolve("query-languages")
            .resolve("esql")
            .resolve("kibana")
            .resolve("definition");
        Files.createDirectories(dir);
        Path file = dir.resolve("inline_cast.json");
        try (XContentBuilder report = new XContentBuilder(JsonXContent.jsonXContent, Files.newOutputStream(file))) {
            report.humanReadable(true).prettyPrint();
            report.startObject();
            List<String> namesAndAliases = new ArrayList<>(DataType.namesAndAliases());
            if (EsqlCapabilities.Cap.SPATIAL_GRID_TYPES.isEnabled() == false) {
                // Some types do not have a converter function if the capability is disabled
                namesAndAliases.removeAll(List.of("geohash", "geotile", "geohex"));
            }
            Collections.sort(namesAndAliases);
            for (String nameOrAlias : namesAndAliases) {
                DataType expectedType = DataType.fromNameOrAlias(nameOrAlias);
                if (EsqlDataTypeConverter.converterFunctionFactory(expectedType) == null) {
                    continue;
                }
                LogicalPlan plan = parser.createStatement("ROW a = 1::" + nameOrAlias);
                Row row = as(plan, Row.class);
                assertThat(row.fields(), hasSize(1));
                Function functionCall = (Function) row.fields().get(0).child();
                assertThat(functionCall.dataType(), equalTo(expectedType));
                report.field(nameOrAlias, registry.snapshotRegistry().functionName(functionCall.getClass()));
            }
            report.endObject();
        }
        logger.info("Wrote to file: {}", file);
    }

    public void testTooBigQuery() {
        StringBuilder query = new StringBuilder("FROM foo | EVAL a = a");
        while (query.length() < EsqlParser.MAX_LENGTH) {
            query.append(", a = CONCAT(a, a)");
        }
        assertEquals("-1:-1: ESQL statement is too large [1000011 characters > 1000000]", error(query.toString()));
    }

    public void testJoinOnConstant() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled()
        );
        assertEquals(
            "1:55: JOIN ON clause must be a comma separated list of fields or a single expression, found [123]",
            error("row languages = 1, gender = \"f\" | lookup join test on 123")
        );
        assertEquals(
            "1:55: JOIN ON clause must be a comma separated list of fields or a single expression, found [\"abc\"]",
            error("row languages = 1, gender = \"f\" | lookup join test on \"abc\"")
        );
        assertEquals(
            "1:55: JOIN ON clause must be a comma separated list of fields or a single expression, found [false]",
            error("row languages = 1, gender = \"f\" | lookup join test on false")
        );
    }

    public void testLookupJoinExpressionMixed() {
        assumeTrue("requires LOOKUP JOIN capability", EsqlCapabilities.Cap.JOIN_LOOKUP_V12.isEnabled());
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled()
        );
        String queryString = """
            from test
            | rename languages as languages_left
            | lookup join languages_lookup ON languages_left == language_code or salary > 1000
            """;

        assertEquals(
            "3:32: JOIN ON clause with expressions must contain at least one condition relating the left index and the lookup index",
            error(queryString)
        );
    }

    public void testLookupJoinExpressionOnlyRightFilter() {
        assumeTrue("requires LOOKUP JOIN capability", EsqlCapabilities.Cap.JOIN_LOOKUP_V12.isEnabled());
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled()
        );
        String queryString = """
            from test
            | rename languages as languages_left
            | lookup join languages_lookup ON salary > 1000
            """;

        assertEquals(
            "3:32: JOIN ON clause with expressions must contain at least one condition relating the left index and the lookup index",
            error(queryString)
        );
    }

    public void testLookupJoinExpressionFieldBasePlusRightFilterAnd() {
        assumeTrue("requires LOOKUP JOIN capability", EsqlCapabilities.Cap.JOIN_LOOKUP_V12.isEnabled());
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled()
        );
        String queryString = """
            from test
            | lookup join languages_lookup ON languages and salary > 1000
            """;

        assertEquals(
            "2:32: JOIN ON clause only supports fields or AND of Binary Expressions at the moment, found [languages]",
            error(queryString)
        );
    }

    public void testLookupJoinExpressionFieldBasePlusRightFilterComma() {
        assumeTrue("requires LOOKUP JOIN capability", EsqlCapabilities.Cap.JOIN_LOOKUP_V12.isEnabled());
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled()
        );
        String queryString = """
            from test
            | lookup join languages_lookup ON languages, salary > 1000
            """;

        assertEquals(
            "2:46: JOIN ON clause must be a comma separated list of fields or a single expression, found [salary > 1000]",
            error(queryString)
        );
    }

    public void testJoinTwiceOnTheSameField() {
        assertEquals(
            "1:66: JOIN ON clause does not support multiple fields with the same name, found multiple instances of [languages]",
            error("row languages = 1, gender = \"f\" | lookup join test on languages, languages")
        );
    }

    public void testJoinTwiceOnTheSameField_TwoLookups() {
        assertEquals(
            "1:108: JOIN ON clause does not support multiple fields with the same name, found multiple instances of [gender]",
            error("row languages = 1, gender = \"f\" | lookup join test on languages | eval x = 1 | lookup join test on gender, gender")
        );
    }

    public void testInvalidLimit() {
        assertLimitWithAndWithoutParams("foo", "\"foo\"", DataType.KEYWORD);
        assertLimitWithAndWithoutParams(1.2, "1.2", DataType.DOUBLE);
        assertLimitWithAndWithoutParams(-1, "-1", DataType.INTEGER);
        assertLimitWithAndWithoutParams(true, "true", DataType.BOOLEAN);
        assertLimitWithAndWithoutParams(false, "false", DataType.BOOLEAN);
        assertLimitWithAndWithoutParams(null, "null", DataType.NULL);
    }

    private void assertLimitWithAndWithoutParams(Object value, String valueText, DataType type) {
        assertEquals(
            "1:13: value of [limit "
                + valueText
                + "] must be a non negative integer, found value ["
                + valueText
                + "] type ["
                + type.typeName()
                + "]",
            error("row a = 1 | limit " + valueText)
        );

        assertEquals(
            "1:13: value of [limit ?param] must be a non negative integer, found value [?param] type [" + type.typeName() + "]",
            error(
                "row a = 1 | limit ?param",
                new QueryParams(List.of(new QueryParam("param", value, type, ParserUtils.ParamClassification.VALUE)))
            )
        );

    }

    public void testInvalidSample() {
        assertEquals(
            "1:13: invalid value for SAMPLE probability [foo], expecting a number between 0 and 1, exclusive",
            error("row a = 1 | sample \"foo\"")
        );
        assertEquals(
            "1:13: invalid value for SAMPLE probability [-1.0], expecting a number between 0 and 1, exclusive",
            error("row a = 1 | sample -1.0")
        );
        assertEquals(
            "1:13: invalid value for SAMPLE probability [0], expecting a number between 0 and 1, exclusive",
            error("row a = 1 | sample 0")
        );
        assertEquals(
            "1:13: invalid value for SAMPLE probability [1], expecting a number between 0 and 1, exclusive",
            error("row a = 1 | sample 1")
        );
    }

    public void testSet() {
        assumeTrue("SET command available in snapshot only", EsqlCapabilities.Cap.SET_COMMAND.isEnabled());
        EsqlStatement query = parse("SET foo = \"bar\"; row a = 1", new QueryParams());
        assertThat(query.plan(), is(instanceOf(Row.class)));
        assertThat(query.settings().size(), is(1));
        checkSetting(query, 0, "foo", BytesRefs.toBytesRef("bar"));

        query = parse("SET bar = 2; row a = 1 | eval x = 12", new QueryParams());
        assertThat(query.plan(), is(instanceOf(Eval.class)));
        assertThat(query.settings().size(), is(1));
        checkSetting(query, 0, "bar", 2);

        query = parse("SET bar = true; row a = 1 | eval x = 12", new QueryParams());
        assertThat(query.plan(), is(instanceOf(Eval.class)));
        assertThat(query.settings().size(), is(1));
        checkSetting(query, 0, "bar", true);

        expectThrows(ParsingException.class, () -> parse("SET foo = 1, bar = 2; row a = 1", new QueryParams()));
    }

    public void testSetWithTripleQuotes() {
        assumeTrue("SET command available in snapshot only", EsqlCapabilities.Cap.SET_COMMAND.isEnabled());
        EsqlStatement query = parse("SET foo = \"\"\"bar\"baz\"\"\"; row a = 1", new QueryParams());
        assertThat(query.plan(), is(instanceOf(Row.class)));
        assertThat(query.settings().size(), is(1));
        checkSetting(query, 0, "foo", BytesRefs.toBytesRef("bar\"baz"));

        query = parse("SET foo = \"\"\"bar\"\"\"\"; row a = 1", new QueryParams());
        assertThat(query.plan(), is(instanceOf(Row.class)));
        assertThat(query.settings().size(), is(1));
        checkSetting(query, 0, "foo", BytesRefs.toBytesRef("bar\""));

        query = parse("SET foo = \"\"\"\"bar\"\"\"; row a = 1 | LIMIT 3", new QueryParams());
        assertThat(query.plan(), is(instanceOf(Limit.class)));
        assertThat(query.settings().size(), is(1));
        checkSetting(query, 0, "foo", BytesRefs.toBytesRef("\"bar"));
    }

    public void testMultipleSet() {
        assumeTrue("SET command available in snapshot only", EsqlCapabilities.Cap.SET_COMMAND.isEnabled());
        EsqlStatement query = parse(
            "SET foo = \"bar\"; SET bar = 2; SET foo = \"baz\"; SET x = 3.5; SET y = false; SET z = null; row a = 1",
            new QueryParams()
        );
        assertThat(query.plan(), is(instanceOf(Row.class)));
        assertThat(query.settings().size(), is(6));

        checkSetting(query, 0, "foo", BytesRefs.toBytesRef("bar"), BytesRefs.toBytesRef("baz"));
        checkSetting(query, 1, "bar", 2);
        checkSetting(query, 2, "foo", BytesRefs.toBytesRef("baz"));
        checkSetting(query, 3, "x", 3.5);
        checkSetting(query, 4, "y", false);
        checkSetting(query, 5, "z", null);
    }

    public void testSetArrays() {
        assumeTrue("SET command available in snapshot only", EsqlCapabilities.Cap.SET_COMMAND.isEnabled());
        EsqlStatement query = parse("SET foo = [\"bar\", \"baz\"]; SET bar = [1, 2, 3]; row a = 1", new QueryParams());
        assertThat(query.plan(), is(instanceOf(Row.class)));
        assertThat(query.settings().size(), is(2));

        checkSetting(query, 0, "foo", List.of(BytesRefs.toBytesRef("bar"), BytesRefs.toBytesRef("baz")));
        checkSetting(query, 1, "bar", List.of(1, 2, 3));
    }

    public void testSetWithNamedParams() {
        assumeTrue("SET command available in snapshot only", EsqlCapabilities.Cap.SET_COMMAND.isEnabled());
        EsqlStatement query = parse(
            "SET foo = \"bar\"; SET bar = ?a; SET foo = \"baz\"; SET x = ?x; row a = 1",
            new QueryParams(
                List.of(
                    new QueryParam("a", 2, DataType.INTEGER, ParserUtils.ParamClassification.VALUE),
                    new QueryParam("x", 3.5, DataType.DOUBLE, ParserUtils.ParamClassification.VALUE)
                )
            )
        );
        assertThat(query.plan(), is(instanceOf(Row.class)));
        assertThat(query.settings().size(), is(4));

        checkSetting(query, 0, "foo", BytesRefs.toBytesRef("bar"), BytesRefs.toBytesRef("baz"));
        checkSetting(query, 1, "bar", 2);
        checkSetting(query, 2, "foo", BytesRefs.toBytesRef("baz"));
        checkSetting(query, 3, "x", 3.5);
    }

    public void testSetWithPositionalParams() {
        assumeTrue("SET command available in snapshot only", EsqlCapabilities.Cap.SET_COMMAND.isEnabled());
        EsqlStatement query = parse(
            "SET foo = \"bar\"; SET bar = ?; SET foo = \"baz\"; SET x = ?; row a = ?",
            new QueryParams(
                List.of(
                    new QueryParam("a", 2, DataType.INTEGER, ParserUtils.ParamClassification.VALUE),
                    new QueryParam("x", 3.5, DataType.DOUBLE, ParserUtils.ParamClassification.VALUE),
                    new QueryParam("y", 8, DataType.DOUBLE, ParserUtils.ParamClassification.VALUE)
                )
            )
        );
        assertThat(query.plan(), is(instanceOf(Row.class)));
        assertThat(((Row) query.plan()).fields().get(0).child().fold(FoldContext.small()), is(8));
        assertThat(query.settings().size(), is(4));

        checkSetting(query, 0, "foo", BytesRefs.toBytesRef("bar"), BytesRefs.toBytesRef("baz"));
        checkSetting(query, 1, "bar", 2);
        checkSetting(query, 2, "foo", BytesRefs.toBytesRef("baz"));
        checkSetting(query, 3, "x", 3.5);
    }

    /**
     * @param query    the query
     * @param position the order of the corresponding SET statement
     * @param name     the setting name
     * @param value    the setting value as it appears in the query at that position
     */
    private void checkSetting(EsqlStatement query, int position, String name, Object value) {
        checkSetting(query, position, name, value, value);
    }

    /**
     * @param query        the query
     * @param position     the order of the corresponding SET statement
     * @param name         the setting name
     * @param value        the setting value as it appears in the query at that position
     * @param maskingValue the final value you'll obtain if you use query.setting(name).
     *                     It could be different from value in case of name collisions in the query
     */
    private void checkSetting(EsqlStatement query, int position, String name, Object value, Object maskingValue) {
        assertThat(settingName(query, position), is(name));
        assertThat(settingValue(query, position), is(value));
        assertThat(query.setting(name).fold(FoldContext.small()), is(maskingValue));
    }

    private String settingName(EsqlStatement query, int position) {
        return query.settings().get(position).name();
    }

    private Object settingValue(EsqlStatement query, int position) {
        return query.settings().get(position).value().fold(FoldContext.small());
    }

    private String error(String query, QueryParams params) {
        ParsingException e = expectThrows(ParsingException.class, () -> defaultAnalyzer.analyze(parse(query, params).plan()));
        String message = e.getMessage();
        assertTrue(message.startsWith("line "));
        return message.substring("line ".length());
    }

    private EsqlStatement parse(String query, QueryParams params) {
        return parser.createQuery(query, params);
    }

    private String error(String query) {
        return error(query, new QueryParams());
    }

    private static IndexResolution loadIndexResolution(String name) {
        return IndexResolution.valid(EsIndexGenerator.esIndex(INDEX_NAME, LoadMapping.loadMapping(name)));
    }

}
