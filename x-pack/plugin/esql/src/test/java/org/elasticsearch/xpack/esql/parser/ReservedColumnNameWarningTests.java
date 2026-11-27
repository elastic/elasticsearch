/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsIdentifier;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.reservedColumnNameWarning;

/**
 * Tests that the parser warns when a query explicitly names a new column with a leading {@code _}, and only then: referencing
 * such a column, or letting a command pick a default name like {@code RERANK}'s {@code _score}, stays silent.
 */
public class ReservedColumnNameWarningTests extends AbstractStatementParserTests {

    public void testEval() {
        assertWarnsAt("ROW a = 1 | EVAL _x = a", "_x");
    }

    public void testEvalOverwritingInternalColumn() {
        assertWarnsAt("FROM foo METADATA _score | EVAL _score = ROUND(_score, 2)", "_score =", "_score");
    }

    public void testDoubleUnderscore() {
        assertWarnsAt("ROW a = 1 | EVAL __x = a", "__x");
    }

    public void testRow() {
        assertWarnsAt("ROW _a = 1, b = 2", "_a");
    }

    public void testStatsAggregate() {
        assertWarnsAt("FROM foo | STATS _c = COUNT(*) BY g = a", "_c");
    }

    public void testStatsGrouping() {
        assertWarnsAt("FROM foo | STATS c = COUNT(*) BY _g = a", "_g");
    }

    /**
     * A query defining several {@code _} names gets a single warning, which points at the first one and lists the others in
     * the order they appear in the query.
     */
    public void testOneWarningListsAllNames() {
        String query = "ROW a = 1 | EVAL b = a, _x = a, _y = a | STATS _c = COUNT(*) BY _g = _x | RENAME _c AS _d";
        TEST_PARSER.parseQuery(query);
        assertWarnings(
            warning(query, "_x", "_x")
                + "; other column names starting with [_]: "
                + String.join(
                    ", ",
                    other(query, "_y", "_y"),
                    other(query, "_c =", "_c"),
                    other(query, "_g", "_g"),
                    other(query, "_d", "_d")
                )
        );
    }

    /**
     * The same name defined twice is listed at both places.
     */
    public void testSameNameTwice() {
        String query = "ROW a = 1 | EVAL _x = a | EVAL _x = _x + 1";
        TEST_PARSER.parseQuery(query);
        assertWarnings(warning(query, "_x", "_x") + "; other column names starting with [_]: " + other(query, "_x =", 1, "_x"));
    }

    /**
     * The list of other names is capped, so a query with very many of them doesn't produce a huge header.
     */
    public void testListOfOtherNamesIsCapped() {
        int count = ExpressionBuilder.MAX_LISTED_RESERVED_COLUMN_NAMES + 3;
        List<String> fields = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            fields.add("_f" + i + " = " + i);
        }
        String query = "ROW " + String.join(", ", fields);
        TEST_PARSER.parseQuery(query);
        List<String> others = new ArrayList<>();
        for (int i = 1; i <= ExpressionBuilder.MAX_LISTED_RESERVED_COLUMN_NAMES; i++) {
            others.add(other(query, "_f" + i + " ", "_f" + i));
        }
        assertWarnings(
            warning(query, "_f0", "_f0") + "; other column names starting with [_]: " + String.join(", ", others) + " and [2] more"
        );
    }

    public void testInlineStats() {
        assertWarnsAt("FROM foo | INLINE STATS _c = COUNT(*) BY a", "_c");
    }

    public void testRenameAs() {
        assertWarnsAt("FROM foo | RENAME a AS _b", "_b");
    }

    public void testRenameAssign() {
        assertWarnsAt("FROM foo | RENAME _b = a", "_b");
    }

    public void testDissect() {
        assertWarnsAt("FROM foo | DISSECT a \"%{_x} %{y}\"", "\"%{_x}", "_x");
    }

    public void testGrok() {
        assertWarnsAt("FROM foo | GROK a \"%{WORD:_x} %{WORD:y}\"", "\"%{WORD:_x}", "_x");
    }

    public void testGrokMultiplePatterns() {
        String query = "FROM foo | GROK a \"%{WORD:x}\", \"%{NUMBER:_n}\"";
        TEST_PARSER.parseQuery(query);
        assertWarnings(warning(query, "\"%{NUMBER:_n}", "_n"));
    }

    public void testEnrichWithRename() {
        assertWarnsAt("FROM foo | ENRICH policy ON a WITH _x = b", "_x");
    }

    public void testChangePointType() {
        assertWarnsAt("FROM foo | CHANGE_POINT v ON k AS _t, p", "_t");
    }

    public void testChangePointPvalue() {
        assertWarnsAt("FROM foo | CHANGE_POINT v ON k AS t, _p", "_p");
    }

    public void testRerankExplicitTarget() {
        assertWarnsAt("FROM foo | RERANK _s = \"q\" ON a WITH { \"inference_id\" : \"id\" }", "_s");
    }

    public void testCompletion() {
        assertWarnsAt("FROM foo | COMPLETION _c = a WITH { \"inference_id\" : \"id\" }", "_c");
    }

    public void testUriParts() {
        assertWarnsAt("FROM foo | URI_PARTS _u = a", "_u");
    }

    public void testRegisteredDomain() {
        assertWarnsAt("FROM foo | REGISTERED_DOMAIN _d = a", "_d");
    }

    public void testUserAgent() {
        assertWarnsAt("FROM foo | USER_AGENT _ua = a", "_ua");
    }

    public void testIpLocation() {
        assertWarnsAt("FROM foo | IP_LOCATION _g = a", "_g");
    }

    public void testHighlightPrefix() {
        assertWarnsAt("FROM foo | HIGHLIGHT prefix = \"_h\" \"q\" ON title", "\"_h\"", "_h");
    }

    public void testDenseVectorTarget() {
        String query = "DENSE_VECTOR _v = title WITH { \"inference_id\" : \"id\" }";
        processingCommand(query);
        assertWarnings(warning("row a = 1 | " + query, "_v", "_v"));
    }

    public void testPromqlValueName() {
        assertWarnsAt("PROMQL index=test step=5m _v=(foo)", "_v");
    }

    /**
     * A view's definition warns when the view is created or updated, so its author learns about such names, but not when a query
     * reads from the view, which would repeat the warning on every query and point into a definition the user may not own.
     */
    public void testViewDefinition() {
        String definition = "FROM foo | EVAL _x = a";
        parseView(definition, false);
        ensureNoWarnings();
        parseView(definition, true);
        assertWarnings(warning(definition, "_x", "_x"));
    }

    private static void parseView(String definition, boolean warnReservedColumnNames) {
        TEST_PARSER.parseView(
            definition,
            new QueryParams(),
            new InferenceSettings(Settings.EMPTY),
            "v",
            QueryPragmas.EMPTY,
            EsqlFlags.DEFAULTS,
            warnReservedColumnNames
        );
    }

    public void testParameterAsName() {
        String query = "ROW a = 1 | EVAL ?n = a";
        query(query, new QueryParams(List.of(paramAsIdentifier("n", "_x"))));
        assertWarnings(warning(query, "?n", "_x"));
    }

    /**
     * Referencing {@code _}-prefixed columns, defining nested names whose leaf starts with {@code _}, and using the default
     * names of commands must not warn.
     */
    public void testNoWarning() {
        query("FROM foo METADATA _score, _index | EVAL x = _score | KEEP _score, _index | STATS m = MAX(_score) BY _index");
        query("FROM foo | EVAL a._b = 1");
        query("FROM foo | EVAL _x + 1");
        query("FROM foo | ENRICH policy ON a WITH _x");
        query("FROM foo | RERANK \"q\" ON a WITH { \"inference_id\" : \"id\" }");
        query("FROM foo | COMPLETION a WITH { \"inference_id\" : \"id\" }");
        query("FROM foo | CHANGE_POINT v ON k");
        query("FROM foo | HIGHLIGHT \"q\" ON title");
        query("FROM foo | DISSECT a \"%{?_skipped} %{y}\"");
        query("FROM foo | FORK (WHERE a > 1) (WHERE a < 1)");
        ensureNoWarnings();
    }

    private void assertWarnsAt(String query, String name) {
        assertWarnsAt(query, name, name);
    }

    /**
     * Parses {@code query} and asserts that it emits exactly one warning, for column {@code name}, located where
     * {@code locator} first occurs in the query.
     */
    private void assertWarnsAt(String query, String locator, String name) {
        TEST_PARSER.parseQuery(query);
        assertWarnings(warning(query, locator, name));
    }

    private static String warning(String query, String locator, String name) {
        return reservedColumnNameWarning(1, column(query, locator, 0), name);
    }

    /**
     * How the warning lists a further name, located where {@code locator} first occurs in the query.
     */
    private static String other(String query, String locator, String name) {
        return other(query, locator, 0, name);
    }

    /**
     * How the warning lists a further name, located at the occurrence of {@code locator} after the first {@code skip} ones.
     */
    private static String other(String query, String locator, int skip, String name) {
        return "[" + name + "] at line 1:" + column(query, locator, skip);
    }

    private static int column(String query, String locator, int skip) {
        int index = query.indexOf(locator);
        for (int i = 0; i < skip && index >= 0; i++) {
            index = query.indexOf(locator, index + 1);
        }
        assertTrue("[" + locator + "] not found in [" + query + "]", index >= 0);
        return index + 1;
    }
}
