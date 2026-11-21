/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.capabilities.UnresolvedException;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.expression.AnyParam;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.plan.QuerySettingDef;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsIdentifier;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsPattern;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * A query parameter with the value that {@code param_any_value} reserves stands for "Any". Where it is an expression, the parser
 * builds an {@link AnyParam} marker for the analysis to deal with. Everywhere else, where nothing could drop or replace it, the
 * parser rejects it with an error that says so. These tests are about what the parser makes of it, which doesn't depend on
 * what later versions do with the marker.
 */
public class AnyParamParserTests extends ESTestCase {

    private static final String ANY = QuerySettings.PARAM_ANY_VALUE_DEFAULT;

    @BeforeClass
    public static void requireAnyParam() {
        assumeTrue("requires the Any controls", EsqlCapabilities.Cap.PARAM_ANY_VALUE.isEnabled());
    }

    private static List<AnyParam> markers(LogicalPlan plan) {
        List<AnyParam> markers = new ArrayList<>();
        plan.forEachExpressionDown(AnyParam.class, markers::add);
        return markers;
    }

    private static QueryParams params(QueryParam... params) {
        return new QueryParams(List.of(params));
    }

    private static AnyParam onlyMarker(String query, QueryParams params) {
        List<AnyParam> markers = markers(TEST_PARSER.parseQuery(query, params));
        assertThat(query, markers, hasSize(1));
        return markers.getFirst();
    }

    private static String parsingError(String query, QueryParams params) {
        return expectThrows(ParsingException.class, query, () -> TEST_PARSER.parseQuery(query, params)).getMessage();
    }

    private static String anyError(String parameter, String where) {
        return "Query parameter [" + parameter + "] has the value [" + ANY + "], which stands for \"Any\" and can't be used " + where;
    }

    public void testValueInAnExpressionIsAMarker() {
        AnyParam marker = onlyMarker("FROM t | WHERE a == ?x", params(paramAsConstant("x", ANY)));
        assertThat(marker.parameterText(), equalTo("?x"));
        assertThat(marker.field(), equalTo(false));
    }

    public void testAnonymousAndPositionalParametersAreMarkers() {
        assertThat(onlyMarker("FROM t | WHERE a == ?", params(paramAsConstant(null, ANY))).parameterText(), equalTo("?"));
        assertThat(onlyMarker("FROM t | WHERE a == ?1", params(paramAsConstant(null, ANY))).parameterText(), equalTo("?1"));
    }

    public void testIdentifierInAnExpressionIsAFieldMarker() {
        AnyParam marker = onlyMarker("FROM t | WHERE ?f == 1", params(paramAsIdentifier("f", ANY)));
        assertThat(marker.parameterText(), equalTo("?f"));
        assertThat(marker.field(), equalTo(true));
    }

    public void testDoubleParameterInAnExpressionIsAFieldMarker() {
        AnyParam marker = onlyMarker("FROM t | WHERE ??f == 1", params(paramAsConstant("f", ANY)));
        assertThat(marker.parameterText(), equalTo("??f"));
        assertThat(marker.field(), equalTo(true));
    }

    public void testMarkersInGroupingAndFunctionArguments() {
        assertThat(onlyMarker("FROM t | STATS BY ??g", params(paramAsConstant("g", ANY))).field(), equalTo(true));
        assertThat(onlyMarker("FROM t | STATS BY ?g", params(paramAsIdentifier("g", ANY))).field(), equalTo(true));
        assertThat(onlyMarker("FROM t | EVAL y = TO_UPPER(?x)", params(paramAsConstant("x", ANY))).field(), equalTo(false));
        assertThat(onlyMarker("FROM t | WHERE a IN (1, ?x)", params(paramAsConstant("x", ANY))).field(), equalTo(false));
    }

    public void testCastOperandIsRejected() {
        QueryParams params = params(paramAsConstant("x", ANY));
        for (String query : List.of(
            "FROM t | WHERE a == ?x::keyword",
            "FROM t | WHERE a == ??x::keyword",
            "FROM t | EVAL b = ?x::counter",
            "FROM t | EVAL b = ?x::gauge"
        )) {
            assertThat(query, parsingError(query, params), containsString(anyError(query.contains("??") ? "??x" : "?x", "in a cast")));
        }
    }

    public void testListWithOnlyTheReservedValueIsAMarker() {
        assertThat(onlyMarker("FROM t | WHERE a == ?x", params(paramAsConstant("x", List.of(ANY)))).parameterText(), equalTo("?x"));
    }

    public void testListWithMoreThanOneValueIsNotAMarker() {
        LogicalPlan plan = TEST_PARSER.parseQuery("FROM t | WHERE a == ?x", params(paramAsConstant("x", List.of(ANY, ANY))));
        assertThat(markers(plan), empty());
    }

    public void testPatternIsNotAMarker() {
        LogicalPlan plan = TEST_PARSER.parseQuery("FROM t | KEEP ?p", params(paramAsPattern("p", ANY)));
        assertThat(markers(plan), empty());
    }

    public void testOtherValuesAndTheLiteralAreNotMarkers() {
        assertThat(markers(TEST_PARSER.parseQuery("FROM t | WHERE a == ?x", params(paramAsConstant("x", "other")))), empty());
        assertThat(markers(TEST_PARSER.parseQuery("FROM t | WHERE a == ?x", params(paramAsConstant("x", 1)))), empty());
        assertThat(markers(TEST_PARSER.parseQuery("FROM t | WHERE a == \"" + ANY + "\"", new QueryParams())), empty());
    }

    public void testRequestValueChangesTheReservedValue() {
        var requestSettings = Map.<QuerySettingDef<?>, Object>of(QuerySettings.PARAM_ANY_VALUE, "*");
        LogicalPlan plan = TEST_PARSER.parse(
            "FROM t | WHERE a == ?x AND b == ?y",
            params(paramAsConstant("x", "*"), paramAsConstant("y", ANY)),
            new InferenceSettings(Settings.EMPTY),
            QueryPragmas.EMPTY,
            EsqlFlags.DEFAULTS,
            requestSettings
        ).plan();
        List<AnyParam> markers = markers(plan);
        assertThat(markers, hasSize(1));
        assertThat(markers.getFirst().parameterText(), equalTo("?x"));
    }

    public void testSetChangesTheReservedValue() {
        QueryParams params = params(paramAsConstant("x", "*"), paramAsConstant("y", ANY));
        LogicalPlan plan = TEST_PARSER.createStatement("SET param_any_value = \"*\"; FROM t | WHERE a == ?x AND b == ?y", params).plan();
        List<AnyParam> markers = markers(plan);
        assertThat(markers, hasSize(1));
        assertThat(markers.getFirst().parameterText(), equalTo("?x"));
    }

    public void testMarkerIsAnUnresolvableLeafThatCantBeFolded() {
        AnyParam marker = onlyMarker("FROM t | WHERE a == ?x", params(paramAsConstant("x", ANY)));
        expectThrows(UnresolvedException.class, marker::dataType);
        ParsingException e = expectThrows(ParsingException.class, () -> marker.fold(FoldContext.small()));
        assertThat(e.getMessage(), containsString(anyError("?x", "here")));
    }

    public void testConstantPositionsAreRejected() {
        QueryParams params = params(paramAsConstant("x", ANY));
        for (String query : List.of(
            "FROM t | LIMIT ?x",
            "FROM t | SAMPLE ?x",
            "FROM t | DISSECT a \"%{b}\" APPEND_SEPARATOR = ?x",
            "FROM t | WHERE a : ?x",
            "FROM t | WHERE MATCH(a, \"q\", {\"fuzziness\": ?x})"
        )) {
            assertThat(query, parsingError(query, params), containsString(anyError("?x", "in this position")));
        }
    }

    public void testSetValueIsRejected() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> TEST_PARSER.createStatement("SET unmapped_fields = ?x; FROM t", params(paramAsConstant("x", ANY)))
        );
        assertThat(e.getMessage(), containsString(anyError("?x", "in this position")));
    }

    public void testNamePositionsAreRejected() {
        QueryParams params = params(paramAsConstant("x", ANY), paramAsIdentifier("i", ANY));
        for (String query : List.of(
            "FROM t | KEEP ??x",
            "FROM t | KEEP ?i",
            "FROM t | KEEP t.??x",
            "FROM t | KEEP ??x.a",
            "FROM t | DROP ??x",
            "FROM t | MV_EXPAND ??x",
            "FROM t | RENAME a AS ??x",
            "FROM t | EVAL ??x = 1",
            "FROM t | EVAL ??x(a)",
            "FROM t | STATS ??x(a)"
        )) {
            assertThat(query, parsingError(query, params), containsString("has the value [" + ANY + "]"));
            assertThat(query, parsingError(query, params), containsString("as an identifier or pattern"));
        }
    }

    public void testPatternPositionsAreRejected() {
        QueryParams params = params(paramAsConstant("x", ANY));
        for (String query : List.of(
            "FROM t | WHERE a LIKE ?x",
            "FROM t | WHERE a RLIKE ?x",
            "FROM t | WHERE a LIKE (?x)",
            "FROM t | WHERE a LIKE (\"a*\", ?x)",
            "FROM t | WHERE a RLIKE (?x, \"a.*\")",
            "FROM t | WHERE a NOT LIKE ?x"
        )) {
            assertThat(query, parsingError(query, params), containsString(anyError("?x", "as a pattern")));
        }
    }

    public void testLikeListOfTheReservedValueIsRejectedBeforeTheListError() {
        QueryParams params = params(paramAsConstant("x", List.of(ANY)));
        assertThat(parsingError("FROM t | WHERE a LIKE (?x)", params), containsString(anyError("?x", "as a pattern")));
    }

    public void testLookupJoinConditionIsRejected() {
        assertThat(
            parsingError("FROM t | LOOKUP JOIN l ON a == ?x", params(paramAsConstant("x", ANY))),
            containsString(anyError("?x", "in a LOOKUP JOIN condition"))
        );
        assertThat(
            parsingError("FROM t | LOOKUP JOIN l ON a == b AND c == ?x", params(paramAsConstant("x", ANY))),
            containsString(anyError("?x", "in a LOOKUP JOIN condition"))
        );
    }

    public void testPromqlParametersAreRejected() {
        QueryParams params = params(paramAsConstant("p", ANY));
        for (String query : List.of(
            "PROMQL index=test step=?p (avg(foo))",
            "PROMQL index=?p step=5m (avg(foo))",
            "PROMQL index=test step=5m (foo{job=?p})",
            "PROMQL index=test step=5m rate(foo[?p])"
        )) {
            assertThat(query, parsingError(query, params), containsString(anyError("?p", "in a PromQL query")));
        }
    }

    public void testOtherErrorsOfParametersAreUnchanged() {
        assertThat(
            parsingError("FROM t | WHERE a == ?unknown", params(paramAsConstant("x", ANY))),
            containsString("Unknown query parameter [unknown]")
        );
        assertThat(
            parsingError("FROM t | KEEP ?x", params(paramAsConstant("x", "other"))),
            containsString("Query parameter [?x] with value [other] declared as a constant, cannot be used as an identifier")
        );
    }
}
