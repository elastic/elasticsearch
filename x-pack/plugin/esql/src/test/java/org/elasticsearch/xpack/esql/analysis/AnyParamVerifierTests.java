/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsIdentifier;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

/**
 * The parameters that stand for "Any" and that nothing drops or replaces stay in the plan as unresolvable markers, and the
 * {@code Verifier} reports them like any other unresolved expression: as a 400 that names the parameter, and never as the
 * server error that asking the type of an unresolved expression would be.
 */
public class AnyParamVerifierTests extends ESTestCase {

    private static final String ANY = QuerySettings.PARAM_ANY_VALUE_DEFAULT;

    /**
     * The analyzer here follows the build, and release builds don't apply the default value of the setting, so no parameter stands
     * for "Any" in them.
     */
    @BeforeClass
    public static void requireAnyParam() {
        assumeTrue("requires the Any query parameter", EsqlCapabilities.Cap.PARAM_ANY_VALUE_V1.isEnabled());
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    private static TestAnalyzer analyzer() {
        return EsqlTestUtils.analyzer().addDefaultIndex().stripErrorPrefix(true);
    }

    private static String anyMessage(String parameter, String where) {
        return "Query parameter [" + parameter + "] has the value [" + ANY + "], which stands for \"Any\" and can't be used " + where;
    }

    public void testMarkerInAComparisonIsReported() {
        analyzer().error(
            "FROM test | WHERE emp_no > ?x",
            containsString(anyMessage("?x", "here")),
            new QueryParams(List.of(paramAsConstant("x", ANY)))
        );
    }

    public void testMarkerAsAFieldIsReported() {
        analyzer().error(
            "FROM test | WHERE ??f > 1",
            containsString(anyMessage("??f", "here")),
            new QueryParams(List.of(paramAsConstant("f", ANY)))
        );
        analyzer().error(
            "FROM test | WHERE ?f > 1",
            containsString(anyMessage("?f", "here")),
            new QueryParams(List.of(paramAsIdentifier("f", ANY)))
        );
    }

    public void testMarkerInGroupingIsReported() {
        analyzer().error(
            "FROM test | STATS BY ??g",
            containsString(anyMessage("??g", "in a STATS that would have no aggregate and no other key without it")),
            new QueryParams(List.of(paramAsConstant("g", ANY)))
        );
        analyzer().error(
            "FROM test | STATS c = COUNT(*) BY k = ??g",
            containsString(anyMessage("??g", "in a named BY key, as only an unnamed key can be dropped")),
            new QueryParams(List.of(paramAsConstant("g", ANY)))
        );
    }

    public void testMarkerInsideFunctionsIsReported() {
        QueryParams params = new QueryParams(List.of(paramAsConstant("x", ANY)));
        analyzer().error("FROM test | EVAL y = TO_UPPER(?x)", containsString(anyMessage("?x", "here")), params);
        analyzer().error("FROM test | WHERE emp_no IN (1, ?x + 1)", containsString(anyMessage("?x", "here")), params);
        analyzer().error("FROM test | WHERE emp_no > 1 AND emp_no < ?x", containsString(anyMessage("?x", "here")), params);
    }

    /**
     * An unresolved expression has no type, so every rule that asks the type of an expression has to be sure that it is resolved.
     * A marker in each kind of place has to end up in the error, not in an {@code UnresolvedException}, which is a server error.
     */
    public void testMarkerInEveryKindOfPlaceIsReported() {
        QueryParams params = new QueryParams(List.of(paramAsConstant("x", ANY), paramAsConstant("g", ANY)));
        List<String> problems = new ArrayList<>();
        for (String query : List.of(
            "ROW a = ?x",
            "ROW a = 1 | EVAL b = ?x",
            "FROM test | EVAL y = ?x",
            "FROM test | EVAL y = ?x + 1",
            "FROM test | EVAL y = emp_no + ?x",
            "FROM test | EVAL y = -?x",
            "FROM test | EVAL y = ABS(?x)",
            "FROM test | EVAL y = CONCAT(first_name, ?x)",
            "FROM test | EVAL y = CASE(emp_no > 1, ?x, 0)",
            "FROM test | EVAL y = COALESCE(?x, 1)",
            "FROM test | EVAL y = DATE_TRUNC(1 day, hire_date) + ?x",
            "FROM test | EVAL y = ??g",
            "FROM test | WHERE ?x",
            "FROM test | WHERE NOT ?x",
            "FROM test | WHERE emp_no > ?x AND emp_no > 1",
            "FROM test | WHERE emp_no IN (?x + 1)",
            "FROM test | WHERE emp_no IS NULL OR ?x",
            "FROM test | WHERE first_name LIKE \"a*\" AND emp_no > ?x",
            "FROM test | WHERE MATCH(first_name, ?x)",
            "FROM test | SORT ?x",
            "FROM test | SORT emp_no + ?x DESC",
            "FROM test | STATS c = COUNT(?x)",
            "FROM test | STATS c = SUM(emp_no) WHERE emp_no > ?x",
            "FROM test | STATS c = COUNT(*) BY ?x",
            "FROM test | STATS BY ??g",
            "FROM test | STATS c = COUNT(*) BY k = ??g",
            "FROM test | STATS c = COUNT(*) BY b = BUCKET(emp_no, ?x)",
            "FROM test | STATS c = COUNT(*) BY emp_no + ?x",
            "FROM test | INLINE STATS c = COUNT(*) BY k = ??g",
            "FROM test | INLINE STATS c = SUM(emp_no) WHERE emp_no > ?x",
            "FROM test | LIMIT 5 BY ?x",
            "FROM test | LIMIT 5 BY ??g",
            "FROM test | LIMIT 5 BY emp_no > ?x"
        )) {
            try {
                analyzer().error(query, containsString("stands for \"Any\""), params);
            } catch (AssertionError | RuntimeException e) {
                problems.add(query + " -> " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        }
        assertThat(String.join("\n", problems), problems, empty());
    }

    /** A condition that would be replaced if it were resolved is not reported as "Any": what keeps it from resolving is. */
    public void testUnsupportedAnyDoesNotHideOtherErrorsAndIsNotAddedToThem() {
        QueryParams params = new QueryParams(List.of(paramAsConstant("x", ANY), paramAsConstant("y", ANY)));
        for (String query : List.of(
            "FROM test | WHERE nope == ?x",
            "FROM test | WHERE ?x == nope",
            "FROM test | WHERE nope IN (1, ?x)",
            "FROM test | WHERE MV_CONTAINS(nope, ?x)",
            "FROM test | WHERE MV_INTERSECTS(?x, nope)",
            "FROM test | WHERE ?x IS NULL OR nope == ?y",
            "FROM test | WHERE emp_no == ?x | EVAL y = nope",
            "FROM test | EVAL y = nope == ?x",
            "FROM test | STATS c = COUNT(*) WHERE nope == ?x"
        )) {
            String error = analyzer().error(query, containsString("Unknown column [nope]"), params);
            assertThat(query, error, not(containsString("\"Any\"")));
        }
    }

    public void testUnknownFunctionIsReportedInsteadOfItsAnyArgument() {
        QueryParams params = new QueryParams(List.of(paramAsConstant("x", ANY)));
        String error = analyzer().error(
            "FROM test | WHERE MV_CONTAIN(languages, ?x)",
            containsString("Unknown function [MV_CONTAIN]"),
            params
        );
        assertThat(error, not(containsString("\"Any\"")));
        error = analyzer().error("FROM test | WHERE MV_CONTAIN(nope, ?x)", containsString("Unknown column [nope]"), params);
        assertThat(error, not(containsString("\"Any\"")));
    }

    /** What stays unsupported is reported next to the other errors, not instead of them. */
    public void testUnsupportedAnyIsReportedWithOtherErrors() {
        QueryParams params = new QueryParams(List.of(paramAsConstant("x", ANY)));
        for (String query : List.of(
            "FROM test | WHERE emp_no > ?x AND nope == 1",
            "FROM test | WHERE nope == 1 AND emp_no > ?x",
            "FROM test | WHERE NOT (nope == ?x)",
            "FROM test | WHERE nope == ?x AND emp_no != ?x"
        )) {
            analyzer().error(query, allOf(containsString("Unknown column [nope]"), containsString("stands for \"Any\"")), params);
        }
    }

    /** As for any unresolved command, the errors of what comes after a command that fails are hidden. */
    public void testErrorsDownstreamOfAFailingCommandAreHidden() {
        String error = analyzer().error(
            "FROM test | EVAL y = ?x | WHERE nope == 1",
            containsString("stands for \"Any\""),
            new QueryParams(List.of(paramAsConstant("x", ANY)))
        );
        assertThat(error, not(containsString("Unknown column")));
    }

    public void testEveryMarkerIsReported() {
        analyzer().error(
            "FROM test | WHERE emp_no > ?x OR languages > ?y",
            containsString(anyMessage("?y", "here")),
            new QueryParams(List.of(paramAsConstant("x", ANY), paramAsConstant("y", ANY)))
        );
    }
}
