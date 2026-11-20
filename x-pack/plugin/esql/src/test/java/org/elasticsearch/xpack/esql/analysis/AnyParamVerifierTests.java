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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;

/**
 * While nothing drops or replaces the parameters that stand for "Any", they stay in the plan as unresolvable markers, and the
 * {@code Verifier} reports them like any other unresolved expression: as a 400 that names the parameter, and never as the
 * server error that asking the type of an unresolved expression would be.
 */
public class AnyParamVerifierTests extends ESTestCase {

    private static final String ANY = QuerySettings.PARAM_ANY_VALUE_DEFAULT;

    @BeforeClass
    public static void requireAnyParam() {
        assumeTrue("requires the Any query parameter", EsqlCapabilities.Cap.PARAM_ANY_VALUE.isEnabled());
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
            "FROM test | WHERE emp_no == ?x",
            containsString(anyMessage("?x", "here")),
            new QueryParams(List.of(paramAsConstant("x", ANY)))
        );
    }

    public void testMarkerAsAFieldIsReported() {
        analyzer().error(
            "FROM test | WHERE ??f == 1",
            containsString(anyMessage("??f", "here")),
            new QueryParams(List.of(paramAsConstant("f", ANY)))
        );
        analyzer().error(
            "FROM test | WHERE ?f == 1",
            containsString(anyMessage("?f", "here")),
            new QueryParams(List.of(paramAsIdentifier("f", ANY)))
        );
    }

    public void testMarkerInGroupingIsReported() {
        analyzer().error(
            "FROM test | STATS c = COUNT(*) BY ??g",
            containsString(anyMessage("??g", "here")),
            new QueryParams(List.of(paramAsConstant("g", ANY)))
        );
    }

    public void testMarkerInsideFunctionsIsReported() {
        QueryParams params = new QueryParams(List.of(paramAsConstant("x", ANY)));
        analyzer().error("FROM test | EVAL y = TO_UPPER(?x)", containsString(anyMessage("?x", "here")), params);
        analyzer().error("FROM test | WHERE emp_no IN (1, ?x)", containsString(anyMessage("?x", "here")), params);
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
            "FROM test | WHERE emp_no == ?x AND emp_no > 1",
            "FROM test | WHERE emp_no IN (?x)",
            "FROM test | WHERE emp_no IS NULL OR ?x",
            "FROM test | WHERE first_name LIKE \"a*\" AND emp_no == ?x",
            "FROM test | WHERE MATCH(first_name, ?x)",
            "FROM test | SORT ?x",
            "FROM test | SORT emp_no + ?x DESC",
            "FROM test | STATS c = COUNT(?x)",
            "FROM test | STATS c = SUM(emp_no) WHERE emp_no > ?x",
            "FROM test | STATS c = COUNT(*) BY ?x",
            "FROM test | STATS c = COUNT(*) BY ??g",
            "FROM test | STATS c = COUNT(*) BY b = BUCKET(emp_no, ?x)",
            "FROM test | STATS c = COUNT(*) BY emp_no + ?x",
            "FROM test | INLINE STATS c = COUNT(*) BY ??g",
            "FROM test | INLINE STATS c = SUM(emp_no) WHERE emp_no > ?x",
            "FROM test | LIMIT 5 BY ?x",
            "FROM test | LIMIT 5 BY ??g",
            "FROM test | LIMIT 5 BY emp_no == ?x"
        )) {
            try {
                analyzer().error(query, containsString("stands for \"Any\""), params);
            } catch (AssertionError | RuntimeException e) {
                problems.add(query + " -> " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        }
        assertThat(String.join("\n", problems), problems, empty());
    }

    public void testEveryMarkerIsReported() {
        analyzer().error(
            "FROM test | WHERE emp_no == ?x OR languages == ?y",
            containsString(anyMessage("?y", "here")),
            new QueryParams(List.of(paramAsConstant("x", ANY), paramAsConstant("y", ANY)))
        );
    }
}
