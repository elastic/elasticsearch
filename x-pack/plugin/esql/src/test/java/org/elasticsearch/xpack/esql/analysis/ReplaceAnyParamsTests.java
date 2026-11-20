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
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.AnyParam;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsIdentifier;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * The conditions that compare with a parameter that stands for "Any" are replaced with {@code true}, and every other use of the
 * parameter is reported.
 */
public class ReplaceAnyParamsTests extends ESTestCase {

    private static final String ANY = QuerySettings.PARAM_ANY_VALUE_DEFAULT;

    /**
     * The analyzer here follows the build, and release builds don't apply the default value of the setting, so no parameter stands
     * for "Any" in them.
     */
    @BeforeClass
    public static void requireAnyParam() {
        assumeTrue("requires the Any conditions replacement", EsqlCapabilities.Cap.PARAM_ANY_VALUE_V3.isEnabled());
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    private static TestAnalyzer analyzer() {
        return EsqlTestUtils.analyzer().addDefaultIndex().stripErrorPrefix(true);
    }

    private static QueryParams anyParams() {
        return new QueryParams(List.of(paramAsConstant("x", ANY), paramAsConstant("f", ANY), paramAsIdentifier("i", ANY)));
    }

    private static String anyMessage(String parameter, String where) {
        return "Query parameter [" + parameter + "] has the value [" + ANY + "], which stands for \"Any\" and can't be used " + where;
    }

    /** The plan has no marker and the first {@link Filter} has {@code true} as its condition. */
    private static Filter assertWhereIsTrue(String query) {
        LogicalPlan plan = analyzer().query(query, anyParams());
        assertNoMarkers(plan);
        List<Filter> filters = new ArrayList<>();
        plan.forEachDown(Filter.class, filters::add);
        assertThat(query, filters.size(), equalTo(1));
        assertThat(query, filters.getFirst().condition(), equalTo(trueLiteral(filters.getFirst().condition())));
        return filters.getFirst();
    }

    private static Literal trueLiteral(Expression replaced) {
        return new Literal(replaced.source(), true, DataType.BOOLEAN);
    }

    private static void assertNoMarkers(LogicalPlan plan) {
        AtomicBoolean found = new AtomicBoolean();
        plan.forEachExpressionDown(AnyParam.class, a -> found.set(true));
        assertFalse("there are markers left in " + plan, found.get());
    }

    public void testEachSupportedShapeBecomesTrue() {
        for (String condition : List.of(
            "emp_no == ?x",
            "?x == emp_no",
            "??f == ?x",
            "?i == ?x",
            "??f == 1",
            "emp_no IN (1, ?x)",
            "emp_no IN (?x)",
            "emp_no IN (?x, ?f)",
            "?x IN (emp_no, 1)",
            "MV_CONTAINS(languages, ?x)",
            "MV_CONTAINS(?x, languages)",
            "MV_CONTAINS(?x, ?f)",
            "MV_INTERSECTS(languages, ?x)",
            "MV_INTERSECTS(?x, languages)",
            "?x IS NULL",
            "??f IS NULL"
        )) {
            assertWhereIsTrue("FROM test | WHERE " + condition);
        }
    }

    public void testTheOtherOperandCanBeAnyExpression() {
        assertWhereIsTrue("FROM test | WHERE TO_LOWER(first_name) == ?x");
        assertWhereIsTrue("FROM test | WHERE emp_no + salary * 2 == ?x");
        assertWhereIsTrue("FROM test | WHERE (NOT languages > 1) == ?x");
    }

    public void testOnlyTheConditionIsReplaced() {
        LogicalPlan plan = analyzer().query("FROM test | WHERE emp_no > 1 AND emp_no == ?x", anyParams());
        assertNoMarkers(plan);
        List<Filter> filters = new ArrayList<>();
        plan.forEachDown(Filter.class, filters::add);
        And and = as(filters.getFirst().condition(), And.class);
        assertThat(and.left(), instanceOf(GreaterThan.class));
        assertThat(and.right(), equalTo(trueLiteral(and.right())));
    }

    public void testOldGuardStillWorks() {
        for (String query : List.of(
            "FROM test | WHERE ?x IS NULL OR MV_INTERSECTS(?x, languages)",
            "FROM test | WHERE ?x IS NULL OR languages == ?x"
        )) {
            assertNoMarkers(analyzer().query(query, anyParams()));
        }
    }

    public void testEveryPositionOfAnExpression() {
        for (String query : List.of(
            "FROM test | EVAL y = emp_no == ?x",
            "FROM test | EVAL y = CASE(emp_no == ?x, 1, 2)",
            "FROM test | EVAL y = CASE(emp_no == ?x, \"a\", \"b\") | WHERE y == \"a\"",
            "FROM test | WHERE emp_no == ?x | WHERE languages IN (?x)",
            "FROM test | SORT emp_no == ?x",
            "FROM test | STATS c = COUNT(*) WHERE emp_no == ?x",
            "FROM test | STATS c = COUNT(*) BY emp_no == ?x",
            "FROM test | STATS c = COUNT(*) BY b = emp_no == ?x",
            "FROM test | STATS c = COUNT(*) BY languages, emp_no == ?x",
            "FROM test | INLINE STATS c = COUNT(*) WHERE emp_no == ?x",
            "FROM test | INLINE STATS c = COUNT(*) BY emp_no == ?x",
            "ROW a = 1 | WHERE a == ?x",
            "ROW a = 1 | EVAL b = a == ?x",
            "FROM test | LIMIT 5 BY emp_no == ?x",
            "FROM test | WHERE emp_no == ?x OR languages == ?x",
            "FROM test | WHERE first_name IN (\"a\", ?x) AND (emp_no == ?x OR NOT languages > 1)"
        )) {
            assertNoMarkers(analyzer().query(query, anyParams()));
        }
    }

    public void testAGroupingThatIsAConditionBecomesAConstantKey() {
        LogicalPlan plan = analyzer().query("FROM test | STATS c = COUNT(*) BY emp_no == ?x", anyParams());
        Aggregate aggregate = (Aggregate) plan.collect(Aggregate.class::isInstance).getFirst();
        assertThat(aggregate.groupings().size(), equalTo(1));
        Alias key = as(aggregate.groupings().getFirst(), Alias.class);
        assertThat(key.child(), equalTo(trueLiteral(key.child())));
    }

    public void testStructures() {
        for (String query : List.of(
            "FROM test | FORK (WHERE emp_no == ?x) (WHERE languages == ?x | LIMIT 3)",
            "FROM test, (FROM test | WHERE emp_no == ?x)",
            "FROM test | WHERE emp_no IN (FROM test | WHERE emp_no == ?x | KEEP emp_no)",
            "FROM test | WHERE emp_no == ?x | EVAL y = salary | WHERE y == ?x"
        )) {
            assertNoMarkers(analyzer().query(query, anyParams()));
        }
    }

    public void testALongPipeline() {
        StringBuilder query = new StringBuilder("FROM test");
        for (int i = 0; i < 40; i++) {
            query.append(" | WHERE emp_no == ?x");
            query.append(" | EVAL y").append(i).append(" = languages IN (?x)");
        }
        assertNoMarkers(analyzer().query(query.toString(), anyParams()));
    }

    public void testNotAnyIsNotReplaced() {
        LogicalPlan plan = analyzer().query("FROM test | WHERE emp_no == ?x", new QueryParams(List.of(paramAsConstant("x", 5))));
        List<Filter> filters = new ArrayList<>();
        plan.forEachDown(Filter.class, filters::add);
        assertThat(filters.getFirst().condition(), instanceOf(Equals.class));
    }

    public void testUnmappedFieldsAreMatchedToo() {
        for (UnmappedResolution resolution : List.of(UnmappedResolution.NULLIFY, UnmappedResolution.LOAD)) {
            LogicalPlan plan = analyzer().unmappedResolution(resolution).query("FROM test | WHERE nope == ?x", anyParams());
            assertNoMarkers(plan);
        }
    }

    // Unsupported uses

    public void testUnsupportedUsesAreReported() {
        QueryParams params = anyParams();
        List<String> problems = new ArrayList<>();
        for (String query : List.of(
            "FROM test | WHERE emp_no != ?x",
            "FROM test | WHERE emp_no > ?x",
            "FROM test | WHERE first_name IS NOT NULL AND ?x IS NOT NULL",
            "FROM test | WHERE TO_UPPER(?x) == \"A\"",
            "FROM test | WHERE ?x AND emp_no > 1",
            "FROM test | EVAL y = ?x",
            "ROW a = ?x",
            "FROM test | SORT ??f",
            "FROM test | EVAL b = BUCKET(hire_date, ?x, \"2020-01-01\", \"2021-01-01\")",
            "FROM test | STATS c = COUNT(*) BY b = BUCKET(emp_no, ?x, 1, 100)",
            "FROM test | WHERE MV_CONTAINS(languages, ?x + 1)",
            "FROM test | WHERE emp_no == ?x + 1"
        )) {
            try {
                analyzer().error(query, containsString("stands for \"Any\""), params);
            } catch (AssertionError | RuntimeException e) {
                problems.add(query + " -> " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        }
        assertThat(String.join("\n", problems), problems, empty());
    }

    public void testUnderNotIsReported() {
        QueryParams params = anyParams();
        String where = "under a NOT or a !=, as it would match nothing instead of everything";
        analyzer().error("FROM test | WHERE emp_no != ?x", containsString(anyMessage("?x", where)), params);
        analyzer().error("FROM test | WHERE NOT (emp_no == ?x)", containsString(anyMessage("?x", where)), params);
        analyzer().error("FROM test | WHERE emp_no NOT IN (1, ?x)", containsString(anyMessage("?x", where)), params);
        analyzer().error("FROM test | WHERE NOT MV_CONTAINS(languages, ?x)", containsString(anyMessage("?x", where)), params);
        analyzer().error("FROM test | WHERE NOT ?x IS NULL", containsString(anyMessage("?x", where)), params);
        analyzer().error(
            "FROM test | WHERE NOT (languages > 1 AND (emp_no == ?x OR salary > 1))",
            containsString(anyMessage("?x", where)),
            params
        );
        analyzer().error("FROM test | WHERE NOT CASE(emp_no == ?x, true, false)", containsString(anyMessage("?x", where)), params);
    }

    /** An unsupported use under a {@code NOT} says what's wrong with the use, not with the {@code NOT}. */
    public void testUnsupportedUnderNotKeepsTheGeneralMessage() {
        analyzer().error("FROM test | WHERE NOT (emp_no > ?x)", containsString(anyMessage("?x", "here")), anyParams());
    }

    /** A {@code NOT} below the condition doesn't change what the condition says about the parameter. */
    public void testNotBelowTheConditionIsFine() {
        assertWhereIsTrue("FROM test | WHERE (NOT languages > 1) == ?x");
    }

    /** The condition is inverted by something that can't be detected: documented, not handled. */
    public void testIndirectInversionsMatchNothing() {
        LogicalPlan plan = analyzer().query("FROM test | WHERE (emp_no == ?x) == false", anyParams());
        assertNoMarkers(plan);
        List<Filter> filters = new ArrayList<>();
        plan.forEachDown(Filter.class, filters::add);
        Equals equals = as(filters.getFirst().condition(), Equals.class);
        assertThat(equals.left(), equalTo(trueLiteral(equals.left())));
        assertThat(equals.right().fold(FoldContext.small()), equalTo(false));
    }

    // Type checks

    /** The condition has the error it has with any value: here, an {@code IN} with values of different types. */
    public void testTypeErrorsAreTheOnesOfAnyValue() {
        String query = "FROM test | WHERE first_name IN (1, ?x)";
        String withAny = analyzer().error(query, containsString("must be"), anyParams());
        String withValue = analyzer().error(query, containsString("must be"), new QueryParams(List.of(paramAsConstant("x", "a"))));
        assertThat(withAny, not(containsString("stands for \"Any\"")));
        assertThat(withAny, equalTo(withValue));
    }
    // The errors of the other operands are the ones that are reported

    public void testUnknownColumnIsReported() {
        analyzer().error("FROM test | WHERE nope == ?x", containsString("Unknown column [nope]"), anyParams());
        analyzer().error("FROM test | WHERE MV_CONTAINS(nope, ?x)", containsString("Unknown column [nope]"), anyParams());
    }

}
