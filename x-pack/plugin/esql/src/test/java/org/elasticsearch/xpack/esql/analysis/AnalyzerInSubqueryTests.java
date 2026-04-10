/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.approximation.Approximation;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.SemiJoin;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class AnalyzerInSubqueryTests extends ESTestCase {

    @Before
    public void checkInSubquerySupport() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY.isEnabled());
    }

    // -- analyzer integration tests --

    /**
     * Verifies that {@code FROM test | WHERE emp_no IN (FROM employees | KEEP emp_no)} produces a SemiJoin
     * with SEMI type after analysis.
     */
    public void testAnalyzerProducesSemiJoinForWhereIn() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
            """);

        var limit = as(plan, Limit.class);
        var semiJoin = as(limit.child(), SemiJoin.class);

        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertFalse(semiJoin.isAntiJoin());

        assertThat(semiJoin.config().leftFields().size(), equalTo(1));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().size(), equalTo(1));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        as(semiJoin.left(), EsRelation.class);

        var rightProject = as(semiJoin.right(), Project.class);
        as(rightProject.child(), EsRelation.class);

        assertThat(semiJoin.output().size(), equalTo(semiJoin.left().output().size()));
    }

    /**
     * Verifies that {@code WHERE emp_no NOT IN (FROM employees | KEEP emp_no)} produces an AntiJoin.
     */
    public void testAnalyzerProducesAntiJoinForWhereNotIn() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no NOT IN (FROM employees | KEEP emp_no)
            """);

        var limit = as(plan, Limit.class);
        var antiJoin = as(limit.child(), AntiJoin.class);

        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertTrue(antiJoin.isAntiJoin());

        assertThat(antiJoin.config().leftFields().size(), equalTo(1));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(antiJoin.config().rightFields().size(), equalTo(1));
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        as(antiJoin.left(), EsRelation.class);

        var rightProject = as(antiJoin.right(), Project.class);
        as(rightProject.child(), EsRelation.class);

        assertThat(antiJoin.output().size(), equalTo(antiJoin.left().output().size()));
    }

    /**
     * Verifies that an IN subquery combined with another filter condition produces a SemiJoin on top of the Filter.
     */
    public void testSemiJoinWithRemainingFilterCondition() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
              AND salary > 50000
            """);

        var limit = as(plan, Limit.class);
        var semiJoin = as(limit.child(), SemiJoin.class);
        var filter = as(semiJoin.left(), Filter.class);

        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
    }

    /**
     * Verifies that the SemiJoin output only includes left-side columns.
     */
    public void testSemiJoinOutputIsLeftOnly() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
            """);

        var limit = as(plan, Limit.class);
        var semiJoin = as(limit.child(), SemiJoin.class);

        var leftOutput = semiJoin.left().output();
        assertThat(semiJoin.output(), equalTo(leftOutput));
    }

    /**
     * Verifies that an EVAL alias used as the left side of IN subquery produces a SemiJoin
     * with the alias as the left join field.
     */
    public void testSemiJoinWithEvalAlias() {
        var plan = analyzeInSubquery("""
            FROM test
            | EVAL x = emp_no + 1
            | WHERE x IN (FROM employees | KEEP emp_no)
            """);

        var limit = as(plan, Limit.class);
        var semiJoin = as(limit.child(), SemiJoin.class);

        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().size(), equalTo(1));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("x"));

        var eval = as(semiJoin.left(), Eval.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * Verifies that an EVAL alias used as the left side of NOT IN subquery produces an AntiJoin
     * with the alias as the left join field.
     */
    public void testAntiJoinWithEvalAlias() {
        var plan = analyzeInSubquery("""
            FROM test
            | EVAL x = emp_no + 1
            | WHERE x NOT IN (FROM employees | KEEP emp_no)
            """);

        var limit = as(plan, Limit.class);
        var antiJoin = as(limit.child(), AntiJoin.class);

        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().leftFields().size(), equalTo(1));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("x"));

        var eval = as(antiJoin.left(), Eval.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * Verifies that an EVAL alias combined with a remaining filter produces a SemiJoin on top
     * of the Filter, with the Eval below the Filter.
     */
    public void testSemiJoinWithEvalAliasAndRemainingFilter() {
        var plan = analyzeInSubquery("""
            FROM test
            | EVAL x = emp_no + 1
            | WHERE x IN (FROM employees | KEEP emp_no)
              AND salary > 50000
            """);

        var limit = as(plan, Limit.class);
        var semiJoin = as(limit.child(), SemiJoin.class);

        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("x"));

        var filter = as(semiJoin.left(), Filter.class);
        var eval = as(filter.child(), Eval.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * Verifies that multiple EVAL aliases can be used as left sides of IN and NOT IN subqueries.
     */
    public void testMixedSemiAndAntiJoinWithEvalAliases() {
        var plan = analyzeInSubquery("""
            FROM test
            | EVAL x = emp_no + 1, y = salary * 2
            | WHERE x IN (FROM employees | KEEP emp_no)
              AND y NOT IN (FROM employees | KEEP salary)
            """);

        var limit = as(plan, Limit.class);
        var antiJoin = as(limit.child(), AntiJoin.class);
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("y"));

        var semiJoin = as(antiJoin.left(), SemiJoin.class);
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("x"));

        var eval = as(semiJoin.left(), Eval.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * Verifies that an IN subquery with STATS produces a SemiJoin whose right side contains an Aggregate.
     */
    public void testSemiJoinWithStatsInSubquery() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | STATS max_emp = max(emp_no))
            """);

        var limit = as(plan, Limit.class);
        var semiJoin = as(limit.child(), SemiJoin.class);

        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));

        as(semiJoin.left(), EsRelation.class);

        var aggregate = as(semiJoin.right(), Aggregate.class);
        as(aggregate.child(), EsRelation.class);
    }

    /**
     * Verifies that a NOT IN subquery with STATS produces an AntiJoin whose right side contains an Aggregate.
     */
    public void testAntiJoinWithStatsInSubquery() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no NOT IN (FROM employees | STATS min_emp = min(emp_no))
            """);

        var limit = as(plan, Limit.class);
        var antiJoin = as(limit.child(), AntiJoin.class);

        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));

        as(antiJoin.left(), EsRelation.class);

        var aggregate = as(antiJoin.right(), Aggregate.class);
        as(aggregate.child(), EsRelation.class);
    }

    /**
     * Verifies that an IN subquery with STATS ... BY and KEEP produces a SemiJoin
     * whose right side is a Project over an Aggregate with grouping.
     */
    public void testSemiJoinWithStatsByInSubquery() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees
                              | STATS max_emp = max(emp_no) BY languages
                              | KEEP max_emp)
            """);

        var limit = as(plan, Limit.class);
        var semiJoin = as(limit.child(), SemiJoin.class);

        var project = as(semiJoin.right(), Project.class);
        var aggregate = as(project.child(), Aggregate.class);
        as(aggregate.child(), EsRelation.class);
        assertThat(aggregate.groupings().size(), equalTo(1));
    }

    /**
     * Verifies that an IN subquery with STATS, SORT and LIMIT produces a SemiJoin
     * whose right side is Limit -> OrderBy -> Aggregate.
     */
    public void testSemiJoinWithStatsSortLimitInSubquery() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees
                              | STATS m = max(emp_no) BY y = date_trunc(1 year, hire_date)
                              | SORT y DESC
                              | LIMIT 5
                              | KEEP m)
            """);

        var outerLimit = as(plan, Limit.class);
        var semiJoin = as(outerLimit.child(), SemiJoin.class);

        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        as(semiJoin.left(), EsRelation.class);

        var project = as(semiJoin.right(), Project.class);
        var innerLimit = as(project.child(), Limit.class);
        var orderBy = as(innerLimit.child(), OrderBy.class);
        var aggregate = as(orderBy.child(), Aggregate.class);
        as(aggregate.child(), EsRelation.class);
        assertThat(aggregate.groupings().size(), equalTo(1));
    }

    // -- tests with commands after the WHERE IN subquery --

    /**
     * Verifies that commands after the WHERE IN subquery are placed on top of the SemiJoin.
     */
    public void testCommandsAfterSemiJoin() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
            | EVAL doubled = salary * 2
            | WHERE doubled > 100000
            | SORT doubled DESC
            | LIMIT 10
            | KEEP emp_no, doubled
            """);

        var outerLimit = as(plan, Limit.class);
        var project = as(outerLimit.child(), Project.class);
        var innerLimit = as(project.child(), Limit.class);
        var orderBy = as(innerLimit.child(), OrderBy.class);
        var filter = as(orderBy.child(), Filter.class);
        var eval = as(filter.child(), Eval.class);
        var semiJoin = as(eval.child(), SemiJoin.class);

        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        as(semiJoin.left(), EsRelation.class);
    }

    /**
     * Verifies that commands after the WHERE NOT IN subquery are placed on top of the AntiJoin.
     */
    public void testCommandsAfterAntiJoin() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no NOT IN (FROM employees | KEEP emp_no)
            | EVAL doubled = salary * 2
            | SORT doubled
            | LIMIT 5
            """);

        var outerLimit = as(plan, Limit.class);
        var innerLimit = as(outerLimit.child(), Limit.class);
        var orderBy = as(innerLimit.child(), OrderBy.class);
        var eval = as(orderBy.child(), Eval.class);
        var antiJoin = as(eval.child(), AntiJoin.class);

        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        as(antiJoin.left(), EsRelation.class);
    }

    /**
     * Verifies that a second WHERE after the IN subquery is separate from the join.
     */
    public void testSecondWhereAfterSemiJoin() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
            | WHERE salary > 50000
            """);

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var semiJoin = as(filter.child(), SemiJoin.class);

        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        as(semiJoin.left(), EsRelation.class);
    }

    /**
     * Verifies that STATS after the IN subquery aggregates over the joined result.
     */
    public void testStatsAfterSemiJoin() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
            | STATS avg_salary = avg(salary) BY languages
            """);

        var limit = as(plan, Limit.class);
        var aggregate = as(limit.child(), Aggregate.class);
        var semiJoin = as(aggregate.child(), SemiJoin.class);

        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        as(semiJoin.left(), EsRelation.class);
    }

    // -- tests with FROM subquery inside the IN subquery --

    /**
     * Verifies that a FROM subquery inside the IN subquery produces a SemiJoin
     * whose right side contains a UnionAll.
     */
    public void testSemiJoinWithFromSubqueryInsideInSubquery() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees, (FROM test | KEEP emp_no) | KEEP emp_no)
            """);

        var limit = as(plan, Limit.class);
        var semiJoin = as(limit.child(), SemiJoin.class);

        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        as(semiJoin.left(), EsRelation.class);

        var project = as(semiJoin.right(), Project.class);
        var unionAll = as(project.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());
    }

    /**
     * Verifies that a NOT IN subquery containing a FROM subquery produces an AntiJoin
     * whose right side contains a UnionAll.
     */
    public void testAntiJoinWithFromSubqueryInsideInSubquery() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no NOT IN (FROM employees, (FROM test | KEEP emp_no) | KEEP emp_no)
            """);

        var limit = as(plan, Limit.class);
        var antiJoin = as(limit.child(), AntiJoin.class);

        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        as(antiJoin.left(), EsRelation.class);

        var project = as(antiJoin.right(), Project.class);
        var unionAll = as(project.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());
    }

    /**
     * Verifies that a FROM subquery containing a WHERE IN subquery produces a UnionAll
     * with a SemiJoin inside the subquery branch.
     */
    public void testFromSubqueryWithInSubqueryInside() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var plan = analyzeInSubquery("""
            FROM test,
                 (FROM employees | WHERE emp_no IN (FROM test | KEEP emp_no) | KEEP emp_no)
            """);

        var limit = as(plan, Limit.class);
        var unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // main query: Project -> EsRelation (Project added for field alignment in UnionAll)
        var mainProject = as(unionAll.children().get(0), Project.class);
        as(mainProject.child(), EsRelation.class);

        // FROM subquery: Project -> SemiJoin
        // FROM subquery: Project (alignment) -> Eval (null columns) -> Subquery -> Project -> SemiJoin
        var alignProject = as(unionAll.children().get(1), Project.class);
        var subEval = as(alignProject.child(), Eval.class);
        var subquery = as(subEval.child(), Subquery.class);
        var subProject = as(subquery.child(), Project.class);
        var semiJoin = as(subProject.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        as(semiJoin.left(), EsRelation.class);
    }

    /**
     * Verifies that a FROM subquery containing a WHERE NOT IN subquery produces a UnionAll
     * with an AntiJoin inside the subquery branch.
     */
    public void testFromSubqueryWithNotInSubqueryInside() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var plan = analyzeInSubquery("""
            FROM test,
                 (FROM employees | WHERE emp_no NOT IN (FROM test | KEEP emp_no) | KEEP emp_no)
            """);

        var limit = as(plan, Limit.class);
        var unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        var mainProject = as(unionAll.children().get(0), Project.class);
        as(mainProject.child(), EsRelation.class);

        var alignProject = as(unionAll.children().get(1), Project.class);
        var subEval = as(alignProject.child(), Eval.class);
        var subquery = as(subEval.child(), Subquery.class);
        var subProject = as(subquery.child(), Project.class);
        var antiJoin = as(subProject.child(), AntiJoin.class);
        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        as(antiJoin.left(), EsRelation.class);
    }

    // -- nested IN/NOT IN subquery tests --

    /**
     * Verifies that a nested IN subquery (IN inside IN) produces a SemiJoin whose right side
     * contains another SemiJoin.
     */
    public void testNestedInSubquery() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (
                FROM employees
                | WHERE salary IN (FROM test | KEEP salary)
                | KEEP emp_no
              )
            """);

        var limit = as(plan, Limit.class);
        var outerSemiJoin = as(limit.child(), SemiJoin.class);
        assertThat(outerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(outerSemiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        as(outerSemiJoin.left(), EsRelation.class);

        // Right side: Project -> SemiJoin (the inner IN subquery)
        var project = as(outerSemiJoin.right(), Project.class);
        var innerSemiJoin = as(project.child(), SemiJoin.class);
        assertThat(innerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(innerSemiJoin.config().leftFields().get(0).name(), equalTo("salary"));
        as(innerSemiJoin.left(), EsRelation.class);

        var innerProject = as(innerSemiJoin.right(), Project.class);
        as(innerProject.child(), EsRelation.class);
    }

    /**
     * Verifies that a nested NOT IN inside IN produces a SemiJoin whose right side contains an AntiJoin.
     */
    public void testNestedNotInInsideInSubquery() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (
                FROM employees
                | WHERE salary NOT IN (FROM test | KEEP salary)
                | KEEP emp_no
              )
            """);

        var limit = as(plan, Limit.class);
        var outerSemiJoin = as(limit.child(), SemiJoin.class);
        assertThat(outerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        as(outerSemiJoin.left(), EsRelation.class);

        var project = as(outerSemiJoin.right(), Project.class);
        var innerAntiJoin = as(project.child(), AntiJoin.class);
        assertThat(innerAntiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(innerAntiJoin.config().leftFields().get(0).name(), equalTo("salary"));
        as(innerAntiJoin.left(), EsRelation.class);
    }

    /**
     * Verifies that a nested IN inside NOT IN produces an AntiJoin whose right side contains a SemiJoin.
     */
    public void testNestedInInsideNotInSubquery() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no NOT IN (
                FROM employees
                | WHERE salary IN (FROM test | KEEP salary)
                | KEEP emp_no
              )
            """);

        var limit = as(plan, Limit.class);
        var outerAntiJoin = as(limit.child(), AntiJoin.class);
        assertThat(outerAntiJoin.config().type(), equalTo(JoinTypes.ANTI));
        as(outerAntiJoin.left(), EsRelation.class);

        var project = as(outerAntiJoin.right(), Project.class);
        var innerSemiJoin = as(project.child(), SemiJoin.class);
        assertThat(innerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(innerSemiJoin.config().leftFields().get(0).name(), equalTo("salary"));
    }

    /**
     * Verifies that a 3 levels deep nested IN subquery is resolved correctly.
     */
    public void testThreeNestedInSubquery() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (
                FROM employees
                | WHERE salary IN (
                    FROM test
                    | WHERE languages IN (FROM employees | KEEP languages)
                    | KEEP salary
                  )
                | KEEP emp_no
              )
            """);

        var limit = as(plan, Limit.class);
        var outerSemiJoin = as(limit.child(), SemiJoin.class);
        assertThat(outerSemiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));

        var project1 = as(outerSemiJoin.right(), Project.class);
        var middleSemiJoin = as(project1.child(), SemiJoin.class);
        assertThat(middleSemiJoin.config().leftFields().get(0).name(), equalTo("salary"));

        var project2 = as(middleSemiJoin.right(), Project.class);
        var innerSemiJoin = as(project2.child(), SemiJoin.class);
        assertThat(innerSemiJoin.config().leftFields().get(0).name(), equalTo("languages"));
    }

    /**
     * Verifies that a nested IN subquery with additional filter conditions resolves correctly.
     */
    public void testNestedInSubqueryWithFilter() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (
                FROM employees
                | WHERE salary IN (FROM test | KEEP salary)
                  AND languages > 2
                | KEEP emp_no
              )
            """);

        var limit = as(plan, Limit.class);
        var outerSemiJoin = as(limit.child(), SemiJoin.class);

        var project = as(outerSemiJoin.right(), Project.class);
        var innerSemiJoin = as(project.child(), SemiJoin.class);
        // The remaining filter (languages > 2) should be below the inner SemiJoin
        var filter = as(innerSemiJoin.left(), Filter.class);
        as(filter.child(), EsRelation.class);
    }

    // -- disjunctive IN/NOT IN subquery tests --

    /**
     * Verifies that an OR of two IN subqueries is rewritten to a UnionAll with exclusive branches.
     * Each branch contains a SemiJoin for the IN subquery.
     */
    public void testDisjunctiveInSubqueries() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
               OR salary IN (FROM employees | KEEP salary)
            """);

        var limit = as(plan, Limit.class);
        var unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // Branch 1: Project -> SemiJoin for emp_no IN (...)
        var branch1Project = as(unionAll.children().get(0), Project.class);
        var branch1 = as(branch1Project.child(), SemiJoin.class);
        assertThat(branch1.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(branch1.config().leftFields().get(0).name(), equalTo("emp_no"));

        // Branch 2: Project -> SemiJoin for salary IN (...), with AntiJoin exclusion below
        var branch2Project = as(unionAll.children().get(1), Project.class);
        var branch2Semi = as(branch2Project.child(), SemiJoin.class);
        assertThat(branch2Semi.config().leftFields().get(0).name(), equalTo("salary"));
        var branch2Anti = as(branch2Semi.left(), AntiJoin.class);
        assertThat(branch2Anti.config().leftFields().get(0).name(), equalTo("emp_no"));
    }

    /**
     * Verifies that an OR of IN and NOT IN subqueries is rewritten to a UnionAll.
     */
    public void testDisjunctiveInAndNotInSubqueries() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no NOT IN (FROM employees | KEEP emp_no)
               OR emp_no IN (FROM employees | WHERE salary > 50000 | KEEP emp_no)
            """);

        var limit = as(plan, Limit.class);
        var unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // Branch 1: Project -> AntiJoin for emp_no NOT IN (...)
        var branch1Project = as(unionAll.children().get(0), Project.class);
        var branch1 = as(branch1Project.child(), AntiJoin.class);
        assertThat(branch1.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(branch1.config().leftFields().get(0).name(), equalTo("emp_no"));

        // Branch 2: Project -> SemiJoin for emp_no IN (sub2), with SemiJoin exclusion below
        // NOT(NOT IN sub1) simplifies to IN sub1, so the exclusion is a SemiJoin
        var branch2Project = as(unionAll.children().get(1), Project.class);
        var branch2Outer = as(branch2Project.child(), SemiJoin.class);
        assertThat(branch2Outer.config().leftFields().get(0).name(), equalTo("emp_no"));
        var branch2Inner = as(branch2Outer.left(), SemiJoin.class);
        assertThat(branch2Inner.config().leftFields().get(0).name(), equalTo("emp_no"));
    }

    /**
     * Verifies that an OR with one regular condition and one IN subquery is rewritten to a UnionAll.
     * The regular condition branch stays as a plain Filter.
     */
    public void testDisjunctiveRegularAndInSubquery() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE salary > 50000
               OR emp_no IN (FROM employees | KEEP emp_no)
            """);

        var limit = as(plan, Limit.class);
        var unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // Branch 1: Project -> Filter for salary > 50000
        var branch1Project = as(unionAll.children().get(0), Project.class);
        var branch1 = as(branch1Project.child(), Filter.class);
        as(branch1.child(), EsRelation.class);

        // Branch 2: Project -> SemiJoin for emp_no IN (...) with NOT(salary > 50000) filter below
        var branch2Project = as(unionAll.children().get(1), Project.class);
        var branch2Semi = as(branch2Project.child(), SemiJoin.class);
        assertThat(branch2Semi.config().leftFields().get(0).name(), equalTo("emp_no"));
        var branch2Filter = as(branch2Semi.left(), Filter.class);
        as(branch2Filter.child(), EsRelation.class);
    }

    // -- date comparison inside IN subquery --

    public void testInSubqueryWithImplicitDateCast() {
        var plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (
                FROM employees
                | WHERE hire_date >= "1989-01-01T00:00:00.000Z"
                | KEEP emp_no
              )
            | KEEP emp_no
            """);
        assertNotNull(plan);
    }

    // -- negative: IN subquery in STATS WHERE filter --

    /**
     * Verifies that an IN subquery in STATS WHERE filter is rejected.
     */
    public void testRejectsInSubqueryInStatsWhereFilter() {
        errorInSubquery(
            """
                FROM test
                | STATS cnt = COUNT(*) WHERE emp_no IN (FROM employees | KEEP emp_no)
                """,
            containsString(
                "IN/NOT IN subquery is not supported in "
                    + "Aggregate [STATS cnt = COUNT(*) WHERE emp_no IN (FROM employees | KEEP emp_no)]"
            )
        );
    }

    /**
     * Verifies that a NOT IN subquery in STATS WHERE filter is rejected.
     */
    public void testRejectsNotInSubqueryInStatsWhereFilter() {
        errorInSubquery(
            """
                FROM test
                | STATS cnt = COUNT(*) WHERE emp_no NOT IN (FROM employees | KEEP emp_no)
                """,
            containsString(
                "IN/NOT IN subquery is not supported in "
                    + "Aggregate [STATS cnt = COUNT(*) WHERE emp_no NOT IN (FROM employees | KEEP emp_no)]"
            )
        );
    }

    /**
     * Verifies that IN subquery in STATS WHERE with BY grouping is rejected.
     */
    public void testRejectsInSubqueryInStatsWhereFilterWithGrouping() {
        errorInSubquery(
            """
                FROM test
                | STATS cnt = COUNT(*) WHERE emp_no IN (FROM employees | KEEP emp_no) BY languages
                """,
            containsString(
                "IN/NOT IN subquery is not supported in "
                    + "Aggregate [STATS cnt = COUNT(*) WHERE emp_no IN (FROM employees | KEEP emp_no) BY languages]"
            )
        );
    }

    // -- negative: IN subquery in INLINESTATS --

    /**
     * Verifies that an IN subquery in INLINESTATS WHERE filter is rejected.
     */
    public void testRejectsInSubqueryInInlineStatsWhereFilter() {
        errorInSubquery(
            """
                FROM test
                | INLINESTATS cnt = COUNT(*) WHERE emp_no IN (FROM employees | KEEP emp_no)
                """,
            containsString(
                "IN/NOT IN subquery is not supported in "
                    + "Aggregate [INLINESTATS cnt = COUNT(*) WHERE emp_no IN (FROM employees | KEEP emp_no)]"
            )
        );
    }

    /**
     * Verifies that a NOT IN subquery in INLINESTATS WHERE filter is rejected.
     */
    public void testRejectsNotInSubqueryInInlineStatsWhereFilter() {
        errorInSubquery(
            """
                FROM test
                | INLINESTATS cnt = COUNT(*) WHERE emp_no NOT IN (FROM employees | KEEP emp_no)
                """,
            containsString(
                "IN/NOT IN subquery is not supported in "
                    + "Aggregate [INLINESTATS cnt = COUNT(*) WHERE emp_no NOT IN (FROM employees | KEEP emp_no)]"
            )
        );
    }

    /**
     * Verifies that IN subquery in INLINESTATS WHERE with BY grouping is rejected.
     */
    public void testRejectsInSubqueryInInlineStatsWhereFilterWithGrouping() {
        errorInSubquery(
            """
                FROM test
                | INLINESTATS cnt = COUNT(*) WHERE emp_no IN (FROM employees | KEEP emp_no) BY languages
                """,
            containsString(
                "IN/NOT IN subquery is not supported in "
                    + "Aggregate [INLINESTATS cnt = COUNT(*) WHERE emp_no IN (FROM employees | KEEP emp_no) BY languages]"
            )
        );
    }

    // -- negative: IN subquery in EVAL --

    /**
     * Verifies that an IN subquery inside EVAL is rejected.
     */
    public void testRejectsInSubqueryInEval() {
        errorInSubquery("""
            FROM test
            | EVAL x = emp_no IN (FROM employees | KEEP emp_no)
            """, containsString("IN/NOT IN subquery is not supported in " + "Eval [EVAL x = emp_no IN (FROM employees | KEEP emp_no)]"));
    }

    /**
     * Verifies that a NOT IN subquery inside EVAL is rejected.
     */
    public void testRejectsNotInSubqueryInEval() {
        errorInSubquery(
            """
                FROM test
                | EVAL x = emp_no NOT IN (FROM employees | KEEP emp_no)
                """,
            containsString("IN/NOT IN subquery is not supported in " + "Eval [EVAL x = emp_no NOT IN (FROM employees | KEEP emp_no)]")
        );
    }

    // -- approximation incompatibility tests --

    /**
     * Verifies that IN subquery before STATS is incompatible with approximation.
     */
    public void testApproximationRejectsInSubqueryBeforeStats() {
        assertApproximationRejects("FROM test | WHERE emp_no IN (FROM employees | KEEP emp_no) | STATS COUNT()");
    }

    /**
     * Verifies that NOT IN subquery before STATS is incompatible with approximation.
     */
    public void testApproximationRejectsNotInSubqueryBeforeStats() {
        assertApproximationRejects("FROM test | WHERE emp_no NOT IN (FROM employees | KEEP emp_no) | STATS COUNT()");
    }

    /**
     * Verifies that IN subquery after STATS is incompatible with approximation.
     */
    public void testApproximationRejectsInSubqueryAfterStats() {
        assertApproximationRejects("FROM test | STATS cnt = COUNT() BY emp_no | WHERE emp_no IN (FROM employees | KEEP emp_no)");
    }

    // -- negative analyzer/verifier tests --

    /**
     * Verifies that an IN subquery returning two columns (KEEP emp_no, salary) is rejected.
     */
    public void testRejectsInSubqueryWithMultipleColumns() {
        errorInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no, salary)
            """, containsString("IN subquery must return exactly one column, found [emp_no, salary]"));
    }

    /**
     * Verifies that a NOT IN subquery returning two columns is rejected.
     */
    public void testRejectsNotInSubqueryWithMultipleColumns() {
        errorInSubquery("""
            FROM test
            | WHERE emp_no NOT IN (FROM employees | KEEP emp_no, salary)
            """, containsString("IN subquery must return exactly one column, found [emp_no, salary]"));
    }

    /**
     * Verifies that an IN subquery returning all columns (no KEEP) is rejected.
     */
    public void testRejectsInSubqueryWithAllColumns() {
        errorInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees)
            """, containsString("IN subquery must return exactly one column"));
    }

    /**
     * Verifies that an IN subquery with integer left side and keyword right side is rejected.
     */
    public void testRejectsTypeMismatchIntegerVsKeyword() {
        errorInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP first_name)
            """, containsString("left field [emp_no] of type [INTEGER] is incompatible with right field [first_name] of type [KEYWORD]"));
    }

    /**
     * Verifies that a NOT IN subquery with keyword left side and integer right side is rejected.
     */
    public void testRejectsTypeMismatchKeywordVsInteger() {
        errorInSubquery("""
            FROM test
            | WHERE first_name NOT IN (FROM employees | KEEP emp_no)
            """, containsString("left field [first_name] of type [KEYWORD] is incompatible with right field [emp_no] of type [INTEGER]"));
    }

    /**
     * Verifies that an IN subquery with integer left side and date right side is rejected.
     */
    public void testRejectsTypeMismatchIntegerVsDate() {
        errorInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP hire_date)
            """, containsString("left field [emp_no] of type [INTEGER] is incompatible with right field [hire_date] of type [DATETIME]"));
    }

    /**
     * Verifies that an IN subquery with STATS ... BY returning two columns is rejected.
     */
    public void testRejectsInSubqueryWithStatsByReturningMultipleColumns() {
        errorInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | STATS max(emp_no) BY languages)
            """, containsString("IN subquery must return exactly one column, found [max(emp_no), languages]"));
    }

    // -- non-comparable types in IN subquery --

    /**
     * Verifies that counter types (COUNTER_LONG) are rejected as IN subquery join keys.
     */
    public void testRejectsCounterLongInSubquery() {
        errorWithK8s("""
            FROM k8s
            | WHERE network.total_bytes_in IN (FROM k8s | KEEP network.total_bytes_in)
            """, containsString("IN/NOT IN subquery with right field [network.total_bytes_in] of type [COUNTER_LONG] is not supported"));
    }

    /**
     * Verifies that counter types (COUNTER_DOUBLE) are rejected as IN subquery join keys.
     */
    public void testRejectsCounterDoubleInSubquery() {
        errorWithK8s("""
            FROM k8s
            | WHERE network.total_cost IN (FROM k8s | KEEP network.total_cost)
            """, containsString("IN/NOT IN subquery with right field [network.total_cost] of type [COUNTER_DOUBLE] is not supported"));
    }

    /**
     * Verifies that aggregate_metric_double is rejected as IN subquery join key.
     */
    public void testRejectsAggregateMetricDoubleInSubquery() {
        errorWithK8sDownsampled(
            """
                FROM k8s
                | WHERE network.eth0.tx IN (FROM k8s | KEEP network.eth0.tx)
                """,
            containsString("IN/NOT IN subquery with right field [network.eth0.tx] of type [AGGREGATE_METRIC_DOUBLE] is not supported")
        );
    }

    /**
     * Verifies that numeric type mismatch (INTEGER vs LONG) is rejected — SemiJoin requires exact type match.
     */
    public void testRejectsNumericTypeMismatchIntegerVsLong() {
        errorInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | EVAL x = languages::long | KEEP x)
            """, containsString("left field [emp_no] of type [INTEGER] is incompatible with right field [x] of type [LONG]"));
    }

    // -- compatible string-like types in IN subquery: KEYWORD, TEXT, IP, VERSION --

    /**
     * Verifies that KEYWORD left vs TEXT right is compatible in IN subquery.
     */
    public void testKeywordVsTextInSubquery() {
        var plan = analyzeWithAllTypes("""
            FROM all_types
            | WHERE keyword IN (FROM all_types | KEEP text)
            """);
        assertNotNull(plan);
    }

    /**
     * Verifies that TEXT left vs KEYWORD right is compatible in IN subquery.
     */
    public void testTextVsKeywordInSubquery() {
        var plan = analyzeWithAllTypes("""
            FROM all_types
            | WHERE text IN (FROM all_types | KEEP keyword)
            """);
        assertNotNull(plan);
    }

    /**
     * Verifies that IP left vs IP right is compatible in IN subquery.
     */
    public void testIpVsIpInSubquery() {
        var plan = analyzeWithAllTypes("""
            FROM all_types
            | WHERE ip IN (FROM all_types | KEEP ip)
            """);
        assertNotNull(plan);
    }

    /**
     * Verifies that VERSION left vs VERSION right is compatible in IN subquery.
     */
    public void testVersionVsVersionInSubquery() {
        var plan = analyzeWithAllTypes("""
            FROM all_types
            | WHERE version IN (FROM all_types | KEEP version)
            """);
        assertNotNull(plan);
    }

    /**
     * Verifies that KEYWORD left vs IP right is compatible in IN subquery.
     */
    public void testRejectsKeywordVsIpInSubquery() {
        errorWithAllTypes("""
            FROM all_types
            | WHERE keyword IN (FROM all_types | KEEP ip)
            """, containsString("left field [keyword] of type [KEYWORD] is incompatible with right field [ip] of type [IP]"));
    }

    /**
     * Verifies that IP left vs VERSION right is incompatible in IN subquery.
     */
    public void testRejectsIpVsVersionInSubquery() {
        errorWithAllTypes("""
            FROM all_types
            | WHERE ip IN (FROM all_types | KEEP version)
            """, containsString("left field [ip] of type [IP] is incompatible with right field [version] of type [VERSION]"));
    }

    /**
     * Verifies that VERSION left vs TEXT right is incompatible in IN subquery.
     */
    public void testRejectsVersionVsTextInSubquery() {
        errorWithAllTypes("""
            FROM all_types
            | WHERE version IN (FROM all_types | KEEP text)
            """, containsString("left field [version] of type [VERSION] is incompatible with right field [text] of type [TEXT]"));
    }

    /**
     * Verifies that IP left vs KEYWORD right is incompatible in IN subquery.
     */
    public void testRejectsIpVsKeywordInSubquery() {
        errorWithAllTypes("""
            FROM all_types
            | WHERE ip IN (FROM all_types | KEEP keyword)
            """, containsString("left field [ip] of type [IP] is incompatible with right field [keyword] of type [KEYWORD]"));
    }

    /**
     * Verifies that VERSION left vs KEYWORD right is incompatible in IN subquery.
     */
    public void testRejectsVersionVsKeywordInSubquery() {
        errorWithAllTypes("""
            FROM all_types
            | WHERE version IN (FROM all_types | KEEP keyword)
            """, containsString("left field [version] of type [VERSION] is incompatible with right field [keyword] of type [KEYWORD]"));
    }

    // -- date vs date_nanos incompatibility --

    /**
     * Verifies that DATETIME left vs DATE_NANOS right is incompatible in IN subquery.
     * employees has hire_date:date (DATETIME), employees_incompatible has hire_date:date_nanos (DATE_NANOS).
     */
    public void testRejectsDateVsDateNanosInSubquery() {
        errorWithIncompatible(
            """
                FROM test
                | WHERE hire_date IN (FROM employees_incompatible | KEEP hire_date)
                """,
            containsString("left field [hire_date] of type [DATETIME] is incompatible with right field [hire_date] of type [DATE_NANOS]")
        );
    }

    /**
     * Verifies that DATE_NANOS left vs DATETIME right is incompatible in IN subquery.
     */
    public void testRejectsDateNanosVsDateInSubquery() {
        errorWithIncompatible(
            """
                FROM employees_incompatible
                | WHERE hire_date IN (FROM test | KEEP hire_date)
                """,
            containsString("left field [hire_date] of type [DATE_NANOS] is incompatible with right field [hire_date] of type [DATETIME]")
        );
    }

    // -- union type tests --

    /**
     * Verifies that a union type field (id: keyword + integer) as the left join key of IN subquery
     * fails without explicit casting.
     */
    public void testRejectsUnionTypeLeftFieldInSubquery() {
        errorWithUnionIndex("""
            FROM union_index*
            | WHERE id IN (FROM test | KEEP emp_no)
            | KEEP id
            """, containsString("id"));
    }

    /**
     * Verifies that a union type field as the right join key of IN subquery fails without explicit casting.
     */
    public void testRejectsUnionTypeRightFieldInSubquery() {
        errorWithUnionIndex("""
            FROM test
            | WHERE first_name IN (FROM union_index* | KEEP id)
            | KEEP first_name
            """, containsString("id"));
    }

    /**
     * Verifies that casting a union type field to a concrete type on the left side resolves the issue.
     */
    public void testUnionTypeLeftFieldWithCastInSubquery() {
        var plan = analyzeWithUnionIndex("""
            FROM union_index*
            | EVAL id_kw = id::keyword
            | WHERE id_kw IN (FROM test | KEEP first_name)
            | KEEP id_kw
            """);
        assertNotNull(plan);
    }

    /**
     * Verifies that casting a union type field to a concrete type on the right side resolves the issue.
     */
    public void testUnionTypeRightFieldWithCastInSubquery() {
        var plan = analyzeWithUnionIndex("""
            FROM test
            | WHERE first_name IN (FROM union_index* | EVAL id_kw = id::keyword | KEEP id_kw)
            | KEEP first_name
            """);
        assertNotNull(plan);
    }

    /**
     * Verifies that NOT IN with a union type field on the left fails without casting.
     */
    public void testRejectsUnionTypeLeftFieldInAntiJoin() {
        errorWithUnionIndex("""
            FROM union_index*
            | WHERE id NOT IN (FROM test | KEEP emp_no)
            | KEEP id
            """, containsString("id"));
    }

    /**
     * Verifies that NOT IN with a cast union type field on the left succeeds.
     */
    public void testUnionTypeLeftFieldWithCastInAntiJoin() {
        var plan = analyzeWithUnionIndex("""
            FROM union_index*
            | EVAL id_kw = id::keyword
            | WHERE id_kw NOT IN (FROM test | KEEP first_name)
            | KEEP id_kw
            """);
        assertNotNull(plan);
    }

    // -- union type tests with FROM subqueries --

    /**
     * Verifies that FROM subqueries with conflicting types for emp_no (integer + long) fail without casting.
     */
    public void testRejectsFromSubqueryUnionTypeLeftField() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        errorWithIncompatible("""
            FROM test, (FROM employees_incompatible | KEEP emp_no, first_name, salary)
            | WHERE emp_no IN (FROM test | WHERE salary > 70000 | KEEP emp_no)
            | KEEP emp_no
            """, containsString("Column [emp_no] has conflicting data types in subqueries: [integer, long]"));
    }

    /**
     * Verifies that FROM subqueries with conflicting types on the right side fail without casting.
     */
    public void testRejectsFromSubqueryUnionTypeRightField() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        errorWithIncompatible("""
            FROM test
            | WHERE emp_no IN (FROM test, (FROM employees_incompatible | KEEP emp_no) | KEEP emp_no)
            | KEEP emp_no
            """, containsString("Column [emp_no] has conflicting data types in subqueries: [integer, long]"));
    }

    /**
     * Verifies that casting resolves FROM subquery union type on the left side.
     */
    public void testFromSubqueryUnionTypeLeftFieldWithCast() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var plan = analyzeWithIncompatible("""
            FROM test, (FROM employees_incompatible | KEEP emp_no, first_name, salary)
            | EVAL id = emp_no::long
            | WHERE id IN (FROM employees_incompatible | WHERE salary > 70000 | KEEP emp_no)
            | KEEP id
            """);
        assertNotNull(plan);
    }

    /**
     * Verifies that casting resolves FROM subquery union type on the right side.
     */
    public void testFromSubqueryUnionTypeRightFieldWithCast() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var plan = analyzeWithIncompatible("""
            FROM test
            | WHERE emp_no IN (FROM test, (FROM employees_incompatible | KEEP emp_no) | EVAL id = emp_no::integer | KEEP id)
            | KEEP emp_no
            """);
        assertNotNull(plan);
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    @Override
    protected boolean enableWarningsCheck() {
        // Some tests call Approximation.verifyPlan which adds header warnings that can't be consumed in unit tests
        return false;
    }

    // -- helpers --

    private static LogicalPlan analyzeInSubquery(String query) {
        return analyzer().addIndex("test", "mapping-basic.json").addIndex("employees", "mapping-basic.json").query(query);
    }

    private static void errorInSubquery(String query, Matcher<String> messageMatcher) {
        analyzer().addIndex("test", "mapping-basic.json").addIndex("employees", "mapping-basic.json").error(query, messageMatcher);
    }

    private static void errorWithK8s(String query, Matcher<String> messageMatcher) {
        analyzer().addK8s().error(query, messageMatcher);
    }

    private static void errorWithK8sDownsampled(String query, Matcher<String> messageMatcher) {
        analyzer().addK8sDownsampled().error(query, messageMatcher);
    }

    private static LogicalPlan analyzeWithAllTypes(String query) {
        return analyzer().addIndex("all_types", "mapping-all-types.json").query(query);
    }

    private static void errorWithAllTypes(String query, Matcher<String> messageMatcher) {
        analyzer().addIndex("all_types", "mapping-all-types.json").error(query, messageMatcher);
    }

    private static LogicalPlan analyzeWithIncompatible(String query) {
        return analyzer().addIndex("test", "mapping-basic.json")
            .addIndex("employees_incompatible", "mapping-default-incompatible.json")
            .query(query);
    }

    private static void errorWithIncompatible(String query, Matcher<String> messageMatcher) {
        analyzer().addIndex("test", "mapping-basic.json")
            .addIndex("employees_incompatible", "mapping-default-incompatible.json")
            .error(query, messageMatcher);
    }

    private static IndexResolution unionIndexResolution() {
        LinkedHashMap<String, Set<String>> typesToIndices = new LinkedHashMap<>();
        typesToIndices.put("keyword", Set.of("union_index_1"));
        typesToIndices.put("integer", Set.of("union_index_2"));
        EsField idField = new InvalidMappedField("id", typesToIndices);
        EsField nameField = new EsField("name", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        EsIndex index = new EsIndex(
            "union_index*",
            Map.of("id", idField, "name", nameField),
            Map.of("union_index_1", IndexMode.STANDARD, "union_index_2", IndexMode.STANDARD),
            Map.of(),
            Map.of(),
            Map.of()
        );
        return IndexResolution.valid(index);
    }

    private static LogicalPlan analyzeWithUnionIndex(String query) {
        return analyzer().addIndex("test", "mapping-basic.json").addIndex(unionIndexResolution()).query(query);
    }

    private static void errorWithUnionIndex(String query, Matcher<String> messageMatcher) {
        analyzer().addIndex("test", "mapping-basic.json").addIndex(unionIndexResolution()).error(query, messageMatcher);
    }

    private void assertApproximationRejects(String query) {
        LogicalPlan plan = analyzeInSubquery(query);
        // verifyPlan returns null when the plan is incompatible with approximation (and adds a warning)
        assertThat("Approximation should reject this query", Approximation.verifyPlan(plan), nullValue());
    }
}
