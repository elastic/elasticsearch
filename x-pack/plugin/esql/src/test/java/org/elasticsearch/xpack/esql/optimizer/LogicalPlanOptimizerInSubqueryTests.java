/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.SemiJoin;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for IN/NOT IN subquery behavior that requires the logical plan optimizer,
 * specifically for post-optimization verification checks like nested UnionAll detection.
 */
public class LogicalPlanOptimizerInSubqueryTests extends AbstractLogicalPlanOptimizerTests {

    @Before
    public void checkInSubquerySupport() {
        assumeTrue("Requires IN_SUBQUERY support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_VIEW.isEnabled());
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
    }

    // disjunctive IN subqueries

    /**
     * Verifies that a disjunctive IN subquery inside a FROM subquery (which already creates a UnionAll)
     * is rejected because it would produce nested UnionAll.
     */
    public void testRejectsDisjunctiveInSubqueryInsideFromSubquery() {
        var e = expectThrows(VerificationException.class, () -> planInSubquery("""
            FROM test,
                 (FROM test | WHERE emp_no IN (FROM test | KEEP emp_no) OR salary > 50000 | KEEP emp_no)
            """));
        assertThat(e.getMessage(), containsString("Disjunctive (OR) IN subqueries are not supported inside FROM subqueries"));
    }

    /**
     * Verifies that a disjunctive NOT IN subquery inside a FROM subquery is rejected.
     */
    public void testRejectsDisjunctiveNotInSubqueryInsideFromSubquery() {
        var e = expectThrows(VerificationException.class, () -> planInSubquery("""
            FROM test,
                 (FROM test | WHERE emp_no NOT IN (FROM test | KEEP emp_no) OR salary > 50000 | KEEP emp_no)
            """));
        assertThat(e.getMessage(), containsString("Disjunctive (OR) IN subqueries are not supported inside FROM subqueries"));
    }

    /**
     * Verifies that nested disjunctive IN subqueries (OR inside OR) are rejected.
     */
    public void testRejectsNestedDisjunctiveInSubqueries() {
        var e = expectThrows(VerificationException.class, () -> planInSubquery("""
            FROM test
            | WHERE emp_no IN (
                FROM test
                | WHERE salary IN (FROM test | KEEP salary) OR languages > 2
                | KEEP emp_no
              ) OR salary > 50000
            """));
        assertThat(e.getMessage(), containsString("Nested disjunctive (OR) IN subqueries are not supported"));
    }

    /**
     * Verifies that a disjunctive IN subquery combined with FORK is rejected.
     */
    public void testRejectsDisjunctiveInSubqueryWithFork() {
        var e = expectThrows(VerificationException.class, () -> planInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM test | KEEP emp_no) OR salary > 50000
            | FORK (WHERE emp_no > 10000) (WHERE emp_no < 10050)
            """));
        assertThat(e.getMessage(), containsString("FORK after disjunctive (OR) IN subquery is not supported"));
    }

    /**
     * Verifies that a disjunctive IN subquery at the top level (no FROM subquery nesting) works fine.
     * This should NOT produce nested UnionAll since there's only one level.
     */
    public void testDisjunctiveInSubqueryAtTopLevelIsAllowed() {
        // Disjunct reordering puts salary > 50000 (complexity 0) before emp_no IN (complexity 1)
        LogicalPlan plan = planInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM test | KEEP emp_no) OR salary > 50000
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // Branch 1: salary > 50000 → Project -> Filter -> EsRelation
        Project branch1Project = as(unionAll.children().get(0), Project.class);
        Filter branch1Filter = as(branch1Project.child(), Filter.class);
        GreaterThan branch1Gt = as(branch1Filter.condition(), GreaterThan.class);
        FieldAttribute branch1Salary = as(branch1Gt.left(), FieldAttribute.class);
        assertEquals("salary", branch1Salary.name());
        EsRelation branch1Rel = as(branch1Filter.child(), EsRelation.class);
        assertEquals("test", branch1Rel.indexPattern());

        // Branch 2: NOT(salary > 50000) AND emp_no IN (...) → Project -> SemiJoin
        Project branch2Project = as(unionAll.children().get(1), Project.class);
        SemiJoin branch2Semi = as(branch2Project.child(), SemiJoin.class);
        assertThat(branch2Semi.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(branch2Semi.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(branch2Semi.config().rightFields().get(0).name(), equalTo("emp_no"));
        assertThat(branch2Semi.output(), equalTo(branch2Semi.left().output()));
        Project branch2SubqueryProject = as(branch2Semi.right(), Project.class);
        EsRelation branch2SubqueryRel = as(branch2SubqueryProject.child(), EsRelation.class);
        assertEquals("test", branch2SubqueryRel.indexPattern());
        Filter branch2Filter = as(branch2Semi.left(), Filter.class);
        Not not = as(branch2Filter.condition(), Not.class);
        as(not.field(), GreaterThan.class);
        EsRelation branch2Rel = as(branch2Filter.child(), EsRelation.class);
        assertEquals("test", branch2Rel.indexPattern());
    }

    // -- SORT inside IN subquery tests --

    /**
     * Verifies that SORT without LIMIT inside an IN subquery is rejected.
     */
    public void testRejectsSortWithoutLimitInInSubquery() {
        var e = expectThrows(VerificationException.class, () -> planInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM test | SORT emp_no | KEEP emp_no)
            """));
        assertThat(
            e.getMessage(),
            containsString("IN subquery [emp_no IN (FROM test | SORT emp_no | KEEP emp_no)] cannot yet have an unbounded SORT")
        );
    }

    /**
     * Verifies that SORT without LIMIT inside a NOT IN subquery is rejected.
     */
    public void testRejectsSortWithoutLimitInNotInSubquery() {
        var e = expectThrows(VerificationException.class, () -> planInSubquery("""
            FROM test
            | WHERE emp_no NOT IN (FROM test | SORT emp_no DESC | KEEP emp_no)
            """));
        assertThat(
            e.getMessage(),
            containsString("IN subquery [emp_no NOT IN (FROM test | SORT emp_no DESC | KEEP emp_no)] cannot yet have an unbounded SORT")
        );
    }

    /**
     * Verifies that SORT with LIMIT inside an IN subquery is allowed.
     */
    public void testSortWithLimitInSubqueryIsAllowed() {
        LogicalPlan plan = planInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM test | SORT emp_no | LIMIT 5 | KEEP emp_no)
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.output(), equalTo(semiJoin.left().output()));

        EsRelation leftRelation = as(semiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        // Right side: optimizer merges SORT+LIMIT into TopN → Project -> TopN -> EsRelation
        Project rightProject = as(semiJoin.right(), Project.class);
        TopN topN = as(rightProject.child(), TopN.class);
        EsRelation rightRelation = as(topN.child(), EsRelation.class);
        assertEquals("test", rightRelation.indexPattern());
    }

    // -- optimizer rules applied inside IN subquery --

    public void testStatsWithSortLimitInSubquery() {
        LogicalPlan plan = planInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM test | STATS m = MAX(emp_no) BY languages | SORT m | LIMIT 3 | KEEP m)
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("m"));

        EsRelation leftRelation = as(semiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        // Right side: Project -> TopN -> Aggregate -> EsRelation
        Project rightProject = as(semiJoin.right(), Project.class);
        TopN topN = as(rightProject.child(), TopN.class);
        Aggregate agg = as(topN.child(), Aggregate.class);
        assertEquals(1, agg.groupings().size());
        EsRelation rightRelation = as(agg.child(), EsRelation.class);
        assertEquals("test", rightRelation.indexPattern());
    }

    public void testMultipleFiltersInSubqueryCombined() {
        LogicalPlan plan = planInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM test | WHERE salary > 50000 | WHERE languages > 2 | KEEP emp_no)
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        // Right side: optimizer combines the two WHERE filters into one
        // Project -> Filter[combined] -> EsRelation
        Project rightProject = as(semiJoin.right(), Project.class);
        Filter rightFilter = as(rightProject.child(), Filter.class);
        And and = as(rightFilter.condition(), And.class);
        GreaterThan greaterThan = as(and.left(), GreaterThan.class);
        FieldAttribute salary = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("salary", salary.name());
        greaterThan = as(and.right(), GreaterThan.class);
        FieldAttribute languages = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("languages", languages.name());
        EsRelation rightRelation = as(rightFilter.child(), EsRelation.class);
        assertEquals("test", rightRelation.indexPattern());
    }

    public void testCombineDisjunctionsInsideInSubquery() {
        LogicalPlan plan = planInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM test | WHERE salary == 50000 or salary == 10000 | KEEP emp_no)
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));
        // combine the two equal predicates into an in list predicate
        Project rightProject = as(semiJoin.right(), Project.class);
        Filter rightFilter = as(rightProject.child(), Filter.class);
        In in = as(rightFilter.condition(), In.class);
        FieldAttribute salary = as(in.value(), FieldAttribute.class);
        assertEquals("salary", salary.name());
        assertEquals(2, in.list().size());
        EsRelation rightRelation = as(rightFilter.child(), EsRelation.class);
        assertEquals("test", rightRelation.indexPattern());
    }

    private LogicalPlan planInSubquery(String query) {
        TestAnalyzer inSubqueryAnalyzer = analyzer().addEmployees("test");
        return optimize(inSubqueryAnalyzer.query(query));
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
