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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.LeftSemiJoin;
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
        assumeTrue("Requires IN_SUBQUERY support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
    }

    // disjunctive IN subqueries
    //
    // Disjunctive IN/NOT IN subqueries are rewritten to {@link LeftSemiJoin}s that emit a synthetic
    // boolean mark attribute referenced from the rewritten WHERE condition. Because the mark
    // semantics correctly handle SQL three-valued logic for NULLs, scenarios that the previous
    // UNION ALL rewrite had to reject (FROM subqueries, FORK, nested disjunctive IN) are now allowed.

    /**
     * Disjunctive IN subqueries at the top level produce a {@link LeftSemiJoin} per IN subquery
     * with a {@link Filter} referencing the mark attributes via {@link Or}. The final {@link Project}
     * strips the synthetic mark column so the apparent output schema is preserved.
     */
    public void testDisjunctiveInSubqueryAtTopLevel() {
        LogicalPlan plan = planInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM test | KEEP emp_no) OR salary > 50000
            """);

        Project topProject = as(plan, Project.class);
        Limit limit = as(topProject.child(), Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        Or or = as(filter.condition(), Or.class);
        as(or.left(), Attribute.class);
        as(or.right(), GreaterThan.class);

        LeftSemiJoin lsj = as(filter.child(), LeftSemiJoin.class);
        assertThat(lsj.config().type(), equalTo(JoinTypes.LEFT_SEMI));
        assertThat(lsj.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(lsj.config().rightFields().get(0).name(), equalTo("emp_no"));
        Project subqueryProject = as(lsj.right(), Project.class);
        EsRelation subqueryRelation = as(subqueryProject.child(), EsRelation.class);
        assertEquals("test", subqueryRelation.indexPattern());
        EsRelation main = as(lsj.left(), EsRelation.class);
        assertEquals("test", main.indexPattern());
    }

    /**
     * Disjunctive IN subqueries are now accepted inside FROM subqueries — the {@link LeftSemiJoin}
     * rewrite does not introduce {@link org.elasticsearch.xpack.esql.plan.logical.UnionAll UnionAll}
     * so there is no nesting concern.
     */
    public void testDisjunctiveInSubqueryInsideFromSubquery() {
        LogicalPlan plan = planInSubquery("""
            FROM test,
                 (FROM test | WHERE emp_no IN (FROM test | KEEP emp_no) OR salary > 50000 | KEEP emp_no)
            """);
        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        EsRelation mainRelation = as(subqueryProject.child(), EsRelation.class);
        assertEquals("test", mainRelation.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        Eval eval = as(subqueryProject.child(), Eval.class);
        Subquery subquery = as(eval.child(), Subquery.class);
        subqueryProject = as(subquery.child(), Project.class);
        Filter filter = as(subqueryProject.child(), Filter.class);
        Or or = as(filter.condition(), Or.class);
        as(or.left(), Attribute.class);
        as(or.right(), GreaterThan.class);
        LeftSemiJoin lsj = as(filter.child(), LeftSemiJoin.class);
        assertThat(lsj.config().type(), equalTo(JoinTypes.LEFT_SEMI));
        assertThat(lsj.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(lsj.config().rightFields().get(0).name(), equalTo("emp_no"));
        subqueryProject = as(lsj.right(), Project.class);
        EsRelation subqueryRelation = as(subqueryProject.child(), EsRelation.class);
        assertEquals("test", subqueryRelation.indexPattern());
        subqueryRelation = as(lsj.left(), EsRelation.class);
        assertEquals("test", subqueryRelation.indexPattern());
    }

    /**
     * Disjunctive NOT IN subqueries are now accepted inside FROM subqueries (see above).
     */
    public void testDisjunctiveNotInSubqueryInsideFromSubquery() {
        LogicalPlan plan = planInSubquery("""
            FROM test,
                 (FROM test | WHERE emp_no NOT IN (FROM test | KEEP emp_no) OR salary > 50000 | KEEP emp_no)
            """);
        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        EsRelation mainRelation = as(subqueryProject.child(), EsRelation.class);
        assertEquals("test", mainRelation.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        Eval eval = as(subqueryProject.child(), Eval.class);
        Subquery subquery = as(eval.child(), Subquery.class);
        subqueryProject = as(subquery.child(), Project.class);
        Filter filter = as(subqueryProject.child(), Filter.class);
        Or or = as(filter.condition(), Or.class);
        Not not = as(or.left(), Not.class);
        as(not.field(), Attribute.class);
        as(or.right(), GreaterThan.class);
        LeftSemiJoin lsj = as(filter.child(), LeftSemiJoin.class);
        assertThat(lsj.config().type(), equalTo(JoinTypes.LEFT_SEMI));
        assertThat(lsj.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(lsj.config().rightFields().get(0).name(), equalTo("emp_no"));
        subqueryProject = as(lsj.right(), Project.class);
        EsRelation subqueryRelation = as(subqueryProject.child(), EsRelation.class);
        assertEquals("test", subqueryRelation.indexPattern());
        subqueryRelation = as(lsj.left(), EsRelation.class);
        assertEquals("test", subqueryRelation.indexPattern());
    }

    /**
     * Nested disjunctive IN subqueries (OR inside OR across nesting levels) are now accepted; each
     * level rewrites to its own {@link LeftSemiJoin}.
     */
    public void testNestedDisjunctiveInSubqueries() {
        LogicalPlan plan = planInSubquery("""
            FROM test
            | WHERE emp_no IN (
                FROM test
                | WHERE salary IN (FROM test | KEEP salary) OR languages > 2
                | KEEP emp_no
              ) OR salary > 50000
            """);
        Project project = as(plan, Project.class);
        Limit limit = as(project.child(), Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        Or or = as(filter.condition(), Or.class);
        as(or.left(), Attribute.class);
        as(or.right(), GreaterThan.class);
        LeftSemiJoin lsj = as(filter.child(), LeftSemiJoin.class);
        assertThat(lsj.config().type(), equalTo(JoinTypes.LEFT_SEMI));
        assertThat(lsj.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(lsj.config().rightFields().get(0).name(), equalTo("emp_no"));
        EsRelation subqueryRelation = as(lsj.left(), EsRelation.class);
        assertEquals("test", subqueryRelation.indexPattern());
        Project subqueryProject = as(lsj.right(), Project.class);
        filter = as(subqueryProject.child(), Filter.class);
        or = as(filter.condition(), Or.class);
        as(or.left(), Attribute.class);
        as(or.right(), GreaterThan.class);
        lsj = as(filter.child(), LeftSemiJoin.class);
        subqueryRelation = as(lsj.left(), EsRelation.class);
        assertEquals("test", subqueryRelation.indexPattern());
        subqueryProject = as(lsj.right(), Project.class);
        subqueryRelation = as(subqueryProject.child(), EsRelation.class);
        assertEquals("test", subqueryRelation.indexPattern());
    }

    /**
     * FORK is now compatible with disjunctive IN subqueries — the rewrite no longer emits UnionAll.
     */
    public void testDisjunctiveInSubqueryWithFork() {
        LogicalPlan plan = planInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM test | KEEP emp_no) OR salary > 50000
            | FORK (WHERE emp_no > 10000) (WHERE emp_no < 10050)
            """);
        Project project = as(plan, Project.class);
        Limit limit = as(project.child(), Limit.class);
        Fork fork = as(limit.child(), Fork.class);
        assertEquals(2, fork.children().size());

        for (int i = 0; i < fork.children().size(); i++) {
            Project forkProject = as(fork.children().get(i), Project.class);
            Eval forkEval = as(forkProject.child(), Eval.class);
            Limit forkLimit = as(forkEval.child(), Limit.class);
            Filter forkFilter = as(forkLimit.child(), Filter.class);
            LeftSemiJoin forkLsj = as(forkFilter.child(), LeftSemiJoin.class);
            EsRelation relation = as(forkLsj.left(), EsRelation.class);
            assertEquals("test", relation.indexPattern());
            Project subqueryProject = as(forkLsj.right(), Project.class);
            relation = as(subqueryProject.child(), EsRelation.class);
            assertEquals("test", relation.indexPattern());
        }
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
