/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Median;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.optimizer.rules.PlanConsistencyChecker;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizerTests.relation;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Unit tests that verify optimizer pushdown rules behave correctly for the heterogeneous FROM shape —
 * a direct-leaf {@link UnionAll} where children are {@link EsRelation} or {@link ExternalRelation}
 * (rather than the subquery-shape where children are {@code Project > Eval? > Subquery}).
 */
public class HeterogeneousFromOptimizerTests extends AbstractLogicalPlanOptimizerTests {

    /**
     * {@link PushDownFilterAndLimitIntoUnionAll} must push a filter predicate into both branches
     * of a direct-leaf UnionAll ({@link EsRelation} and {@link ExternalRelation}).
     *
     * <pre>{@code
     * -- Input:
     * Filter[emp_no > 1]
     *   UnionAll[[emp_no{r}]]
     *     EsRelation[[emp_no{f}]]
     *     ExternalRelation[[emp_no{f}]]
     *
     * -- Expected:
     * UnionAll[[emp_no{r}]]
     *   Filter[emp_no > 1]
     *     EsRelation[[emp_no{f}]]
     *   Filter[emp_no > 1]
     *     ExternalRelation[[emp_no{f}]]
     * }</pre>
     */
    public void testFilterPushedDownIntoLeafUnionAll() {
        // UnionAll output uses a ReferenceAttribute — the filter condition must reference it by ID
        ReferenceAttribute unionEmpNo = new ReferenceAttribute(EMPTY, "emp_no", INTEGER);

        FieldAttribute esEmpNo = getFieldAttribute("emp_no", INTEGER);
        EsRelation esRelation = relation().withAttributes(List.of(esEmpNo));

        Attribute extEmpNo = extAttr("emp_no", INTEGER);
        ExternalRelation extRelation = externalRelation(List.of(extEmpNo));

        UnionAll unionAll = new UnionAll(EMPTY, List.of(esRelation, extRelation), List.of(unionEmpNo));

        GreaterThan condition = new GreaterThan(EMPTY, unionEmpNo, new Literal(EMPTY, 1, INTEGER), null);
        Filter filter = new Filter(EMPTY, unionAll, condition);

        LogicalPlan result = new PushDownFilterAndLimitIntoUnionAll().apply(filter, unboundLogicalOptimizerContext());

        UnionAll resultUnionAll = as(result, UnionAll.class);
        assertThat(resultUnionAll.children(), hasSize(2));

        Filter branch1Filter = as(resultUnionAll.children().get(0), Filter.class);
        as(branch1Filter.child(), EsRelation.class);

        Filter branch2Filter = as(resultUnionAll.children().get(1), Filter.class);
        as(branch2Filter.child(), ExternalRelation.class);
    }

    /**
     * {@link PushDownAndCombineLimits} must not push a LIMIT into the branches of a direct-leaf
     * UnionAll — the limit must be applied globally across the combined results.
     */
    public void testLimitNotPushedIntoLeafUnionAll() {
        ReferenceAttribute unionEmpNo = new ReferenceAttribute(EMPTY, "emp_no", INTEGER);

        FieldAttribute esEmpNo = getFieldAttribute("emp_no", INTEGER);
        EsRelation esRelation = relation().withAttributes(List.of(esEmpNo));

        Attribute extEmpNo = extAttr("emp_no", INTEGER);
        ExternalRelation extRelation = externalRelation(List.of(extEmpNo));

        UnionAll unionAll = new UnionAll(EMPTY, List.of(esRelation, extRelation), List.of(unionEmpNo));
        Limit limit = new Limit(EMPTY, new Literal(EMPTY, 10, INTEGER), unionAll);

        LogicalPlan result = new PushDownAndCombineLimits().apply(limit, unboundLogicalOptimizerContext());

        // Limit must remain on top — it must not be pushed into each branch
        Limit resultLimit = as(result, Limit.class);
        UnionAll resultUnionAll = as(resultLimit.child(), UnionAll.class);
        // Branches must still be bare leaf plans (not wrapped in Limit)
        as(resultUnionAll.children().get(0), EsRelation.class);
        as(resultUnionAll.children().get(1), ExternalRelation.class);
    }

    /**
     * {@link PruneColumns} must prune unused columns from the {@link ExternalRelation} inside a
     * direct-leaf UnionAll while leaving the {@link EsRelation} unchanged (InsertFieldExtraction
     * handles EsRelation column pruning at execution time).
     *
     * <p>The test uses the {@link ExternalRelation}'s own attribute object as the {@link UnionAll}
     * output so that the attribute IDs in the {@code used} set match those in the external relation.
     * This matches the semantics the pruning rule relies on.
     */
    public void testColumnsPrunedForExternalRelationInLeafUnionAll() {
        Attribute empNo = extAttr("emp_no", INTEGER);
        Attribute salary = extAttr("salary", INTEGER);

        ExternalRelation extRelation = externalRelation(List.of(empNo, salary));

        FieldAttribute esEmpNo = getFieldAttribute("emp_no", INTEGER);
        FieldAttribute esSalary = getFieldAttribute("salary", INTEGER);
        EsRelation esRelation = relation().withAttributes(List.of(esEmpNo, esSalary));

        // UnionAll output uses empNo directly (same ID as ExternalRelation's attribute) so that
        // PruneColumns' ID-based 'used' tracking can reach the external relation's columns.
        UnionAll unionAll = new UnionAll(EMPTY, List.of(esRelation, extRelation), List.of(empNo));

        // Project keeps only emp_no
        Project project = new Project(EMPTY, unionAll, List.of(empNo));

        LogicalPlan result = new PruneColumns().apply(project);

        Project resultProject = as(result, Project.class);
        UnionAll resultUnionAll = as(resultProject.child(), UnionAll.class);
        assertThat(resultUnionAll.children(), hasSize(2));

        // EsRelation is left unchanged — InsertFieldExtraction handles its column extraction
        EsRelation esR = as(resultUnionAll.children().get(0), EsRelation.class);
        assertThat(esR.output(), hasSize(2));

        // ExternalRelation is pruned: only emp_no survives
        ExternalRelation extR = as(resultUnionAll.children().get(1), ExternalRelation.class);
        assertThat(extR.output(), hasSize(1));
        assertThat(Expressions.names(extR.output()), contains("emp_no"));
    }

    /**
     * {@link PushDownLimitAndOrderByIntoFork} must push a SORT + LIMIT (TopN) into both branches
     * of a direct-leaf UnionAll, enabling partial sorting at each data source.
     *
     * <pre>{@code
     * -- Input:
     * Limit[10]
     *   OrderBy[emp_no ASC]
     *     UnionAll[[emp_no{r}]]
     *       EsRelation[[emp_no{f}]]
     *       ExternalRelation[[emp_no{f}]]
     *
     * -- Expected:
     * Limit[10]
     *   OrderBy[emp_no ASC]
     *     UnionAll[[emp_no{r}]]
     *       Limit[10]
     *         OrderBy[emp_no ASC]
     *           EsRelation[[emp_no{f}]]
     *       Limit[10]
     *         OrderBy[emp_no ASC]
     *           ExternalRelation[[emp_no{f}]]
     * }</pre>
     */
    public void testTopNPushedIntoLeafUnionAll() {
        ReferenceAttribute unionEmpNo = new ReferenceAttribute(EMPTY, "emp_no", INTEGER);

        FieldAttribute esEmpNo = getFieldAttribute("emp_no", INTEGER);
        EsRelation esRelation = relation().withAttributes(List.of(esEmpNo));

        Attribute extEmpNo = extAttr("emp_no", INTEGER);
        ExternalRelation extRelation = externalRelation(List.of(extEmpNo));

        UnionAll unionAll = new UnionAll(EMPTY, List.of(esRelation, extRelation), List.of(unionEmpNo));

        Order order = new Order(EMPTY, unionEmpNo, Order.OrderDirection.ASC, null);
        OrderBy orderBy = new OrderBy(EMPTY, unionAll, List.of(order));
        Limit limit = new Limit(EMPTY, new Literal(EMPTY, 10, INTEGER), orderBy);

        LogicalPlan result = new PushDownLimitAndOrderByIntoFork().apply(limit, unboundLogicalOptimizerContext());

        // Outer structure: Limit → OrderBy → UnionAll remains intact
        Limit resultLimit = as(result, Limit.class);
        OrderBy resultOrderBy = as(resultLimit.child(), OrderBy.class);
        UnionAll resultUnionAll = as(resultOrderBy.child(), UnionAll.class);
        assertThat(resultUnionAll.children(), hasSize(2));

        // First branch: Limit → OrderBy → EsRelation
        Limit branch1Limit = as(resultUnionAll.children().get(0), Limit.class);
        OrderBy branch1OrderBy = as(branch1Limit.child(), OrderBy.class);
        as(branch1OrderBy.child(), EsRelation.class);

        // Second branch: Limit → OrderBy → ExternalRelation
        Limit branch2Limit = as(resultUnionAll.children().get(1), Limit.class);
        OrderBy branch2OrderBy = as(branch2Limit.child(), OrderBy.class);
        as(branch2OrderBy.child(), ExternalRelation.class);
    }

    // -------------------------------------------------------------------------
    // PushAggregateThroughUnionAll tests
    // -------------------------------------------------------------------------

    /**
     * {@link PushAggregateThroughUnionAll} must push {@code COUNT(*)} (no grouping) into both
     * branches and replace the outer aggregate with a SUM over the per-branch counts.
     *
     * <pre>{@code
     * -- Input:
     * Aggregate[c = COUNT(*)] BY []
     *   UnionAll[[emp_no{r}]]
     *     EsRelation[[emp_no{f}]]
     *     ExternalRelation[[emp_no{f}]]
     *
     * -- Expected:
     * Aggregate[c = SUM($$partial$$c)] BY []
     *   UnionAll[[$$partial$$c]]
     *     Aggregate[$$partial$$c = COUNT(*)] BY []
     *       EsRelation[[emp_no{f}]]
     *     Aggregate[$$partial$$c = COUNT(*)] BY []
     *       ExternalRelation[[emp_no{f}]]
     * }</pre>
     */
    public void testCountStarPushedThroughLeafUnionAll() {
        ReferenceAttribute unionEmpNo = new ReferenceAttribute(EMPTY, "emp_no", INTEGER);

        EsRelation esRelation = relation().withAttributes(List.of(getFieldAttribute("emp_no", INTEGER)));
        ExternalRelation extRelation = externalRelation(List.of(extAttr("emp_no", INTEGER)));

        UnionAll unionAll = new UnionAll(EMPTY, List.of(esRelation, extRelation), List.of(unionEmpNo));

        Alias countAlias = new Alias(EMPTY, "c", new Count(EMPTY, Literal.TRUE));
        Aggregate aggregate = new Aggregate(EMPTY, unionAll, List.of(), List.of(countAlias));

        LogicalPlan result = new PushAggregateThroughUnionAll().apply(aggregate);

        // Outer: Aggregate with no groupings, one SUM combiner
        Aggregate outerAgg = as(result, Aggregate.class);
        assertThat(outerAgg.groupings(), empty());
        assertThat(outerAgg.aggregates(), hasSize(1));

        Alias outerAlias = as(outerAgg.aggregates().get(0), Alias.class);
        assertThat(outerAlias.name(), equalTo("c"));
        assertThat(outerAlias.id(), equalTo(countAlias.id())); // original output ID preserved
        assertThat(outerAlias.child(), instanceOf(Sum.class));
        Sum combinerSum = (Sum) outerAlias.child();
        assertThat(combinerSum.field(), instanceOf(ReferenceAttribute.class));
        ReferenceAttribute partialRef = (ReferenceAttribute) combinerSum.field();
        assertThat(partialRef.name(), equalTo("$$partial$$c"));

        // Outer UnionAll has exactly one output column: the partial count
        UnionAll newUnionAll = as(outerAgg.child(), UnionAll.class);
        assertThat(newUnionAll.output(), hasSize(1));
        assertThat(newUnionAll.output().get(0).name(), equalTo("$$partial$$c"));
        assertThat(newUnionAll.output().get(0).id(), equalTo(partialRef.id()));

        assertThat(newUnionAll.children(), hasSize(2));

        // Branch 1: inner Aggregate over EsRelation with COUNT(*)
        Aggregate branch1 = as(newUnionAll.children().get(0), Aggregate.class);
        as(branch1.child(), EsRelation.class);
        assertThat(branch1.groupings(), empty());
        assertThat(branch1.aggregates(), hasSize(1));
        Alias branch1Alias = as(branch1.aggregates().get(0), Alias.class);
        assertThat(branch1Alias.name(), equalTo("$$partial$$c"));
        assertThat(branch1Alias.id(), equalTo(partialRef.id())); // shared ID
        assertThat(branch1Alias.child(), instanceOf(Count.class));

        // Branch 2: inner Aggregate over ExternalRelation with COUNT(*)
        Aggregate branch2 = as(newUnionAll.children().get(1), Aggregate.class);
        as(branch2.child(), ExternalRelation.class);
        assertThat(branch2.groupings(), empty());
        assertThat(branch2.aggregates(), hasSize(1));
        Alias branch2Alias = as(branch2.aggregates().get(0), Alias.class);
        assertThat(branch2Alias.name(), equalTo("$$partial$$c"));
        assertThat(branch2Alias.id(), equalTo(partialRef.id())); // same shared ID across branches
        assertThat(branch2Alias.child(), instanceOf(Count.class));
    }

    /**
     * {@link PushAggregateThroughUnionAll} must push {@code COUNT(*)} and {@code MAX(salary)}
     * grouped by {@code dept} into both branches, resolving field references per branch, and
     * produce the correct combiner aggregate with SUM and MAX over the partial results.
     *
     * <pre>{@code
     * -- Input:
     * Aggregate[c = COUNT(*), m = MAX(salary), dept] BY [dept]
     *   UnionAll[[dept{r1}, salary{r2}]]
     *     EsRelation[[dept{f1}, salary{f2}]]
     *     ExternalRelation[[dept{f3}, salary{f4}]]
     * }</pre>
     */
    public void testCountAndMaxByDeptPushedThroughLeafUnionAll() {
        ReferenceAttribute unionDept = new ReferenceAttribute(EMPTY, "dept", INTEGER);
        ReferenceAttribute unionSalary = new ReferenceAttribute(EMPTY, "salary", INTEGER);

        FieldAttribute esDept = getFieldAttribute("dept", INTEGER);
        FieldAttribute esSalary = getFieldAttribute("salary", INTEGER);
        EsRelation esRelation = relation().withAttributes(List.of(esDept, esSalary));

        Attribute extDept = extAttr("dept", INTEGER);
        Attribute extSalary = extAttr("salary", INTEGER);
        ExternalRelation extRelation = externalRelation(List.of(extDept, extSalary));

        UnionAll unionAll = new UnionAll(EMPTY, List.of(esRelation, extRelation), List.of(unionDept, unionSalary));

        Alias countAlias = new Alias(EMPTY, "c", new Count(EMPTY, Literal.TRUE));
        Alias maxAlias = new Alias(EMPTY, "m", new Max(EMPTY, unionSalary));
        // aggregates: [c = COUNT(*), m = MAX(salary), dept] groupings: [dept]
        Aggregate aggregate = new Aggregate(EMPTY, unionAll, List.of(unionDept), List.of(countAlias, maxAlias, unionDept));

        LogicalPlan result = new PushAggregateThroughUnionAll().apply(aggregate);

        // Outer combiner Aggregate
        Aggregate outerAgg = as(result, Aggregate.class);
        assertThat(outerAgg.groupings(), hasSize(1));
        assertThat(outerAgg.aggregates(), hasSize(3)); // c combiner, m combiner, dept passthrough

        // Outer aggregates[0]: c = SUM($$partial$$c)
        Alias outerCount = as(outerAgg.aggregates().get(0), Alias.class);
        assertThat(outerCount.name(), equalTo("c"));
        assertThat(outerCount.id(), equalTo(countAlias.id()));
        assertThat(outerCount.child(), instanceOf(Sum.class));

        // Outer aggregates[1]: m = MAX($$partial$$m)
        Alias outerMax = as(outerAgg.aggregates().get(1), Alias.class);
        assertThat(outerMax.name(), equalTo("m"));
        assertThat(outerMax.id(), equalTo(maxAlias.id()));
        assertThat(outerMax.child(), instanceOf(Max.class));

        // Outer aggregates[2]: dept passthrough with original ID
        Alias outerDept = as(outerAgg.aggregates().get(2), Alias.class);
        assertThat(outerDept.name(), equalTo("dept"));
        assertThat(outerDept.id(), equalTo(unionDept.id())); // original grouping column ID preserved

        // Outer grouping references the shared grouping ID
        ReferenceAttribute outerGroupingRef = as(outerAgg.groupings().get(0), ReferenceAttribute.class);
        assertThat(outerGroupingRef.name(), equalTo("dept"));
        // sharedGId: the outer dept alias's child is a ReferenceAttribute with the shared ID
        ReferenceAttribute outerDeptChild = as(outerDept.child(), ReferenceAttribute.class);
        assertThat(outerGroupingRef.id(), equalTo(outerDeptChild.id()));

        // Outer UnionAll: 3 output columns ($$partial$$c, $$partial$$m, dept{shared})
        UnionAll newUnionAll = as(outerAgg.child(), UnionAll.class);
        assertThat(newUnionAll.output(), hasSize(3));
        assertThat(Expressions.names(newUnionAll.output()), contains("$$partial$$c", "$$partial$$m", "dept"));
        assertThat(newUnionAll.children(), hasSize(2));

        // Branch 1: inner Aggregate over EsRelation
        Aggregate branch1 = as(newUnionAll.children().get(0), Aggregate.class);
        as(branch1.child(), EsRelation.class);
        assertThat(branch1.groupings(), hasSize(1));
        assertThat(branch1.aggregates(), hasSize(3));

        // Branch 1 aggregates[0]: $$partial$$c = COUNT(*)
        Alias b1Count = as(branch1.aggregates().get(0), Alias.class);
        assertThat(b1Count.name(), equalTo("$$partial$$c"));
        assertThat(b1Count.child(), instanceOf(Count.class));

        // Branch 1 aggregates[1]: $$partial$$m = MAX(salary{f2})
        Alias b1Max = as(branch1.aggregates().get(1), Alias.class);
        assertThat(b1Max.name(), equalTo("$$partial$$m"));
        Max b1MaxFn = as(b1Max.child(), Max.class);
        assertThat(b1MaxFn.field(), equalTo(esSalary)); // resolved to branch's FieldAttribute

        // Branch 2: inner Aggregate over ExternalRelation
        Aggregate branch2 = as(newUnionAll.children().get(1), Aggregate.class);
        as(branch2.child(), ExternalRelation.class);
        Alias b2Max = as(branch2.aggregates().get(1), Alias.class);
        Max b2MaxFn = as(b2Max.child(), Max.class);
        assertThat(b2MaxFn.field(), equalTo(extSalary)); // resolved to external branch's attribute

        // Partial IDs are shared across branches
        assertThat(branch1.aggregates().get(0).id(), equalTo(branch2.aggregates().get(0).id())); // $$partial$$c
        assertThat(branch1.aggregates().get(1).id(), equalTo(branch2.aggregates().get(1).id())); // $$partial$$m
        assertThat(branch1.aggregates().get(2).id(), equalTo(branch2.aggregates().get(2).id())); // dept shared

        // Shared ID matches the outer UnionAll output and the outer grouping
        assertThat(branch1.aggregates().get(0).id(), equalTo(newUnionAll.output().get(0).id()));
        assertThat(branch1.aggregates().get(1).id(), equalTo(newUnionAll.output().get(1).id()));
        assertThat(branch1.aggregates().get(2).id(), equalTo(newUnionAll.output().get(2).id()));
    }

    /**
     * {@link PushAggregateThroughUnionAll} must leave windowed aggregates untouched —
     * they cannot be split across branches.
     */
    public void testWindowedAggNotPushed() {
        ReferenceAttribute unionField = new ReferenceAttribute(EMPTY, "x", INTEGER);
        EsRelation esRelation = relation().withAttributes(List.of(getFieldAttribute("x", INTEGER)));
        ExternalRelation extRelation = externalRelation(List.of(extAttr("x", INTEGER)));
        UnionAll unionAll = new UnionAll(EMPTY, List.of(esRelation, extRelation), List.of(unionField));

        // Count with a non-trivial window (not NO_WINDOW)
        Literal window = Literal.timeDuration(EMPTY, Duration.ofSeconds(1));
        AggregateFunction windowedCount = new Count(EMPTY, Literal.TRUE, Literal.TRUE, window);
        Alias alias = new Alias(EMPTY, "c", windowedCount);
        Aggregate aggregate = new Aggregate(EMPTY, unionAll, List.of(), List.of(alias));

        LogicalPlan result = new PushAggregateThroughUnionAll().apply(aggregate);

        // Rule must not fire — the plan is returned unchanged
        assertSame(aggregate, result);
    }

    /**
     * {@link PushAggregateThroughUnionAll} must not push aggregates into a subquery-shape
     * UnionAll (children are Project → Relation, not bare leaf relations).
     */
    public void testNonLeafUnionAllAggNotPushed() {
        ReferenceAttribute unionField = new ReferenceAttribute(EMPTY, "emp_no", INTEGER);
        FieldAttribute esEmpNo = getFieldAttribute("emp_no", INTEGER);
        EsRelation esRelation = relation().withAttributes(List.of(esEmpNo));

        // Wrap EsRelation in a Project to make this a non-leaf branch
        Project project = new Project(EMPTY, esRelation, List.of(esEmpNo));

        UnionAll unionAll = new UnionAll(EMPTY, List.of(project, project), List.of(unionField));

        Alias countAlias = new Alias(EMPTY, "c", new Count(EMPTY, Literal.TRUE));
        Aggregate aggregate = new Aggregate(EMPTY, unionAll, List.of(), List.of(countAlias));

        LogicalPlan result = new PushAggregateThroughUnionAll().apply(aggregate);

        // Rule must not fire — subquery-shape UnionAll is not eligible
        assertSame(aggregate, result);
    }

    /**
     * {@link PushAggregateThroughUnionAll} must not push an aggregate whose {@code BY} clause
     * contains an expression (rather than a plain attribute reference, e.g. {@code BY BUCKET(...)}).
     * Decomposing across branches requires per-branch attribute resolution; arbitrary expressions
     * cannot be resolved this way.
     */
    public void testExpressionGroupingNotPushed() {
        ReferenceAttribute unionEmpNo = new ReferenceAttribute(EMPTY, "emp_no", INTEGER);
        EsRelation esRelation = relation().withAttributes(List.of(getFieldAttribute("emp_no", INTEGER)));
        ExternalRelation extRelation = externalRelation(List.of(extAttr("emp_no", INTEGER)));
        UnionAll unionAll = new UnionAll(EMPTY, List.of(esRelation, extRelation), List.of(unionEmpNo));

        // BY <literal> — any non-Attribute expression blocks the rewrite
        Alias countAlias = new Alias(EMPTY, "c", new Count(EMPTY, Literal.TRUE));
        Aggregate aggregate = new Aggregate(EMPTY, unionAll, List.of(new Literal(EMPTY, 1, INTEGER)), List.of(countAlias));

        assertSame(aggregate, new PushAggregateThroughUnionAll().apply(aggregate));
    }

    /**
     * {@link PushAggregateThroughUnionAll} must not push a non-decomposable aggregate such as
     * {@link Median} — there is no per-branch partial that can be merged.
     */
    public void testNonDecomposableAggNotPushed() {
        ReferenceAttribute unionField = new ReferenceAttribute(EMPTY, "salary", INTEGER);
        EsRelation esRelation = relation().withAttributes(List.of(getFieldAttribute("salary", INTEGER)));
        ExternalRelation extRelation = externalRelation(List.of(extAttr("salary", INTEGER)));
        UnionAll unionAll = new UnionAll(EMPTY, List.of(esRelation, extRelation), List.of(unionField));

        Alias medianAlias = new Alias(EMPTY, "m", new Median(EMPTY, unionField));
        Aggregate aggregate = new Aggregate(EMPTY, unionAll, List.of(), List.of(medianAlias));

        assertSame(aggregate, new PushAggregateThroughUnionAll().apply(aggregate));
    }

    /**
     * {@link PushAggregateThroughUnionAll} must not push a {@code STATS} that mixes a decomposable
     * aggregate ({@code COUNT}) with a non-decomposable one ({@code MEDIAN}) — any non-decomposable
     * function in the list blocks the rewrite.
     */
    public void testMixedDecomposableAndNonDecomposableNotPushed() {
        ReferenceAttribute unionField = new ReferenceAttribute(EMPTY, "salary", INTEGER);
        EsRelation esRelation = relation().withAttributes(List.of(getFieldAttribute("salary", INTEGER)));
        ExternalRelation extRelation = externalRelation(List.of(extAttr("salary", INTEGER)));
        UnionAll unionAll = new UnionAll(EMPTY, List.of(esRelation, extRelation), List.of(unionField));

        Alias countAlias = new Alias(EMPTY, "c", new Count(EMPTY, Literal.TRUE));
        Alias medianAlias = new Alias(EMPTY, "m", new Median(EMPTY, unionField));
        Aggregate aggregate = new Aggregate(EMPTY, unionAll, List.of(), List.of(countAlias, medianAlias));

        assertSame(aggregate, new PushAggregateThroughUnionAll().apply(aggregate));
    }

    /**
     * {@link PushAggregateThroughUnionAll} must bail out when a grouping column is present in the
     * {@link UnionAll} output but absent from one of the branches (column mismatch across sources).
     */
    public void testGroupingAbsentFromBranchNotPushed() {
        ReferenceAttribute unionDept = new ReferenceAttribute(EMPTY, "dept", INTEGER);

        // branch 1 has dept; branch 2 has only emp_no — dept is missing
        EsRelation esRelation = relation().withAttributes(List.of(getFieldAttribute("dept", INTEGER)));
        ExternalRelation extRelation = externalRelation(List.of(extAttr("emp_no", INTEGER)));
        UnionAll unionAll = new UnionAll(EMPTY, List.of(esRelation, extRelation), List.of(unionDept));

        Alias countAlias = new Alias(EMPTY, "c", new Count(EMPTY, Literal.TRUE));
        Aggregate aggregate = new Aggregate(EMPTY, unionAll, List.of(unionDept), List.of(countAlias, unionDept));

        assertSame(aggregate, new PushAggregateThroughUnionAll().apply(aggregate));
    }

    /**
     * {@link PushAggregateThroughUnionAll} must carry a grouping column through the {@link UnionAll}
     * even when that column has been pruned from the {@link Aggregate}'s output list. This is the
     * shape {@link PruneColumns} produces for {@code FROM idx, ds | STATS c = COUNT(*) BY dept | KEEP c}:
     * {@code dept} is dropped from {@code aggregates()} (unused after KEEP) but kept in
     * {@code groupings()}.
     *
     * <p>Asserts plan validity via the engine's own {@link PlanConsistencyChecker}: every node's
     * attribute references must be produced by its children.
     */
    public void testGroupingPrunedFromOutputProducesConsistentPlan() {
        ReferenceAttribute unionDept = new ReferenceAttribute(EMPTY, "dept", INTEGER);

        EsRelation esRelation = relation().withAttributes(List.of(getFieldAttribute("dept", INTEGER)));
        ExternalRelation extRelation = externalRelation(List.of(extAttr("dept", INTEGER)));
        UnionAll unionAll = new UnionAll(EMPTY, List.of(esRelation, extRelation), List.of(unionDept));

        // Post-PruneColumns shape: dept is in groupings() but NOT in aggregates()
        Alias countAlias = new Alias(EMPTY, "c", new Count(EMPTY, Literal.TRUE));
        Aggregate aggregate = new Aggregate(EMPTY, unionAll, List.of(unionDept), List.of(countAlias));

        LogicalPlan result = new PushAggregateThroughUnionAll().apply(aggregate);

        assertNotSame(aggregate, result);
        assertValidPlan(result);
    }

    /**
     * {@link PushAggregateThroughUnionAll} must push {@code COUNT(emp_no)} (field-level count) into
     * both branches, resolving the field reference to the branch-local attribute in each branch.
     */
    public void testCountFieldPushedThroughLeafUnionAll() {
        ReferenceAttribute unionEmpNo = new ReferenceAttribute(EMPTY, "emp_no", INTEGER);

        FieldAttribute esEmpNo = getFieldAttribute("emp_no", INTEGER);
        EsRelation esRelation = relation().withAttributes(List.of(esEmpNo));

        Attribute extEmpNo = extAttr("emp_no", INTEGER);
        ExternalRelation extRelation = externalRelation(List.of(extEmpNo));

        UnionAll unionAll = new UnionAll(EMPTY, List.of(esRelation, extRelation), List.of(unionEmpNo));

        Alias countAlias = new Alias(EMPTY, "c", new Count(EMPTY, unionEmpNo));
        Aggregate aggregate = new Aggregate(EMPTY, unionAll, List.of(), List.of(countAlias));

        LogicalPlan result = new PushAggregateThroughUnionAll().apply(aggregate);

        Aggregate outerAgg = as(result, Aggregate.class);
        assertThat(outerAgg.aggregates(), hasSize(1));
        Alias outerAlias = as(outerAgg.aggregates().get(0), Alias.class);
        assertThat(outerAlias.child(), instanceOf(Sum.class));

        UnionAll newUnionAll = as(outerAgg.child(), UnionAll.class);
        assertThat(newUnionAll.children(), hasSize(2));

        // Branch 1: COUNT(emp_no{f}) — resolved to EsRelation's FieldAttribute
        Aggregate branch1 = as(newUnionAll.children().get(0), Aggregate.class);
        Alias b1Alias = as(branch1.aggregates().get(0), Alias.class);
        Count b1Count = as(b1Alias.child(), Count.class);
        assertThat(b1Count.field(), equalTo(esEmpNo));

        // Branch 2: COUNT(emp_no{f}) — resolved to ExternalRelation's attribute
        Aggregate branch2 = as(newUnionAll.children().get(1), Aggregate.class);
        Alias b2Alias = as(branch2.aggregates().get(0), Alias.class);
        Count b2Count = as(b2Alias.child(), Count.class);
        assertThat(b2Count.field(), equalTo(extEmpNo));
    }

    /**
     * {@link PushAggregateThroughUnionAll} must push {@code SUM(salary)} into both branches,
     * with the outer combiner also being a {@code SUM} over the per-branch sums.
     */
    public void testSumPushedThroughLeafUnionAll() {
        ReferenceAttribute unionSalary = new ReferenceAttribute(EMPTY, "salary", INTEGER);

        FieldAttribute esSalary = getFieldAttribute("salary", INTEGER);
        EsRelation esRelation = relation().withAttributes(List.of(esSalary));

        Attribute extSalary = extAttr("salary", INTEGER);
        ExternalRelation extRelation = externalRelation(List.of(extSalary));

        UnionAll unionAll = new UnionAll(EMPTY, List.of(esRelation, extRelation), List.of(unionSalary));

        Alias sumAlias = new Alias(EMPTY, "s", new Sum(EMPTY, unionSalary));
        Aggregate aggregate = new Aggregate(EMPTY, unionAll, List.of(), List.of(sumAlias));

        LogicalPlan result = new PushAggregateThroughUnionAll().apply(aggregate);

        Aggregate outerAgg = as(result, Aggregate.class);
        Alias outerAlias = as(outerAgg.aggregates().get(0), Alias.class);
        assertThat(outerAlias.name(), equalTo("s"));
        assertThat(outerAlias.id(), equalTo(sumAlias.id()));
        Sum combinerSum = as(outerAlias.child(), Sum.class);
        assertThat(combinerSum.field(), instanceOf(ReferenceAttribute.class));
        assertThat(((ReferenceAttribute) combinerSum.field()).name(), equalTo("$$partial$$s"));

        UnionAll newUnionAll = as(outerAgg.child(), UnionAll.class);
        assertThat(newUnionAll.children(), hasSize(2));

        Aggregate branch1 = as(newUnionAll.children().get(0), Aggregate.class);
        Sum b1Sum = as(as(branch1.aggregates().get(0), Alias.class).child(), Sum.class);
        assertThat(b1Sum.field(), equalTo(esSalary));

        Aggregate branch2 = as(newUnionAll.children().get(1), Aggregate.class);
        Sum b2Sum = as(as(branch2.aggregates().get(0), Alias.class).child(), Sum.class);
        assertThat(b2Sum.field(), equalTo(extSalary));

        assertValidPlan(result);
    }

    /**
     * {@link PushAggregateThroughUnionAll} must push {@code MIN(salary)} into both branches,
     * with the outer combiner also being a {@code MIN} over the per-branch minima.
     */
    public void testMinPushedThroughLeafUnionAll() {
        ReferenceAttribute unionSalary = new ReferenceAttribute(EMPTY, "salary", INTEGER);

        FieldAttribute esSalary = getFieldAttribute("salary", INTEGER);
        EsRelation esRelation = relation().withAttributes(List.of(esSalary));

        Attribute extSalary = extAttr("salary", INTEGER);
        ExternalRelation extRelation = externalRelation(List.of(extSalary));

        UnionAll unionAll = new UnionAll(EMPTY, List.of(esRelation, extRelation), List.of(unionSalary));

        Alias minAlias = new Alias(EMPTY, "m", new Min(EMPTY, unionSalary));
        Aggregate aggregate = new Aggregate(EMPTY, unionAll, List.of(), List.of(minAlias));

        LogicalPlan result = new PushAggregateThroughUnionAll().apply(aggregate);

        Aggregate outerAgg = as(result, Aggregate.class);
        Alias outerAlias = as(outerAgg.aggregates().get(0), Alias.class);
        assertThat(outerAlias.name(), equalTo("m"));
        assertThat(outerAlias.id(), equalTo(minAlias.id()));
        assertThat(outerAlias.child(), instanceOf(Min.class));

        UnionAll newUnionAll = as(outerAgg.child(), UnionAll.class);
        Aggregate branch1 = as(newUnionAll.children().get(0), Aggregate.class);
        assertThat(as(branch1.aggregates().get(0), Alias.class).child(), instanceOf(Min.class));

        assertValidPlan(result);
    }

    /**
     * {@link PushAggregateThroughUnionAll} must push a filtered aggregate
     * ({@code COUNT(emp_no) WHERE salary > 0}) into both branches, resolving both the field
     * reference and the filter predicate to the branch-local attributes.
     */
    public void testAggWithFilterPushedThroughLeafUnionAll() {
        ReferenceAttribute unionEmpNo = new ReferenceAttribute(EMPTY, "emp_no", INTEGER);
        ReferenceAttribute unionSalary = new ReferenceAttribute(EMPTY, "salary", INTEGER);

        FieldAttribute esEmpNo = getFieldAttribute("emp_no", INTEGER);
        FieldAttribute esSalary = getFieldAttribute("salary", INTEGER);
        EsRelation esRelation = relation().withAttributes(List.of(esEmpNo, esSalary));

        Attribute extEmpNo = extAttr("emp_no", INTEGER);
        Attribute extSalary = extAttr("salary", INTEGER);
        ExternalRelation extRelation = externalRelation(List.of(extEmpNo, extSalary));

        UnionAll unionAll = new UnionAll(EMPTY, List.of(esRelation, extRelation), List.of(unionEmpNo, unionSalary));

        GreaterThan salaryFilter = new GreaterThan(EMPTY, unionSalary, new Literal(EMPTY, 0, INTEGER), null);
        Count countWithFilterAndCondition = (Count) new Count(EMPTY, unionEmpNo).withFilter(salaryFilter);
        Alias countAlias = new Alias(EMPTY, "c", countWithFilterAndCondition);
        Aggregate aggregate = new Aggregate(EMPTY, unionAll, List.of(), List.of(countAlias));

        LogicalPlan result = new PushAggregateThroughUnionAll().apply(aggregate);

        assertNotSame(aggregate, result);
        assertValidPlan(result);

        UnionAll newUnionAll = as(as(result, Aggregate.class).child(), UnionAll.class);

        // Branch 1: filter resolved to EsRelation's FieldAttribute
        Aggregate branch1 = as(newUnionAll.children().get(0), Aggregate.class);
        Count b1Count = as(as(branch1.aggregates().get(0), Alias.class).child(), Count.class);
        assertThat(b1Count.filter(), instanceOf(GreaterThan.class));
        GreaterThan b1Filter = (GreaterThan) b1Count.filter();
        assertThat(b1Filter.left(), equalTo(esSalary));

        // Branch 2: filter resolved to ExternalRelation's attribute
        Aggregate branch2 = as(newUnionAll.children().get(1), Aggregate.class);
        Count b2Count = as(as(branch2.aggregates().get(0), Alias.class).child(), Count.class);
        assertThat(b2Count.filter(), instanceOf(GreaterThan.class));
        GreaterThan b2Filter = (GreaterThan) b2Count.filter();
        assertThat(b2Filter.left(), equalTo(extSalary));
    }

    /**
     * {@link PushAggregateThroughUnionAll} must push an aggregate through a leaf {@link UnionAll}
     * with three branches (one {@link EsRelation} and two {@link ExternalRelation}s), producing
     * three per-branch partial aggregates with consistent shared IDs.
     */
    public void testThreeBranchesPushedThroughLeafUnionAll() {
        ReferenceAttribute unionEmpNo = new ReferenceAttribute(EMPTY, "emp_no", INTEGER);

        FieldAttribute esEmpNo = getFieldAttribute("emp_no", INTEGER);
        EsRelation esRelation = relation().withAttributes(List.of(esEmpNo));

        Attribute extEmpNo1 = extAttr("emp_no", INTEGER);
        ExternalRelation extRelation1 = externalRelation(List.of(extEmpNo1));

        Attribute extEmpNo2 = extAttr("emp_no", INTEGER);
        ExternalRelation extRelation2 = externalRelation(List.of(extEmpNo2));

        UnionAll unionAll = new UnionAll(EMPTY, List.of(esRelation, extRelation1, extRelation2), List.of(unionEmpNo));

        Alias countAlias = new Alias(EMPTY, "c", new Count(EMPTY, Literal.TRUE));
        Aggregate aggregate = new Aggregate(EMPTY, unionAll, List.of(), List.of(countAlias));

        LogicalPlan result = new PushAggregateThroughUnionAll().apply(aggregate);

        assertNotSame(aggregate, result);
        assertValidPlan(result);

        Aggregate outerAgg = as(result, Aggregate.class);
        UnionAll newUnionAll = as(outerAgg.child(), UnionAll.class);
        assertThat(newUnionAll.children(), hasSize(3));

        as(as(newUnionAll.children().get(0), Aggregate.class).child(), EsRelation.class);
        as(as(newUnionAll.children().get(1), Aggregate.class).child(), ExternalRelation.class);
        as(as(newUnionAll.children().get(2), Aggregate.class).child(), ExternalRelation.class);

        // All three branches must share the same partial ID
        assertThat(newUnionAll.children().get(0).output().get(0).id(), equalTo(newUnionAll.children().get(1).output().get(0).id()));
        assertThat(newUnionAll.children().get(1).output().get(0).id(), equalTo(newUnionAll.children().get(2).output().get(0).id()));
    }

    /**
     * {@link PushAggregateThroughUnionAll} must push an aggregate with two grouping keys
     * ({@code BY dept, salary}) into both branches, with the outer combiner grouping by the
     * same shared grouping IDs.
     */
    public void testMultipleGroupingKeysPushedThroughLeafUnionAll() {
        ReferenceAttribute unionDept = new ReferenceAttribute(EMPTY, "dept", INTEGER);
        ReferenceAttribute unionSalary = new ReferenceAttribute(EMPTY, "salary", INTEGER);

        FieldAttribute esDept = getFieldAttribute("dept", INTEGER);
        FieldAttribute esSalary = getFieldAttribute("salary", INTEGER);
        EsRelation esRelation = relation().withAttributes(List.of(esDept, esSalary));

        Attribute extDept = extAttr("dept", INTEGER);
        Attribute extSalary = extAttr("salary", INTEGER);
        ExternalRelation extRelation = externalRelation(List.of(extDept, extSalary));

        UnionAll unionAll = new UnionAll(EMPTY, List.of(esRelation, extRelation), List.of(unionDept, unionSalary));

        Alias countAlias = new Alias(EMPTY, "c", new Count(EMPTY, Literal.TRUE));
        Aggregate aggregate = new Aggregate(EMPTY, unionAll, List.of(unionDept, unionSalary), List.of(countAlias, unionDept, unionSalary));

        LogicalPlan result = new PushAggregateThroughUnionAll().apply(aggregate);

        assertNotSame(aggregate, result);
        assertValidPlan(result);

        Aggregate outerAgg = as(result, Aggregate.class);
        assertThat(outerAgg.groupings(), hasSize(2));
        assertThat(outerAgg.aggregates(), hasSize(3)); // c, dept, salary

        UnionAll newUnionAll = as(outerAgg.child(), UnionAll.class);
        assertThat(newUnionAll.output(), hasSize(3)); // $$partial$$c, dept{shared}, salary{shared}

        Aggregate branch1 = as(newUnionAll.children().get(0), Aggregate.class);
        assertThat(branch1.groupings(), hasSize(2));
        assertThat(branch1.aggregates(), hasSize(3)); // $$partial$$c, dept, salary
    }

    /**
     * Runs the engine's own {@link PlanConsistencyChecker} over every node in {@code plan}.
     * Every attribute reference in the plan must be produced by its children — the same invariant
     * {@code LogicalVerifier} enforces after optimization.
     */
    private static void assertValidPlan(LogicalPlan plan) {
        Failures failures = new Failures();
        plan.forEachUp(p -> PlanConsistencyChecker.checkPlan(p, failures));
        assertThat(failures.toString(), failures.hasFailures(), equalTo(false));
    }

    private static Attribute extAttr(String name, DataType type) {
        return new FieldAttribute(EMPTY, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static ExternalRelation externalRelation(List<Attribute> attributes) {
        SourceMetadata metadata = new SourceMetadata() {
            @Override
            public List<Attribute> schema() {
                return attributes;
            }

            @Override
            public String sourceType() {
                return "parquet";
            }

            @Override
            public String location() {
                return "s3://bucket/data.parquet";
            }

            @Override
            public boolean equals(Object o) {
                return o instanceof SourceMetadata;
            }

            @Override
            public int hashCode() {
                return 1;
            }
        };
        return new ExternalRelation(EMPTY, "s3://bucket/data.parquet", metadata, attributes, FileList.UNRESOLVED, Map.of());
    }
}
