/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

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
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
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
