/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

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
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizerTests.relation;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;

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
