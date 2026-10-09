/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.hamcrest.Matchers.contains;

public class PruneRedundantAggregateGroupingsTests extends AbstractLogicalPlanOptimizerTests {

    private static final String DATASET_NAME = "ext_ds";
    private static final String S3_RESOURCE = "s3://bucket/data.parquet";

    public PruneRedundantAggregateGroupingsTests(VersionMode versionMode) {
        super(versionMode);
    }

    public void testPrunesEvalConstantGrouping() {
        var plan = plan("""
            FROM test
            | EVAL const1 = 1
            | STATS c = COUNT(*) BY const1, last_name
            """);

        var project = rewrittenProject(plan);
        assertThat(Expressions.names(project.projections()), contains("c", "const1", "last_name"));

        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("const1"));

        var aggregate = rewrittenAggregate(eval);
        assertThat(Expressions.names(aggregate.groupings()), contains("last_name"));
        assertThat(Expressions.names(aggregate.aggregates()), contains("c", "last_name"));
        as(aggregate.child(), EsRelation.class);
    }

    public void testPrunesDirectLiteralGrouping() {
        var plan = plan("""
            FROM test
            | STATS c = COUNT(*) BY 1, last_name
            """);

        var project = rewrittenProject(plan);
        assertThat(Expressions.names(project.projections()), contains("c", "1", "last_name"));

        var aggregate = rewrittenAggregate(as(project.child(), Eval.class));
        assertThat(Expressions.names(aggregate.groupings()), contains("last_name"));
        assertThat(Expressions.names(aggregate.aggregates()), contains("c", "last_name"));
        as(aggregate.child(), EsRelation.class);
    }

    public void testDoesNotPruneOnlyEvalConstantGrouping() {
        var plan = plan("""
            FROM test
            | EVAL const1 = 1
            | STATS c = COUNT(*) BY const1
            """);

        var aggregate = as(as(plan, Limit.class).child(), Aggregate.class);
        assertThat(Expressions.names(aggregate.groupings()), contains("const1"));
        assertThat(Expressions.names(aggregate.aggregates()), contains("c", "const1"));
        as(aggregate.child(), Eval.class);
    }

    public void testDoesNotPruneOnlyDirectLiteralGrouping() {
        var plan = plan("""
            FROM test
            | STATS c = COUNT(*) BY 1
            """);

        var aggregate = as(as(plan, Limit.class).child(), Aggregate.class);
        assertThat(Expressions.names(aggregate.groupings()), contains("1"));
        assertThat(Expressions.names(aggregate.aggregates()), contains("c", "1"));
        as(aggregate.child(), Eval.class);
    }

    public void testDoesNotPruneOnlyMultipleConstantGroupings() {
        var plan = plan("""
            FROM test
            | EVAL const1 = 1
            | STATS c = COUNT(*) BY const1, 2
            """);

        var aggregate = as(as(plan, Limit.class).child(), Aggregate.class);
        assertThat(Expressions.names(aggregate.groupings()), contains("const1", "2"));
        assertThat(Expressions.names(aggregate.aggregates()), contains("c", "const1", "2"));
        as(aggregate.child(), Eval.class);
    }

    public void testDoesNotPruneMultivalueConstantGrouping() {
        var plan = plan("""
            FROM test
            | EVAL mv = [1, 2]
            | STATS c = COUNT(*) BY mv, last_name
            """);

        var aggregate = as(as(plan, Limit.class).child(), Aggregate.class);
        assertThat(Expressions.names(aggregate.groupings()), contains("mv", "last_name"));
    }

    /**
     * A key derived from another external key with {@code -} looks functionally dependent on it, but only while the
     * source column is single-valued: on a row holding a list the derived key evaluates to {@code null} and the
     * aggregate unrolls the list into one group per element. The rule therefore keeps derived keys in the aggregate,
     * where they are evaluated on the row, and leaves the pre-aggregate {@code EVAL} in place.
     */
    public void testDoesNotPruneDerivedExternalGroupings() {
        var plan = externalPlan("""
            FROM ext_ds
            | EVAL ip_m1 = ClientIP - 1, ip_m2 = ClientIP - 2, ip_m3 = ClientIP - 3
            | STATS c = COUNT(*) BY ClientIP, ip_m1, ip_m2, ip_m3
            """);

        var aggregate = as(as(plan, Limit.class).child(), Aggregate.class);
        assertThat(Expressions.names(aggregate.groupings()), contains("ClientIP", "ip_m1", "ip_m2", "ip_m3"));
        assertThat(Expressions.names(aggregate.aggregates()), contains("c", "ClientIP", "ip_m1", "ip_m2", "ip_m3"));

        var eval = as(aggregate.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("ip_m1", "ip_m2", "ip_m3"));
        as(eval.child(), ExternalRelation.class);
    }

    public void testDoesNotPruneRecursiveDerivedExternalGrouping() {
        var plan = externalPlan("""
            FROM ext_ds
            | EVAL ip_m1 = ClientIP - 1, ip_m2 = ip_m1 - 1
            | STATS c = COUNT(*) BY ClientIP, ip_m2
            """);

        var aggregate = as(as(plan, Limit.class).child(), Aggregate.class);
        assertThat(Expressions.names(aggregate.groupings()), contains("ClientIP", "ip_m2"));
        assertThat(Expressions.names(aggregate.aggregates()), contains("c", "ClientIP", "ip_m2"));

        var eval = as(aggregate.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("ip_m1", "ip_m2"));
        as(eval.child(), ExternalRelation.class);
    }

    /**
     * A constant key next to a derived key: the constant is pruned and rebuilt above the aggregate, the derived key is
     * kept, and the pre-aggregate {@code EVAL} drops the constant's field while retaining the derived one.
     */
    public void testPrunesConstantButKeepsDerivedExternalGrouping() {
        var plan = externalPlan("""
            FROM ext_ds
            | EVAL const1 = 1, ip_m1 = ClientIP - 1
            | STATS c = COUNT(*) BY ClientIP, const1, ip_m1
            """);

        var project = rewrittenProject(plan);
        assertThat(Expressions.names(project.projections()), contains("c", "ClientIP", "const1", "ip_m1"));

        var postAggregateEval = as(project.child(), Eval.class);
        assertThat(Expressions.names(postAggregateEval.fields()), contains("const1"));

        var aggregate = rewrittenAggregate(postAggregateEval);
        assertThat(Expressions.names(aggregate.groupings()), contains("ClientIP", "ip_m1"));
        assertThat(Expressions.names(aggregate.aggregates()), contains("c", "ClientIP", "ip_m1"));

        var preAggregateEval = as(aggregate.child(), Eval.class);
        assertThat(Expressions.names(preAggregateEval.fields()), contains("ip_m1"));
        as(preAggregateEval.child(), ExternalRelation.class);
    }

    /**
     * An external grouping column is renamed, a value is derived from the renamed column, then both are used as
     * STATS BY keys. The derived key stays in the aggregate (see {@link #testDoesNotPruneDerivedExternalGroupings}),
     * and the aggregate re-exposes the renamed column as {@code ClientIP AS cip}.
     */
    public void testDoesNotPruneRenamedDerivedExternalGrouping() {
        var plan = externalPlan("""
            FROM ext_ds
            | RENAME ClientIP AS cip
            | EVAL c = cip - 1
            | STATS count = COUNT(*) BY cip, c
            """);

        var aggregate = as(as(plan, Limit.class).child(), Aggregate.class);
        assertThat(Expressions.names(aggregate.groupings()), contains("ClientIP", "c"));
        assertThat(Expressions.names(aggregate.aggregates()), contains("count", "cip", "c"));

        var eval = as(aggregate.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("c"));
        as(eval.child(), ExternalRelation.class);
    }

    public void testDoesNotPruneInlineStatsGroupings() {
        assumeTrue("requires INLINE STATS command capability", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        var plan = plan("""
            FROM test
            | EVAL const1 = 1
            | INLINE STATS c = COUNT(*) BY const1, last_name
            """);

        var inlineJoin = as(as(plan, Limit.class).child(), InlineJoin.class);
        var aggregate = as(inlineJoin.right(), Aggregate.class);
        assertThat(Expressions.names(aggregate.groupings()), contains("const1", "last_name"));
        assertThat(Expressions.names(aggregate.aggregates()), contains("c", "const1", "last_name"));
        as(aggregate.child(), StubRelation.class);
    }

    public void testDoesNotPruneInlineStatsLiteralGroupings() {
        assumeTrue("requires INLINE STATS command capability", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        var plan = plan("""
            FROM test
            | INLINE STATS c = COUNT(*) BY 1, last_name
            """);

        var inlineJoin = as(as(plan, Limit.class).child(), InlineJoin.class);
        var aggregate = as(inlineJoin.right(), Aggregate.class);
        assertThat(Expressions.names(aggregate.groupings()), contains("1", "last_name"));
        assertThat(Expressions.names(aggregate.aggregates()), contains("c", "1", "last_name"));
        as(aggregate.child(), StubRelation.class);
    }

    public void testDoesNotPruneDerivedOrdinaryIndexGrouping() {
        var plan = plan("""
            FROM test
            | EVAL emp_m1 = emp_no - 1
            | STATS c = COUNT(*) BY emp_no, emp_m1
            """);

        var aggregate = as(as(plan, Limit.class).child(), Aggregate.class);
        assertThat(Expressions.names(aggregate.groupings()), contains("emp_no", "emp_m1"));
    }

    private LogicalPlan externalPlan(String query) {
        List<Attribute> schema = List.of(
            referenceAttribute("ClientIP", INTEGER),
            referenceAttribute("OtherIP", INTEGER),
            referenceAttribute("URL", KEYWORD)
        );
        return datasetPlan(query, DATASET_NAME, S3_RESOURCE, schema);
    }

    private static Project rewrittenProject(LogicalPlan plan) {
        return as(plan, Project.class);
    }

    private static Aggregate rewrittenAggregate(Eval eval) {
        return as(as(eval.child(), Limit.class).child(), Aggregate.class);
    }
}
