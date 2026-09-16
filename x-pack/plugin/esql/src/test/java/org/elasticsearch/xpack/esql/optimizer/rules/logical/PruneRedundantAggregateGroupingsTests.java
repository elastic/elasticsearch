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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

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

    public void testPrunesDerivedExternalGroupings() {
        var plan = externalPlan("""
            FROM ext_ds
            | EVAL ip_m1 = ClientIP - 1, ip_m2 = ClientIP - 2, ip_m3 = ClientIP - 3
            | STATS c = COUNT(*) BY ClientIP, ip_m1, ip_m2, ip_m3
            """);

        var project = rewrittenProject(plan);
        assertThat(Expressions.names(project.projections()), contains("c", "ClientIP", "ip_m1", "ip_m2", "ip_m3"));

        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("ip_m1", "ip_m2", "ip_m3"));

        var aggregate = rewrittenAggregate(eval);
        assertThat(Expressions.names(aggregate.groupings()), contains("ClientIP"));
        assertThat(Expressions.names(aggregate.aggregates()), contains("c", "ClientIP"));
        as(aggregate.child(), ExternalRelation.class);
        assertThat(
            eval.fields().get(0).child(),
            instanceOf(org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub.class)
        );
    }

    public void testPrunesRecursiveDerivedExternalGrouping() {
        var plan = externalPlan("""
            FROM ext_ds
            | EVAL ip_m1 = ClientIP - 1, ip_m2 = ip_m1 - 1
            | STATS c = COUNT(*) BY ClientIP, ip_m2
            """);

        var project = rewrittenProject(plan);
        assertThat(Expressions.names(project.projections()), contains("c", "ClientIP", "ip_m2"));

        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("ip_m2"));

        var aggregate = rewrittenAggregate(eval);
        assertThat(Expressions.names(aggregate.groupings()), contains("ClientIP"));
        assertThat(Expressions.names(aggregate.aggregates()), contains("c", "ClientIP"));
        as(aggregate.child(), ExternalRelation.class);
        assertThat(
            eval.fields().get(0).child(),
            instanceOf(org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub.class)
        );
    }

    public void testPartialDerivedExternalPruningKeepsNeededPreAggregateEval() {
        var plan = externalPlan("""
            FROM ext_ds
            | EVAL ip_m1 = ClientIP - 1, other_m1 = OtherIP - 1
            | STATS c = COUNT(*) BY ClientIP, ip_m1, other_m1
            """);

        var project = rewrittenProject(plan);
        assertThat(Expressions.names(project.projections()), contains("c", "ClientIP", "ip_m1", "other_m1"));

        var postAggregateEval = as(project.child(), Eval.class);
        assertThat(Expressions.names(postAggregateEval.fields()), contains("ip_m1"));

        var aggregate = rewrittenAggregate(postAggregateEval);
        assertThat(Expressions.names(aggregate.groupings()), contains("ClientIP", "other_m1"));
        assertThat(Expressions.names(aggregate.aggregates()), contains("c", "ClientIP", "other_m1"));

        var preAggregateEval = as(aggregate.child(), Eval.class);
        assertThat(Expressions.names(preAggregateEval.fields()), contains("other_m1"));
        as(preAggregateEval.child(), ExternalRelation.class);
    }

    /**
     * An external grouping column is renamed, a value is derived from the renamed column, then both the
     * renamed column and the derived value are used as STATS BY keys. The derived key is functionally dependent on the
     * renamed column, so it is pruned and rebuilt above the aggregate. The rebuilt expression must reference the column
     * as the aggregate re-exposes it (i.e. the rename alias {@code cip}), not the pre-aggregate external id which the
     * aggregate no longer surfaces. Otherwise the rebuilt Eval dangles and the plan fails the post-optimization
     * consistency check. The same query over a native index is unaffected because the rule only prunes external
     * groupings (see {@link #testDoesNotPruneDerivedOrdinaryIndexGrouping}).
     */
    public void testPrunesRenamedDerivedExternalGrouping() {
        var plan = externalPlan("""
            FROM ext_ds
            | RENAME ClientIP AS cip
            | EVAL c = cip - 1
            | STATS count = COUNT(*) BY cip, c
            """);

        var project = rewrittenProject(plan);
        assertThat(Expressions.names(project.projections()), contains("count", "cip", "c"));

        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("c"));
        // the rebuilt grouping must read the aggregate's renamed output, not the pre-aggregate external attribute
        assertThat(Expressions.names(eval.fields().get(0).child().references()), contains("cip"));

        var aggregate = rewrittenAggregate(eval);
        assertThat(Expressions.names(aggregate.groupings()), contains("ClientIP"));
        assertThat(Expressions.names(aggregate.aggregates()), contains("count", "cip"));
        as(aggregate.child(), ExternalRelation.class);
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

    public void testDoesNotPruneIndependentExternalExpression() {
        var plan = externalPlan("""
            FROM ext_ds
            | EVAL other_m1 = OtherIP - 1
            | STATS c = COUNT(*) BY ClientIP, other_m1
            """);

        var aggregate = as(as(plan, Limit.class).child(), Aggregate.class);
        assertThat(Expressions.names(aggregate.groupings()), contains("ClientIP", "other_m1"));
    }

    public void testDoesNotPruneNonWhitelistedExternalExpression() {
        var plan = externalPlan("""
            FROM ext_ds
            | EVAL ip_mul = ClientIP * 2
            | STATS c = COUNT(*) BY ClientIP, ip_mul
            """);

        var aggregate = as(as(plan, Limit.class).child(), Aggregate.class);
        assertThat(Expressions.names(aggregate.groupings()), contains("ClientIP", "ip_mul"));
    }

    /**
     * Thousands of chained EVAL aliases grouped on, over an ordinary index, each alias referencing the two before it so the
     * unbounded expansion of https://github.com/elastic/elasticsearch/issues/150104 grows exponentially and exhausts the
     * heap. Over an ordinary index nothing is prunable, so the expansion is now skipped before any cap applies; the
     * depth-only shape of {@code HeapAttackIT.testGroupOnManyLongs} only fails on a node's 1 MB stack and is covered there.
     */
    public void testLongAliasChainOverOrdinaryIndexDoesNotOverflow() {
        int count = 5000;
        StringBuilder query = new StringBuilder("FROM test\n| EVAL i0 = emp_no + salary, i1 = salary + i0");
        for (int i = 2; i < count; i++) {
            query.append(", i").append(i).append(" = i").append(i - 2).append(" + i").append(i - 1);
        }
        query.append("\n| STATS c = COUNT(*) BY emp_no, salary, i0");
        for (int i = 1; i < count; i++) {
            query.append(", i").append(i);
        }
        var plan = plan(query.toString());

        var aggregate = as(as(plan, Limit.class).child(), Aggregate.class);
        assertThat(aggregate.groupings(), hasSize(count + 2));
    }

    /** A chain of depth {@code d} takes {@code d - 1} substitutions to reach the external attribute. */
    public void testPrunesDerivedExternalGroupingWithinAliasBudget() {
        int depth = PruneRedundantAggregateGroupings.MAX_ALIAS_SUBSTITUTIONS + 1;
        var plan = externalPlan(chainedExternalQuery(depth));

        var project = rewrittenProject(plan);
        assertThat(Expressions.names(project.projections()), contains("c", "ClientIP", "ip_" + depth));
        var aggregate = rewrittenAggregate(as(project.child(), Eval.class));
        assertThat(Expressions.names(aggregate.groupings()), contains("ClientIP"));
    }

    public void testDoesNotPruneDerivedExternalGroupingBeyondAliasBudget() {
        int depth = PruneRedundantAggregateGroupings.MAX_ALIAS_SUBSTITUTIONS + 2;
        var plan = externalPlan(chainedExternalQuery(depth));

        var aggregate = as(as(plan, Limit.class).child(), Aggregate.class);
        assertThat(Expressions.names(aggregate.groupings()), contains("ClientIP", "ip_" + depth));
    }

    /** A pruned alias stays in the child Eval while a kept grouping still reaches it through the chain. */
    public void testKeepsPrunedAliasStillReachableFromKeptGrouping() {
        int depth = PruneRedundantAggregateGroupings.MAX_ALIAS_SUBSTITUTIONS + 50;
        var plan = externalPlan(chainedExternalQuery(depth, "ip_50"));

        var project = rewrittenProject(plan);
        assertThat(Expressions.names(project.projections()), contains("c", "ClientIP", "ip_50", "ip_" + depth));
        var aggregate = rewrittenAggregate(as(project.child(), Eval.class));
        assertThat(Expressions.names(aggregate.groupings()), contains("ClientIP", "ip_" + depth));
        var childEval = as(aggregate.child(), Eval.class);
        assertThat(childEval.fields(), hasSize(depth));
    }

    /** {@code b} is not prunable and reads {@code a}, so pruning {@code a} must not drop its definition. */
    public void testKeepsPrunedAliasReadByUnprunableGrouping() {
        var plan = externalPlan("""
            FROM ext_ds
            | EVAL a = ClientIP - 1, b = a * 2
            | STATS c = COUNT(*) BY ClientIP, a, b
            """);

        var project = rewrittenProject(plan);
        assertThat(Expressions.names(project.projections()), contains("c", "ClientIP", "a", "b"));
        var aggregate = rewrittenAggregate(as(project.child(), Eval.class));
        assertThat(Expressions.names(aggregate.groupings()), contains("ClientIP", "b"));
        assertThat(Expressions.names(as(aggregate.child(), Eval.class).fields()), contains("a", "b"));
    }

    /**
     * {@code d} is dead but reads the pruned {@code a}; the rule alone must leave a valid plan rather than rely on
     * {@code PruneColumns} removing {@code d} later in the batch.
     */
    public void testKeepsPrunedAliasReadByUnusedField() {
        var analyzed = analyzedExternalPlan("""
            FROM ext_ds
            | EVAL a = ClientIP - 1, d = a + 1
            | STATS c = COUNT(*) BY ClientIP, a
            """);

        var plan = new PruneRedundantAggregateGroupings().apply(analyzed);

        var project = as(as(plan, Limit.class).child(), Project.class);
        var aggregate = as(as(project.child(), Eval.class).child(), Aggregate.class);
        assertThat(Expressions.names(aggregate.groupings()), contains("ClientIP"));
        assertThat(Expressions.names(as(aggregate.child(), Eval.class).fields()), contains("a", "d"));
    }

    private static String chainedExternalQuery(int depth) {
        return chainedExternalQuery(depth, null);
    }

    /** {@code ip_1 = ClientIP - 1, ip_2 = ip_1 - 1, ...} grouped by {@code ClientIP}, an optional extra alias, and the last one. */
    private static String chainedExternalQuery(int depth, String extraGrouping) {
        StringBuilder query = new StringBuilder("FROM ext_ds\n| EVAL ip_1 = ClientIP - 1");
        for (int i = 2; i <= depth; i++) {
            query.append(", ip_").append(i).append(" = ip_").append(i - 1).append(" - 1");
        }
        query.append("\n| STATS c = COUNT(*) BY ClientIP, ");
        if (extraGrouping != null) {
            query.append(extraGrouping).append(", ");
        }
        return query.append("ip_").append(depth).toString();
    }

    private LogicalPlan externalPlan(String query) {
        return datasetPlan(query, DATASET_NAME, S3_RESOURCE, externalSchema());
    }

    private LogicalPlan analyzedExternalPlan(String query) {
        return analyzedDatasetPlan(query, DATASET_NAME, S3_RESOURCE, externalSchema());
    }

    private static List<Attribute> externalSchema() {
        return List.of(referenceAttribute("ClientIP", INTEGER), referenceAttribute("OtherIP", INTEGER), referenceAttribute("URL", KEYWORD));
    }

    private static Project rewrittenProject(LogicalPlan plan) {
        return as(plan, Project.class);
    }

    private static Aggregate rewrittenAggregate(Eval eval) {
        return as(as(eval.child(), Limit.class).child(), Aggregate.class);
    }
}
