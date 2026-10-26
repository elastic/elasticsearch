/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.parser.ExpressionBuilder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;

public class PruneRedundantAggregateGroupingsTests extends AbstractLogicalPlanOptimizerTests {

    private static final String DATASET_NAME = "ext_ds";
    private static final String S3_RESOURCE = "s3://bucket/data.parquet";

    /** Twice the chain {@code HeapAttackIT#testGroupOnManyLongs} groups on. */
    private static final int LONG_CHAIN_FIELDS = 10_000;

    /**
     * Aliases in the deep-definition tests, each defined about as deep as a query may spell out. Expanding the last one without
     * a bound recurses around fifty thousand levels, which needs many times {@link #SMALL_STACK_BYTES} however the JIT has
     * compiled the recursion, so on HotSpot, which honors a thread's requested stack size, those tests fail reliably on an
     * unbounded expansion.
     */
    private static final int DEEP_ALIASES = 200;

    /** Comfortably more than the rule needs for any test here, even when interpreted. */
    private static final long SMALL_STACK_BYTES = 512 * 1024;

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

    /** The shape of {@code HeapAttackIT#testGroupOnManyLongs}: no grouping is a constant, so every grouping stays. */
    public void testLongAliasChainOverIndexKeepsEveryGrouping() {
        LogicalPlan analyzed = defaultAnalyzer().query(aliasChainQuery("test", "emp_no", "salary", LONG_CHAIN_FIELDS));

        LogicalPlan result = applyOnSmallStack(analyzed);

        assertThat(groupingNames(result), hasSize(LONG_CHAIN_FIELDS + 2));
    }

    /** Each alias reads the two before it, so expanding every reference separately would grow exponentially with the chain. */
    public void testSharedAliasReferencesKeepEveryGrouping() {
        int fields = 5_000;
        StringBuilder query = new StringBuilder("FROM ext_ds\n| EVAL i0 = ClientIP + OtherIP, i1 = OtherIP + i0");
        for (int i = 2; i < fields; i++) {
            query.append(", i").append(i).append(" = i").append(i - 2).append(" + i").append(i - 1);
        }
        appendGroupings(query, "ClientIP", "OtherIP", fields);

        LogicalPlan result = applyOnSmallStack(analyzedExternalPlan(query.toString()));

        assertThat(groupingNames(result), hasSize(fields + 2));
    }

    /** Over an index, like {@code HeapAttackIT#testGroupOnManyLongs}, but with the depth in the definitions, not the alias count. */
    public void testDeepAliasDefinitionsOverIndexKeepEveryGrouping() {
        LogicalPlan analyzed = defaultAnalyzer().query(deepDefinitionsQuery("test", "emp_no", "salary"));

        LogicalPlan result = applyOnSmallStack(analyzed);

        assertThat(groupingNames(result), contains("emp_no", "salary", "j" + (DEEP_ALIASES - 1)));
    }

    public void testDeepAliasDefinitionsOverExternalSourceKeepEveryGrouping() {
        LogicalPlan analyzed = analyzedExternalPlan(deepDefinitionsQuery(DATASET_NAME, "ClientIP", "OtherIP"));

        LogicalPlan result = applyOnSmallStack(analyzed);

        assertThat(groupingNames(result), contains("ClientIP", "OtherIP", "j" + (DEEP_ALIASES - 1)));
    }

    /** {@code b} stays a grouping and reads the pruned {@code a}, so {@code a} must stay defined below the aggregate. */
    public void testKeepsPrunedConstantReadByKeptGrouping() {
        LogicalPlan result = applyRuleOnly("""
            FROM ext_ds
            | EVAL a = 1, b = a * 2
            | STATS c = COUNT(*) BY ClientIP, a, b
            """);

        Aggregate aggregate = singleAggregate(result);
        assertThat(Expressions.names(aggregate.groupings()), contains("ClientIP", "b"));
        assertThat(Expressions.names(as(aggregate.child(), Eval.class).fields()), contains("a", "b"));
    }

    /** Like {@link #testKeepsPrunedConstantReadByKeptGrouping}, but {@code b} reads the pruned {@code a} as an aggregate input. */
    public void testKeepsPrunedConstantReadByAggregateInput() {
        LogicalPlan result = applyRuleOnly("""
            FROM ext_ds
            | EVAL a = 1, b = a * 2
            | STATS s = SUM(b) BY ClientIP, a
            """);

        Aggregate aggregate = singleAggregate(result);
        assertThat(Expressions.names(aggregate.groupings()), contains("ClientIP"));
        assertThat(Expressions.names(as(aggregate.child(), Eval.class).fields()), contains("a", "b"));
    }

    /**
     * {@code d} is unused but reads the pruned {@code a}. Both go: dropping only {@code a} would leave {@code d} dangling, and
     * keeping {@code a} for {@code d} would compute it for every row although nothing needs either.
     */
    public void testDropsUnusedFieldReadingPrunedConstant() {
        LogicalPlan result = applyRuleOnly("""
            FROM ext_ds
            | EVAL a = 1, d = a + 1
            | STATS c = COUNT(*) BY ClientIP, a
            """);

        Aggregate aggregate = singleAggregate(result);
        assertThat(Expressions.names(aggregate.groupings()), contains("ClientIP"));
        as(aggregate.child(), ExternalRelation.class);
    }

    /** Mirrors {@code HeapAttackTestCase#makeManyLongs}: two interleaved chains, each link adding a literal to the link two back. */
    private static String aliasChainQuery(String from, String first, String second, int fields) {
        StringBuilder query = new StringBuilder("FROM ").append(from);
        query.append("\n| EVAL i0 = ").append(first).append(" + ").append(second).append(", i1 = ").append(second).append(" + i0");
        for (int i = 2; i < fields; i++) {
            query.append(", i").append(i).append(" = i").append(i - 2).append(" + ").append(i - 1);
        }
        appendGroupings(query, first, second, fields);
        return query.toString();
    }

    /** {@link #DEEP_ALIASES} aliases, each adding a long run of literals to the one before it. */
    private static String deepDefinitionsQuery(String from, String first, String second) {
        String definitionTail = " + 1".repeat(ExpressionBuilder.MAX_EXPRESSION_DEPTH - 50);
        StringBuilder query = new StringBuilder("FROM ").append(from).append("\n| EVAL j0 = ").append(first).append(" + ").append(second);
        for (int i = 1; i < DEEP_ALIASES; i++) {
            query.append(", j").append(i).append(" = j").append(i - 1).append(definitionTail);
        }
        return query.append("\n| STATS c = COUNT(*) BY ")
            .append(first)
            .append(", ")
            .append(second)
            .append(", j")
            .append(DEEP_ALIASES - 1)
            .toString();
    }

    private static void appendGroupings(StringBuilder query, String first, String second, int fields) {
        query.append("\n| STATS c = COUNT(*) BY ").append(first).append(", ").append(second);
        for (int i = 0; i < fields; i++) {
            query.append(", i").append(i);
        }
    }

    /** Applies only this rule, on a thread with a {@link #SMALL_STACK_BYTES} stack. */
    private static LogicalPlan applyOnSmallStack(LogicalPlan plan) {
        AtomicReference<LogicalPlan> result = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread thread = new Thread(null, () -> {
            try {
                result.set(new PruneRedundantAggregateGroupings().apply(plan));
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "prune-groupings-small-stack", SMALL_STACK_BYTES);
        // A rule still running at the timeout must not also fail the suite as a leaked thread.
        thread.setDaemon(true);
        thread.start();
        // Longer than safeJoin's timeout, so that a slow interpreted run is not mistaken for a rule that never finishes.
        try {
            thread.join(TimeValue.timeValueMinutes(2).millis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted waiting for the rule", e);
        }
        assertFalse("rule still running", thread.isAlive());
        if (failure.get() != null) {
            throw new AssertionError("rule failed on a " + SMALL_STACK_BYTES + " byte stack", failure.get());
        }
        return result.get();
    }

    /**
     * Applies only this rule to the analyzed plan. In the full optimizer {@link PropagateEvalFoldables} runs first and inlines a
     * constant into every field that reads it, so a field below the aggregate still reads a pruned constant only when this rule
     * runs on its own.
     */
    private LogicalPlan applyRuleOnly(String query) {
        return new PruneRedundantAggregateGroupings().apply(analyzedExternalPlan(query));
    }

    private static Aggregate singleAggregate(LogicalPlan plan) {
        List<Aggregate> aggregates = new ArrayList<>();
        plan.forEachDown(Aggregate.class, aggregates::add);
        assertThat(aggregates, hasSize(1));
        return aggregates.get(0);
    }

    private static List<String> groupingNames(LogicalPlan plan) {
        return Expressions.names(singleAggregate(plan).groupings());
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
