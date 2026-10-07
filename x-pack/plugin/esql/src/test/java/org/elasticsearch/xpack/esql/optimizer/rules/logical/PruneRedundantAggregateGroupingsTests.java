/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

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

    /** Comfortably more than the bounded rule needs for any test here, even when interpreted. */
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

    /** The shape of {@code HeapAttackIT#testGroupOnManyLongs}: over an index nothing derived is prunable, so every grouping stays. */
    public void testLongAliasChainOverIndexKeepsEveryGrouping() {
        LogicalPlan analyzed = defaultAnalyzer().query(aliasChainQuery("test", "emp_no", "salary", LONG_CHAIN_FIELDS));

        LogicalPlan result = applyOnSmallStack(analyzed);

        assertThat(groupingNames(result), hasSize(LONG_CHAIN_FIELDS + 2));
    }

    /** The same chain over an external source: the short links are pruned, the long ones are kept, and nothing rebuilt is deep. */
    public void testLongAliasChainOverExternalSourceIsBounded() {
        LogicalPlan analyzed = analyzedExternalPlan(aliasChainQuery(DATASET_NAME, "ClientIP", "OtherIP", LONG_CHAIN_FIELDS));

        LogicalPlan result = applyOnSmallStack(analyzed);

        List<String> groupings = groupingNames(result);
        assertThat(groupings, not(hasItem("i0")));
        assertThat(groupings, hasItem("i" + (LONG_CHAIN_FIELDS - 1)));
        assertThat(evalFieldDepths(result), everyItem(lessThanOrEqualTo(ExpressionBuilder.MAX_EXPRESSION_DEPTH)));
    }

    /** Each alias reads the two before it, so expanding every reference separately grows exponentially with the chain. */
    public void testSharedAliasReferencesAreBounded() {
        int fields = 5_000;
        StringBuilder query = new StringBuilder("FROM ext_ds\n| EVAL i0 = ClientIP + OtherIP, i1 = OtherIP + i0");
        for (int i = 2; i < fields; i++) {
            query.append(", i").append(i).append(" = i").append(i - 2).append(" + i").append(i - 1);
        }
        appendGroupings(query, "ClientIP", "OtherIP", fields);

        LogicalPlan result = applyOnSmallStack(analyzedExternalPlan(query.toString()));

        List<String> groupings = groupingNames(result);
        assertThat(groupings, not(hasItem("i0")));
        assertThat(groupings, hasItem("i" + (fields - 1)));
    }

    /** Over an index, like {@code HeapAttackIT#testGroupOnManyLongs}, but with the depth in the definitions, not the alias count. */
    public void testDeepAliasDefinitionsOverIndexKeepEveryGrouping() {
        LogicalPlan analyzed = defaultAnalyzer().query(deepDefinitionsQuery("test", "emp_no", "salary"));

        LogicalPlan result = applyOnSmallStack(analyzed);

        assertThat(groupingNames(result), contains("emp_no", "salary", "j" + (DEEP_ALIASES - 1)));
    }

    public void testDeepAliasDefinitionsOverExternalSourceAreBounded() {
        LogicalPlan analyzed = analyzedExternalPlan(deepDefinitionsQuery(DATASET_NAME, "ClientIP", "OtherIP"));

        LogicalPlan result = applyOnSmallStack(analyzed);

        assertThat(groupingNames(result), contains("ClientIP", "OtherIP", "j" + (DEEP_ALIASES - 1)));
    }

    /** Plain renames add no expression depth, but following each one is still a step of the expansion. */
    public void testRenameChainIsBounded() {
        StringBuilder query = new StringBuilder("FROM ext_ds\n| EVAL i0 = ClientIP");
        for (int i = 1; i < LONG_CHAIN_FIELDS; i++) {
            query.append(", i").append(i).append(" = i").append(i - 1);
        }
        query.append("\n| STATS c = COUNT(*) BY ClientIP, i").append(LONG_CHAIN_FIELDS - 1);

        LogicalPlan result = applyOnSmallStack(analyzedExternalPlan(query.toString()));

        assertThat(groupingNames(result), contains("ClientIP", "i" + (LONG_CHAIN_FIELDS - 1)));
    }

    /** Expanding a chain of {@code - 1} links visits three nodes per link: the subtraction, the alias it reads and the literal. */
    public void testPrunesDerivedGroupingWithinExpansionBudget() {
        int links = ExpressionBuilder.MAX_EXPRESSION_DEPTH / 3;

        var plan = externalPlan(subtractionChainQuery(links));

        assertThat(groupingNames(plan), contains("ClientIP"));
    }

    public void testKeepsDerivedGroupingBeyondExpansionBudget() {
        int links = ExpressionBuilder.MAX_EXPRESSION_DEPTH / 3 + 1;

        var plan = externalPlan(subtractionChainQuery(links));

        assertThat(groupingNames(plan), contains("ClientIP", "j" + (links - 1)));
    }

    /** {@code b} stays a grouping and reads the pruned {@code a}, so {@code a} must stay defined below the aggregate. */
    public void testKeepsPrunedAliasReadByKeptGrouping() {
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
     * {@code d} is unused but reads the pruned {@code a}. Both go: dropping only {@code a} would leave {@code d} dangling, and
     * keeping {@code a} for {@code d} would compute it for every row although nothing needs either.
     */
    public void testDropsUnusedFieldReadingPrunedAlias() {
        LogicalPlan analyzed = analyzedExternalPlan("""
            FROM ext_ds
            | EVAL a = ClientIP - 1, d = a + 1
            | STATS c = COUNT(*) BY ClientIP, a
            """);

        LogicalPlan result = new PruneRedundantAggregateGroupings().apply(analyzed);

        assertThat(groupingNames(result), contains("ClientIP"));
        List<Aggregate> aggregates = new ArrayList<>();
        result.forEachDown(Aggregate.class, aggregates::add);
        as(aggregates.get(0).child(), ExternalRelation.class);
    }

    /** A constant subtree is folded into the rebuilt grouping, so each node the expansion visits adds one node to the result. */
    public void testFoldsConstantInRebuiltGrouping() {
        LogicalPlan analyzed = analyzedExternalPlan("""
            FROM ext_ds
            | EVAL d = ClientIP + ABS(-1)
            | STATS c = COUNT(*) BY ClientIP, d
            """);

        LogicalPlan result = new PruneRedundantAggregateGroupings().apply(analyzed);

        assertThat(groupingNames(result), contains("ClientIP"));
        List<Expression> rebuilt = new ArrayList<>();
        result.forEachDown(
            Eval.class,
            eval -> eval.fields().stream().filter(f -> f.name().equals("d")).forEach(f -> rebuilt.add(f.child()))
        );
        assertThat(rebuilt, hasSize(1));
        as(as(rebuilt.get(0), Add.class).right(), Literal.class);
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

    private static String subtractionChainQuery(int links) {
        StringBuilder query = new StringBuilder("FROM ext_ds\n| EVAL j0 = ClientIP - 1");
        for (int i = 1; i < links; i++) {
            query.append(", j").append(i).append(" = j").append(i - 1).append(" - 1");
        }
        return query.append("\n| STATS c = COUNT(*) BY ClientIP, j").append(links - 1).toString();
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
        // Longer than safeJoin's timeout: the rule visits millions of nodes for the long chains, which takes a while interpreted.
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

    private static List<String> groupingNames(LogicalPlan plan) {
        List<Aggregate> aggregates = new ArrayList<>();
        plan.forEachDown(Aggregate.class, aggregates::add);
        assertThat(aggregates, hasSize(1));
        return Expressions.names(aggregates.get(0).groupings());
    }

    private static List<Integer> evalFieldDepths(LogicalPlan plan) {
        List<Integer> depths = new ArrayList<>();
        plan.forEachDown(Eval.class, eval -> eval.fields().forEach(field -> depths.add(depth(field.child()))));
        return depths;
    }

    /** Iterative, so it can measure expressions deeper than a recursive walk could visit. */
    private static int depth(Expression root) {
        int max = 0;
        Deque<Tuple<Expression, Integer>> pending = new ArrayDeque<>();
        pending.push(Tuple.tuple(root, 1));
        while (pending.isEmpty() == false) {
            Tuple<Expression, Integer> next = pending.pop();
            max = Math.max(max, next.v2());
            for (Expression child : next.v1().children()) {
                pending.push(Tuple.tuple(child, next.v2() + 1));
            }
        }
        return max;
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
