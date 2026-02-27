/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class PromqlPlanVectorMatchingTests extends AbstractPromqlPlanOptimizerTests {

    // --- 1:1 matching, compatible groupings ---

    public void testOnClauseSameGroupings() {
        var plan = planPromql(
            "PROMQL index=k8s step=1m ratio=(sum by (cluster)(network.total_bytes_in) / on(cluster) sum by (cluster)(network.bytes_in))"
        );
        assertThat(outputColumns(plan), equalTo(List.of("ratio", "step", "cluster")));
        assertThat(outerAggregates(plan), hasSize(1));
        assertThat(countAggFunctions(plan, Sum.class), equalTo(2L));
    }

    public void testOnEmptyListGlobalJoin() {
        var plan = planPromql("PROMQL index=k8s step=1m ratio=(sum(network.total_bytes_in) / on() sum(network.bytes_in))");
        assertThat(outputColumns(plan), equalTo(List.of("ratio", "step")));
        assertThat(outerAggregates(plan), hasSize(1));
        assertThat(countAggFunctions(plan, Sum.class), equalTo(2L));
    }

    public void testIgnoringEmptyListMatchAll() {
        var plan = planPromql("PROMQL index=k8s step=1m ratio=(sum(network.total_bytes_in) / ignoring() sum(network.bytes_in))");
        assertThat(outputColumns(plan), equalTo(List.of("ratio", "step")));
        assertThat(outerAggregates(plan), hasSize(1));
        assertThat(countAggFunctions(plan, Sum.class), equalTo(2L));
    }

    public void testOnClauseWithSubtraction() {
        var plan = planPromql(
            "PROMQL index=k8s step=1m diff=(max by (cluster)(network.bytes_in) - on(cluster) min by (cluster)(network.bytes_in))"
        );
        assertThat(outputColumns(plan), equalTo(List.of("diff", "step", "cluster")));
        assertThat(outerAggregates(plan), hasSize(1));
        assertThat(countAggFunctions(plan, Max.class), equalTo(1L));
        assertThat(countAggFunctions(plan, Min.class), equalTo(1L));
    }

    public void testOnClauseWithMultiplication() {
        var plan = planPromql(
            "PROMQL index=k8s step=1m prod=(sum by (cluster)(network.bytes_in) * on(cluster) sum by (cluster)(network.bytes_in))"
        );
        assertThat(outputColumns(plan), equalTo(List.of("prod", "step", "cluster")));
        assertThat(outerAggregates(plan), hasSize(1));
        assertThat(countAggFunctions(plan, Sum.class), equalTo(1L));
    }

    public void testOnClauseDifferentFieldsSameGroupings() {
        var plan = planPromql(
            "PROMQL index=k8s step=1m prod=(sum by (cluster)(network.bytes_in) * on(cluster) sum by (cluster)(network.total_bytes_in))"
        );
        assertThat(outputColumns(plan), equalTo(List.of("prod", "step", "cluster")));
        assertThat(outerAggregates(plan), hasSize(1));
        assertThat(countAggFunctions(plan, Sum.class), equalTo(2L));
    }

    public void testBlocked_onClauseInstantSelectors() {
        expectThrows(
            IllegalStateException.class,
            () -> planPromql("PROMQL index=k8s step=1m r=(network.bytes_in / on(cluster) network.bytes_in)")
        );
    }

    // --- 1:1 matching, incompatible groupings (requires group modifier) ---

    public void testBlocked_onClauseDifferentGroupings() {
        var e = expectThrows(
            QlIllegalArgumentException.class,
            () -> planPromql(
                "PROMQL index=k8s step=1m ratio=(sum by (cluster)(network.total_bytes_in) / on(cluster) sum(network.bytes_in))"
            )
        );
        assertThat(e.getMessage(), containsString("different grouping keys"));
    }

    public void testBlocked_ignoringDifferentGroupings() {
        var e = expectThrows(
            QlIllegalArgumentException.class,
            () -> planPromql(
                "PROMQL index=k8s step=1m ratio=(sum by (cluster, pod)(network.bytes_in)"
                    + " / ignoring(pod) sum by (cluster)(network.bytes_in))"
            )
        );
        assertThat(e.getMessage(), containsString("different grouping keys"));
    }

    public void testBlocked_ignoringMultipleLabels() {
        var e = expectThrows(
            QlIllegalArgumentException.class,
            () -> planPromql(
                "PROMQL index=k8s step=1m r=(sum by (cluster, pod, region)(network.bytes_in)"
                    + " / ignoring(pod, region) sum by (cluster)(network.bytes_in))"
            )
        );
        assertThat(e.getMessage(), containsString("different grouping keys"));
    }

    public void testBlocked_ignoringRightHasMoreDims() {
        var e = expectThrows(
            QlIllegalArgumentException.class,
            () -> planPromql(
                "PROMQL index=k8s step=1m r=(sum by (cluster)(network.bytes_in)"
                    + " / ignoring(pod) sum by (cluster, pod)(network.bytes_in))"
            )
        );
        assertThat(e.getMessage(), containsString("different grouping keys"));
    }

    // --- group_left / group_right (blocked until Phase 2) ---

    public void testGroupLeftBasic() {
        var query = "PROMQL index=k8s step=1m r=(sum by (cluster, pod)(network.bytes_in)"
            + " / ignoring(pod) group_left sum by (cluster)(network.bytes_in))";
        var analyzed = tsAnalyzer.analyze(parser.parseQuery(query));
        logger.info("analyzed plan:\n{}", analyzed);
        try {
            var optimized = logicalOptimizer.optimize(analyzed);
            logger.info("optimized plan:\n{}", optimized);
            assertThat(outputColumns(optimized), equalTo(List.of("r", "step", "cluster", "pod")));
            assertSelfContainedInlineJoin(optimized, Set.of("step", "cluster"));
        } catch (Exception e) {
            logger.info("optimization failed", e);
            throw e;
        }
    }

    public void testGroupRightBasic() {
        var query = "PROMQL index=k8s step=1m r=(sum by (cluster)(network.bytes_in)"
            + " / on(cluster) group_right sum by (cluster, pod)(network.bytes_in))";
        var analyzed = tsAnalyzer.analyze(parser.parseQuery(query));
        logger.info("analyzed plan:\n{}", analyzed);
        try {
            var optimized = logicalOptimizer.optimize(analyzed);
            logger.info("optimized plan:\n{}", optimized);
            assertThat(outputColumns(optimized), equalTo(List.of("r", "step", "cluster", "pod")));
            assertSelfContainedInlineJoin(optimized, Set.of("step", "cluster"));
        } catch (Exception e) {
            logger.info("optimization failed", e);
            throw e;
        }
    }

    public void testGroupLeftWithInlineSideNestedFunctionLayering() {
        var query = "PROMQL index=k8s step=1m r=(sum by (cluster, pod)(network.bytes_in)"
            + " / ignoring(pod) group_left ceil(sum by (cluster)(network.bytes_in)))";
        var optimized = logicalOptimizer.optimize(tsAnalyzer.analyze(parser.parseQuery(query)));
        assertThat(outputColumns(optimized), equalTo(List.of("r", "step", "cluster", "pod")));
        assertSelfContainedInlineJoin(optimized, Set.of("step", "cluster"));
    }

    public void testGroupRightWithInlineSideNestedFunctionLayering() {
        var query = "PROMQL index=k8s step=1m r=(ceil(sum by (cluster)(network.bytes_in))"
            + " / on(cluster) group_right sum by (cluster, pod)(network.bytes_in))";
        var optimized = logicalOptimizer.optimize(tsAnalyzer.analyze(parser.parseQuery(query)));
        assertThat(outputColumns(optimized), equalTo(List.of("r", "step", "cluster", "pod")));
        assertSelfContainedInlineJoin(optimized, Set.of("step", "cluster"));
    }

    // TODO: Phase 3 — extra labels from group_left(region) should project 'region' from the inline side
    public void testGroupLeftWithExtraLabels() {
        var query = "PROMQL index=k8s step=1m r=(sum by (cluster, pod)(network.bytes_in)"
            + " / ignoring(pod) group_left(region) sum by (cluster)(network.bytes_in))";
        var analyzed = tsAnalyzer.analyze(parser.parseQuery(query));
        var optimized = logicalOptimizer.optimize(analyzed);
        assertThat(outputColumns(optimized), equalTo(List.of("r", "step", "cluster", "pod")));
    }

    // --- parser validations ---

    public void testParserRejectsModifierWithScalarLeft() {
        var e = expectThrows(
            ParsingException.class,
            () -> planPromql("PROMQL index=k8s step=1m r=(1 / on(cluster) sum(network.bytes_in))")
        );
        assertThat(e.getMessage(), containsString("Vector matching allowed only between instant vectors"));
    }

    public void testParserRejectsModifierWithScalarRight() {
        var e = expectThrows(
            ParsingException.class,
            () -> planPromql("PROMQL index=k8s step=1m r=(sum(network.bytes_in) / on(cluster) 1)")
        );
        assertThat(e.getMessage(), containsString("Vector matching allowed only between instant vectors"));
    }

    public void testParserRejectsModifierWithScalarFunctionLeft() {
        var e = expectThrows(
            ParsingException.class,
            () -> planPromql("PROMQL index=k8s step=1m r=(pi() / on(cluster) sum(network.bytes_in))")
        );
        assertThat(e.getMessage(), containsString("Vector matching allowed only between instant vectors"));
    }

    public void testParserRejectsModifierWithRangeVectors() {
        var e = expectThrows(
            ParsingException.class,
            () -> planPromql("PROMQL index=k8s step=1m r=(network.bytes_in[1m] + on(cluster) network.bytes_in[1m])")
        );
        assertThat(e.getMessage(), containsString("Vector matching allowed only between instant vectors"));
    }

    public void testParserRejectsLabelInBothOnAndGroupClauses() {
        var e = expectThrows(
            ParsingException.class,
            () -> planPromql(
                "PROMQL index=k8s step=1m r=(sum by (cluster, pod)(network.bytes_in)"
                    + " / on(cluster, pod) group_left(cluster, region) sum by (pod)(network.bytes_in))"
            )
        );
        assertThat(e.getMessage(), containsString("must not occur in ON and GROUP clause at once"));
    }

    public void testParserRejectsGroupModifierWithSetOperators() {
        var e = expectThrows(
            ParsingException.class,
            () -> planPromql("PROMQL index=k8s step=1m r=(network.bytes_in and on(cluster) group_left network.bytes_in)")
        );
        assertThat(e.getMessage(), containsString("No grouping"));
    }

    // -- helpers --

    private static List<String> outputColumns(LogicalPlan plan) {
        return plan.output().stream().map(a -> a.name()).toList();
    }

    private static List<Aggregate> outerAggregates(LogicalPlan plan) {
        return plan.collect(Aggregate.class).stream().filter(a -> a instanceof TimeSeriesAggregate == false).toList();
    }

    private static void assertSelfContainedInlineJoin(LogicalPlan plan, Set<String> expectedKeys) {
        var joins = plan.collect(InlineJoin.class);
        assertThat(joins, hasSize(1));
        InlineJoin join = joins.getFirst();
        assertFalse(join.right().anyMatch(p -> p instanceof StubRelation));
        assertThat(plan.collect(StubRelation.class), hasSize(0));

        Set<String> keyNames = new HashSet<>(join.config().leftFields().stream().map(a -> a.name()).toList());
        assertThat(keyNames, equalTo(expectedKeys));
    }

    private static long countAggFunctions(LogicalPlan plan, Class<?> aggType) {
        return outerAggregates(plan).getFirst().aggregates().stream().filter(e -> e.anyMatch(aggType::isInstance)).count();
    }
}
