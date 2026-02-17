/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.BROADCAST_JOIN_MAX_ROWS_PER_SHARD;
import static org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.INDEX_JOIN_SLOWER_FACTOR;
import static org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.NO_LIMIT;

public class ShouldUseBroadcastStrategyTests extends ESTestCase {

    // -----------------------------------------------------------
    // shouldUseBroadcastStrategy — right-side rules
    // -----------------------------------------------------------

    public void testRightSideUnknown_returnsFalse() {
        PhysicalPlan left = esQueryExec(10_000);
        EsRelation right = esRelation(-1);
        LookupJoinExec join = join(left, right);

        assertFalse(callShouldUseBroadcast(join, right));
    }

    public void testRightSideTooLarge_returnsFalse() {
        PhysicalPlan left = esQueryExec(10_000);
        EsRelation right = esRelation(BROADCAST_JOIN_MAX_ROWS_PER_SHARD + 1);
        LookupJoinExec join = join(left, right);

        assertFalse(callShouldUseBroadcast(join, right));
    }

    public void testRightSideExactlyAtThreshold_withLargeLeft_returnsTrue() {
        PhysicalPlan left = esQueryExec(100_000);
        EsRelation right = esRelation(BROADCAST_JOIN_MAX_ROWS_PER_SHARD);
        LookupJoinExec join = join(left, right);

        // left=100_000, right=50_000; 100_000 * 5 = 500_000 > 50_000 → broadcast
        assertTrue(callShouldUseBroadcast(join, right));
    }

    public void testRightSideZeroRows_returnsFalse() {
        PhysicalPlan left = esQueryExec(0);
        EsRelation right = esRelation(0);
        LookupJoinExec join = join(left, right);

        // 0 * 5 = 0, not > 0 → false
        assertFalse(callShouldUseBroadcast(join, right));
    }

    // -----------------------------------------------------------
    // shouldUseBroadcastStrategy — cost comparison
    // -----------------------------------------------------------

    public void testLargeLeftSmallRight_broadcast() {
        PhysicalPlan left = esQueryExec(10_000);
        EsRelation right = esRelation(1_000);
        LookupJoinExec join = join(left, right);

        // 10_000 * 5 = 50_000 > 1_000 → broadcast
        assertTrue(callShouldUseBroadcast(join, right));
    }

    public void testSmallLeftSmallRight_indexJoin() {
        PhysicalPlan left = limitExec(esQueryExec(10_000), 1);
        EsRelation right = esRelation(100);
        LookupJoinExec join = join(left, right);

        // left limit = 1; 1 * 5 = 5, not > 100 → index join
        assertFalse(callShouldUseBroadcast(join, right));
    }

    public void testLeftExactlyAtBoundary_indexJoin() {
        int rightRows = 500;
        int leftRows = rightRows / INDEX_JOIN_SLOWER_FACTOR; // 100
        PhysicalPlan left = esQueryExec(leftRows);
        EsRelation right = esRelation(rightRows);
        LookupJoinExec join = join(left, right);

        // 100 * 5 = 500, not > 500 → false
        assertFalse(callShouldUseBroadcast(join, right));
    }

    public void testLeftJustAboveBoundary_broadcast() {
        int rightRows = 500;
        int leftRows = rightRows / INDEX_JOIN_SLOWER_FACTOR + 1; // 101
        PhysicalPlan left = esQueryExec(leftRows);
        EsRelation right = esRelation(rightRows);
        LookupJoinExec join = join(left, right);

        // 101 * 5 = 505 > 500 → true
        assertTrue(callShouldUseBroadcast(join, right));
    }

    public void testLeftSourceUnknown_smallRight_broadcast() {
        PhysicalPlan left = esQueryExec(-1);
        EsRelation right = esRelation(1_000);
        LookupJoinExec join = join(left, right);

        // Unknown left source → NO_LIMIT → huge * 5 > small right → broadcast
        assertTrue(callShouldUseBroadcast(join, right));
    }

    // -----------------------------------------------------------
    // shouldUseBroadcastStrategy — upstream limit
    // -----------------------------------------------------------

    public void testUpstreamLimitConstrainsLeft() {
        PhysicalPlan left = limitExec(esQueryExec(100_000), 10);
        EsRelation right = esRelation(1_000);
        LookupJoinExec join = join(left, right);

        // upstream=10, downstream=NO_LIMIT, min=10; 10*5=50, not > 1_000 → false
        assertFalse(callShouldUseBroadcast(join, right));
    }

    public void testUpstreamTopNConstrainsLeft() {
        PhysicalPlan left = topNExec(esQueryExec(100_000), 10);
        EsRelation right = esRelation(1_000);
        LookupJoinExec join = join(left, right);

        // upstream=10 (topN caps), downstream=NO_LIMIT, min=10; 10*5=50, not > 1_000 → false
        assertFalse(callShouldUseBroadcast(join, right));
    }

    public void testUpstreamLimitHigherThanSource_usesSource() {
        PhysicalPlan left = limitExec(esQueryExec(100), 10_000);
        EsRelation right = esRelation(1_000);
        LookupJoinExec join = join(left, right);

        // upstream = min(100, 10_000) = 100; 100*5=500, not > 1_000 → false
        assertFalse(callShouldUseBroadcast(join, right));
    }

    public void testUpstreamAggPreservesChildEstimate() {
        PhysicalPlan left = aggregateExec(esQueryExec(10_000));
        EsRelation right = esRelation(1_000);
        LookupJoinExec join = join(left, right);

        // upstream = 10_000 (agg passes through), 10_000*5 = 50_000 > 1_000 → true
        assertTrue(callShouldUseBroadcast(join, right));
    }

    public void testUpstreamFilterPreservesChildEstimate() {
        PhysicalPlan left = filterExec(esQueryExec(10_000));
        EsRelation right = esRelation(1_000);
        LookupJoinExec join = join(left, right);

        // upstream = 10_000 (filter passes through), 10_000*5 = 50_000 > 1_000 → true
        assertTrue(callShouldUseBroadcast(join, right));
    }

    public void testUpstreamLimitThenAgg() {
        PhysicalPlan left = aggregateExec(limitExec(esQueryExec(100_000), 50));
        EsRelation right = esRelation(1_000);
        LookupJoinExec join = join(left, right);

        // upstream: agg → limit 50 → source 100_000 → min(100_000, 50)=50; 50*5=250, not > 1_000 → false
        assertFalse(callShouldUseBroadcast(join, right));
    }

    // -----------------------------------------------------------
    // shouldUseBroadcastStrategy — downstream limit
    // -----------------------------------------------------------

    public void testDownstreamLimitConstrainsJoin() {
        PhysicalPlan left = esQueryExec(100_000);
        EsRelation right = esRelation(1_000);
        LookupJoinExec join = join(left, right);
        PhysicalPlan root = limitExec(join, 10);

        // upstream=100_000, downstream=10, min=10; 10*5=50, not > 1_000 → false
        assertFalse(callShouldUseBroadcast(root, join, right));
    }

    public void testDownstreamTopNConstrainsJoin() {
        PhysicalPlan left = esQueryExec(100_000);
        EsRelation right = esRelation(1_000);
        LookupJoinExec join = join(left, right);
        PhysicalPlan root = topNExec(join, 10);

        // upstream=100_000, downstream=10, min=10; 10*5=50, not > 1_000 → false
        assertFalse(callShouldUseBroadcast(root, join, right));
    }

    public void testDownstreamAggResetsLimit() {
        // limit 10 → agg → join → source 100_000
        PhysicalPlan left = esQueryExec(100_000);
        EsRelation right = esRelation(1_000);
        LookupJoinExec join = join(left, right);
        PhysicalPlan root = limitExec(aggregateExec(join), 10);

        // downstream: limit 10 → agg resets → NO_LIMIT; upstream=100_000; min(100_000, NO_LIMIT)=100_000
        // 100_000*5 = 500_000 > 1_000 → true
        assertTrue(callShouldUseBroadcast(root, join, right));
    }

    public void testDownstreamAggThenLimit() {
        // agg → limit 10 → join → source 100_000
        PhysicalPlan left = esQueryExec(100_000);
        EsRelation right = esRelation(1_000);
        LookupJoinExec join = join(left, right);
        PhysicalPlan root = aggregateExec(limitExec(join, 10));

        // downstream: agg resets → NO_LIMIT → limit 10 → 10; upstream=100_000; min(100_000, 10)=10
        // 10*5=50, not > 1_000 → false
        assertFalse(callShouldUseBroadcast(root, join, right));
    }

    public void testDownstreamMultipleLimits_takesTightest() {
        // limit 100 → limit 10 → join → source 100_000
        PhysicalPlan left = esQueryExec(100_000);
        EsRelation right = esRelation(1_000);
        LookupJoinExec join = join(left, right);
        PhysicalPlan root = limitExec(limitExec(join, 10), 100);

        // downstream: min(100, 10) = 10; upstream=100_000; min=10; 10*5=50, not > 1_000 → false
        assertFalse(callShouldUseBroadcast(root, join, right));
    }

    // -----------------------------------------------------------
    // shouldUseBroadcastStrategy — combined upstream + downstream
    // -----------------------------------------------------------

    public void testMinOfUpstreamAndDownstream() {
        // limit 50 → join → limit 20 → source 100_000
        PhysicalPlan left = limitExec(esQueryExec(100_000), 20);
        EsRelation right = esRelation(1_000);
        LookupJoinExec join = join(left, right);
        PhysicalPlan root = limitExec(join, 50);

        // upstream = min(100_000, 20) = 20; downstream = 50; expectedLeftRows = 20
        // 20*5=100, not > 1_000 → false
        assertFalse(callShouldUseBroadcast(root, join, right));
    }

    public void testDownstreamTighterThanUpstream() {
        // limit 5 → join → source 10_000
        PhysicalPlan left = esQueryExec(10_000);
        EsRelation right = esRelation(1_000);
        LookupJoinExec join = join(left, right);
        PhysicalPlan root = limitExec(join, 5);

        // upstream = 10_000; downstream = 5; expectedLeftRows = 5
        // 5*5=25, not > 1_000 → false
        assertFalse(callShouldUseBroadcast(root, join, right));
    }

    public void testUpstreamTighterThanDownstream() {
        // limit 10_000 → join → limit 5 → source 100_000
        PhysicalPlan left = limitExec(esQueryExec(100_000), 5);
        EsRelation right = esRelation(1_000);
        LookupJoinExec join = join(left, right);
        PhysicalPlan root = limitExec(join, 10_000);

        // upstream = min(100_000, 5) = 5; downstream = 10_000; expectedLeftRows = 5
        // 5*5=25, not > 1_000 → false
        assertFalse(callShouldUseBroadcast(root, join, right));
    }

    // -----------------------------------------------------------
    // shouldUseBroadcastStrategy — EsSourceExec (non-optimized source)
    // -----------------------------------------------------------

    public void testEsSourceExecWithKnownAvg_broadcast() {
        PhysicalPlan left = esSourceExec(10_000);
        EsRelation right = esRelation(1_000);
        LookupJoinExec join = join(left, right);

        // 10_000*5 = 50_000 > 1_000 → true
        assertTrue(callShouldUseBroadcast(join, right));
    }

    public void testEsSourceExecWithUnknownAvg_broadcast() {
        PhysicalPlan left = esSourceExec(-1);
        EsRelation right = esRelation(1_000);
        LookupJoinExec join = join(left, right);

        // unknown → NO_LIMIT; NO_LIMIT*5 doesn't overflow; > 1_000 → true
        assertTrue(callShouldUseBroadcast(join, right));
    }

    // -----------------------------------------------------------
    // estimateRows — unit tests
    // -----------------------------------------------------------

    public void testEstimateRows_esQueryExecKnown() {
        long avg = randomLongBetween(0, 1_000_000);
        assertEquals(avg, LocalExecutionPlanner.estimateRows(esQueryExec(avg), context(esQueryExec(avg))));
    }

    public void testEstimateRows_esQueryExecUnknown() {
        assertEquals(NO_LIMIT, LocalExecutionPlanner.estimateRows(esQueryExec(-1), context(esQueryExec(-1))));
    }

    public void testEstimateRows_esSourceExecKnown() {
        long avg = randomLongBetween(0, 1_000_000);
        assertEquals(avg, LocalExecutionPlanner.estimateRows(esSourceExec(avg), context(esSourceExec(avg))));
    }

    public void testEstimateRows_esSourceExecUnknown() {
        assertEquals(NO_LIMIT, LocalExecutionPlanner.estimateRows(esSourceExec(-1), context(esSourceExec(-1))));
    }

    public void testEstimateRows_limitCapsChild() {
        PhysicalPlan source = esQueryExec(10_000);
        PhysicalPlan limited = limitExec(source, 50);
        assertEquals(50, LocalExecutionPlanner.estimateRows(limited, context(limited)));
    }

    public void testEstimateRows_limitHigherThanChild() {
        PhysicalPlan source = esQueryExec(100);
        PhysicalPlan limited = limitExec(source, 10_000);
        assertEquals(100, LocalExecutionPlanner.estimateRows(limited, context(limited)));
    }

    public void testEstimateRows_topNCapsChild() {
        PhysicalPlan source = esQueryExec(10_000);
        PhysicalPlan topn = topNExec(source, 50);
        assertEquals(50, LocalExecutionPlanner.estimateRows(topn, context(topn)));
    }

    public void testEstimateRows_aggPassesThrough() {
        PhysicalPlan source = esQueryExec(10_000);
        PhysicalPlan agg = aggregateExec(source);
        assertEquals(10_000, LocalExecutionPlanner.estimateRows(agg, context(agg)));
    }

    public void testEstimateRows_filterPassesThrough() {
        PhysicalPlan source = esQueryExec(10_000);
        PhysicalPlan filter = filterExec(source);
        assertEquals(10_000, LocalExecutionPlanner.estimateRows(filter, context(filter)));
    }

    public void testEstimateRows_chainLimitAgg() {
        // source 100_000 → limit 50 → agg → result should be 50
        PhysicalPlan source = esQueryExec(100_000);
        PhysicalPlan limited = limitExec(source, 50);
        PhysicalPlan agg = aggregateExec(limited);
        assertEquals(50, LocalExecutionPlanner.estimateRows(agg, context(agg)));
    }

    public void testEstimateRows_chainAggLimit() {
        // source 100_000 → agg → limit 50 → result should be 50
        PhysicalPlan source = esQueryExec(100_000);
        PhysicalPlan agg = aggregateExec(source);
        PhysicalPlan limited = limitExec(agg, 50);
        assertEquals(50, LocalExecutionPlanner.estimateRows(limited, context(limited)));
    }

    public void testEstimateRows_multipleChildren_takesMax() {
        // LookupJoinExec has two children; estimateRows should take the max
        PhysicalPlan left = esQueryExec(100);
        PhysicalPlan right = esQueryExec(10_000);
        LookupJoinExec join = lookupJoinExec(left, right);
        assertEquals(10_000, LocalExecutionPlanner.estimateRows(join, context(join)));
    }

    // -----------------------------------------------------------
    // estimateDownstreamRows — unit tests
    // -----------------------------------------------------------

    public void testDownstreamRows_noConstraint() {
        PhysicalPlan source = esQueryExec(10_000);
        PhysicalPlan right = esQueryExec(100);
        LookupJoinExec join = lookupJoinExec(source, right);

        assertEquals(NO_LIMIT, LocalExecutionPlanner.estimateDownstreamRows(join, context(join)));
    }

    public void testDownstreamRows_limitAbove() {
        PhysicalPlan source = esQueryExec(10_000);
        PhysicalPlan right = esQueryExec(100);
        LookupJoinExec join = lookupJoinExec(source, right);
        PhysicalPlan root = limitExec(join, 42);

        assertEquals(42, LocalExecutionPlanner.estimateDownstreamRows(join, context(root)));
    }

    public void testDownstreamRows_topNAbove() {
        PhysicalPlan source = esQueryExec(10_000);
        PhysicalPlan right = esQueryExec(100);
        LookupJoinExec join = lookupJoinExec(source, right);
        PhysicalPlan root = topNExec(join, 42);

        assertEquals(42, LocalExecutionPlanner.estimateDownstreamRows(join, context(root)));
    }

    public void testDownstreamRows_aggAboveResetsLimit() {
        // limit 10 → agg → join: agg resets the downstream limit
        PhysicalPlan source = esQueryExec(10_000);
        PhysicalPlan right = esQueryExec(100);
        LookupJoinExec join = lookupJoinExec(source, right);
        PhysicalPlan agg = aggregateExec(join);
        PhysicalPlan root = limitExec(agg, 10);

        assertEquals(NO_LIMIT, LocalExecutionPlanner.estimateDownstreamRows(join, context(root)));
    }

    public void testDownstreamRows_limitAggLimit() {
        // limit 100 → agg → limit 10 → join
        // limit 100 sets limit, agg resets to NO_LIMIT, limit 10 tightens to 10
        PhysicalPlan source = esQueryExec(10_000);
        PhysicalPlan right = esQueryExec(100);
        LookupJoinExec join = lookupJoinExec(source, right);
        PhysicalPlan innerLimit = limitExec(join, 10);
        PhysicalPlan agg = aggregateExec(innerLimit);
        PhysicalPlan root = limitExec(agg, 100);

        assertEquals(10, LocalExecutionPlanner.estimateDownstreamRows(join, context(root)));
    }

    public void testDownstreamRows_multipleLimits() {
        // limit 100 → limit 10 → join
        PhysicalPlan source = esQueryExec(10_000);
        PhysicalPlan right = esQueryExec(100);
        LookupJoinExec join = lookupJoinExec(source, right);
        PhysicalPlan limit1 = limitExec(join, 10);
        PhysicalPlan root = limitExec(limit1, 100);

        assertEquals(10, LocalExecutionPlanner.estimateDownstreamRows(join, context(root)));
    }

    public void testDownstreamRows_filterDoesNotAffect() {
        // filter → limit 42 → join
        PhysicalPlan source = esQueryExec(10_000);
        PhysicalPlan right = esQueryExec(100);
        LookupJoinExec join = lookupJoinExec(source, right);
        PhysicalPlan limited = limitExec(join, 42);
        PhysicalPlan root = filterExec(limited);

        assertEquals(42, LocalExecutionPlanner.estimateDownstreamRows(join, context(root)));
    }

    // -----------------------------------------------------------
    // NO_LIMIT arithmetic safety
    // -----------------------------------------------------------

    public void testNoLimitTimesFactorDoesNotOverflow() {
        long product = NO_LIMIT * INDEX_JOIN_SLOWER_FACTOR;
        assertTrue("NO_LIMIT * INDEX_JOIN_SLOWER_FACTOR should not overflow", product > 0);
    }

    public void testNoLimitExceedsAnyRealisticRight() {
        // Even the maximum allowed right-side value should be smaller than NO_LIMIT * factor
        assertTrue(NO_LIMIT * INDEX_JOIN_SLOWER_FACTOR > BROADCAST_JOIN_MAX_ROWS_PER_SHARD);
    }

    // -----------------------------------------------------------
    // Helper builders
    // -----------------------------------------------------------

    private static EsQueryExec esQueryExec(long avgRowsPerShard) {
        return new EsQueryExec(
            Source.EMPTY,
            "test",
            IndexMode.STANDARD,
            List.of(),
            null,
            null,
            null,
            List.of(new EsQueryExec.QueryBuilderAndTags(null, List.of())),
            avgRowsPerShard
        );
    }

    private static EsSourceExec esSourceExec(long avgRowsPerShard) {
        return new EsSourceExec(Source.EMPTY, "test", IndexMode.STANDARD, List.of(), null, avgRowsPerShard);
    }

    private static EsRelation esRelation(long avgRowsPerShard) {
        return new EsRelation(Source.EMPTY, "lookup_index", IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), List.of(), avgRowsPerShard);
    }

    private static Literal intLiteral(int value) {
        return new Literal(Source.EMPTY, value, DataType.INTEGER);
    }

    private static LimitExec limitExec(PhysicalPlan child, int limit) {
        return new LimitExec(Source.EMPTY, child, intLiteral(limit), null);
    }

    private static TopNExec topNExec(PhysicalPlan child, int limit) {
        return new TopNExec(Source.EMPTY, child, List.of(), intLiteral(limit), null);
    }

    private static AggregateExec aggregateExec(PhysicalPlan child) {
        return new AggregateExec(Source.EMPTY, child, List.of(), List.of(), AggregatorMode.SINGLE, List.of(), null);
    }

    private static FilterExec filterExec(PhysicalPlan child) {
        return new FilterExec(Source.EMPTY, child, new Literal(Source.EMPTY, true, DataType.BOOLEAN));
    }

    private static LookupJoinExec lookupJoinExec(PhysicalPlan left, PhysicalPlan right) {
        return new LookupJoinExec(Source.EMPTY, left, right, List.of(), List.of(), List.of(), null);
    }

    /**
     * Build a LookupJoinExec with the given left plan and a right side consistent with the EsRelation.
     */
    private static LookupJoinExec join(PhysicalPlan left, EsRelation right) {
        return lookupJoinExec(left, esQueryExec(right.avgRowsPerShard()));
    }

    /**
     * Build a context whose localPhysicalPlan is the given root (needed by estimateDownstreamRows).
     */
    private static LocalExecutionPlanner.LocalExecutionPlannerContext context(PhysicalPlan root) {
        return new LocalExecutionPlanner.LocalExecutionPlannerContext(
            "test",
            new ArrayList<>(),
            new Holder<>(LocalExecutionPlanner.DriverParallelism.SINGLE),
            new QueryPragmas(Settings.EMPTY),
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            FoldContext.small(),
            PlannerSettings.DEFAULTS,
            false,
            null,
            root
        );
    }

    /**
     * Convenience for when the join is the root of the plan (no downstream operators).
     */
    private static boolean callShouldUseBroadcast(LookupJoinExec join, EsRelation right) {
        return callShouldUseBroadcast(join, join, right);
    }

    /**
     * Convenience: build a context + call shouldUseBroadcastStrategy.
     * {@code root} must be the entire plan tree (including the join), so that downstream estimation works.
     */
    private static boolean callShouldUseBroadcast(PhysicalPlan root, LookupJoinExec join, EsRelation right) {
        return LocalExecutionPlanner.shouldUseBroadcastStrategy(join, right, context(root));
    }
}
