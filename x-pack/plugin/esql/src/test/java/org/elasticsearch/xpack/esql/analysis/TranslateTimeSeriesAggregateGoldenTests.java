/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;

/**
 * Golden tests for {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.TranslateTimeSeriesAggregate},
 * which runs as an Analyzer rule and splits a {@code TimeSeriesAggregate} into an inner first-pass and an
 * outer second-pass {@code Aggregate}.
 *
 * <p>Regression coverage for constant literal aggregates (e.g. {@code metric_type = "cost"}) being
 * placed in the wrong phase before the fix tracked by {@code TS_STATS_LITERAL_AGG_FIX}.
 *
 * <p>All tests are bounded below by {@code DimensionValues.DIMENSION_VALUES_VERSION}: below that
 * transport version {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.TranslateTimeSeriesAggregate}
 * uses {@code Values} rather than {@code DimensionValues} for dimension group-by keys, producing a
 * different plan shape. At {@code pack_dims_agg} the PackDims node folds into the TimeSeriesAggregate
 * as PACKDIMSAGG, so the older shape lives in [before_pack_dims_agg].
 */
public class TranslateTimeSeriesAggregateGoldenTests extends GoldenTestCase {

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public TranslateTimeSeriesAggregateGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS);
    private static final String PACK_DIMS_AGG = "pack_dims_agg";

    /** Single literal aggregate first in STATS clause. */
    public void testLiteralAggregateFirst() {
        builder("""
            TS k8s
            | STATS metric_type = "cost", cnt = COUNT(COUNT_OVER_TIME(network.cost)) BY cluster
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    /** Single literal aggregate last in STATS clause — position must not affect routing. */
    public void testLiteralAggregateLast() {
        builder("""
            TS k8s
            | STATS cnt = COUNT(COUNT_OVER_TIME(network.cost)), metric_type = "cost" BY cluster
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    /** Multiple literal aggregates in one STATS — all must reach the outer Aggregate. */
    public void testMultipleLiteralAggregates() {
        builder("""
            TS k8s
            | STATS source = "k8s", metric = "cost", cnt = COUNT(COUNT_OVER_TIME(network.cost)) BY cluster
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    /** Integer literal — type is irrelevant; the fix applies to any Literal child. */
    public void testIntegerLiteralAggregate() {
        builder("""
            TS k8s
            | STATS version = 2, cnt = COUNT(COUNT_OVER_TIME(network.cost)) BY cluster
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    /** Multiple grouping keys — literals must still be routed to the outer Aggregate. */
    public void testLiteralAggregateWithMultipleGroupings() {
        builder("""
            TS k8s
            | STATS metric_type = "cost", cnt = COUNT(COUNT_OVER_TIME(network.cost)) BY cluster, pod
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    /** FORK branches each containing a TS STATS with a per-branch literal label. */
    public void testLiteralAggregateInForkBranch() {
        builder("""
            TS k8s
            | FORK
                (STATS metric_type = "cost", cnt = COUNT(COUNT_OVER_TIME(network.cost)) BY cluster)
                (STATS metric_type = "cost_max", cnt = COUNT(MAX_OVER_TIME(network.cost)) BY cluster)
            | KEEP _fork, cluster, metric_type, cnt
            | SORT _fork, cluster
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }
}
