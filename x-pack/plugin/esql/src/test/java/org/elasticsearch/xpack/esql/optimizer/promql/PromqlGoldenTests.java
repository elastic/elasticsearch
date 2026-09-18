/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;

/**
 * Golden tests for PromQL to ESQL plan translation.
 */
public class PromqlGoldenTests extends GoldenTestCase {
    private static final String DIMENSION_VALUES = "dimension_values";
    private static final String ESQL_SUM_LONG_OVERFLOW_FIX = "esql_sum_long_overflow_fix";
    private static final String PACK_DIMS_AGG = "pack_dims_agg";

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public PromqlGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    public void testSimpleInstantSelector() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        builder("PROMQL index=k8s step=1h network.bytes_in").expectationChangesAt(DIMENSION_VALUES)
            .expectationChangesAt(PACK_DIMS_AGG)
            .run();
    }

    public void testAdditionScalarScalar() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        builder("PROMQL index=k8s step=1h sum=(1+1)").run();
    }

    public void testMultiplicationMetricScalar() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        builder("PROMQL index=k8s step=1h network_in_bits=(network.total_bytes_in * 8)").expectationChangesAt(DIMENSION_VALUES)
            .expectationChangesAt(PACK_DIMS_AGG)
            .run();
    }

    public void testMultiplicationAcrossSeriesScalar() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        builder("""
            PROMQL index=k8s step=1h max_bits=(
              max(network.total_bytes_in) * 8
            )""").run();
    }

    public void testFirstOverTimeAllValueTypes() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        builder("PROMQL index=k8s step=10m events=(sum by (pod) (first_over_time(events_received[10m])))").expectationChangesAt(
            DIMENSION_VALUES
        ).expectationChangesAt(ESQL_SUM_LONG_OVERFLOW_FIX).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    public void testPromqlSourceWithGrok() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        assumeTrue("requires URI_PARTS command", EsqlCapabilities.Cap.URI_PARTS_COMMAND.isEnabled());
        builder("""
            PROMQL index=k8s-downsampled step=1h oYJdEiiJ=(network.bytes_in{cluster!="qa",pod!="two"})
            | GROK _timeseries "%{WORD:zEyDkwmbYa} %{WORD:step} %{WORD:step}"
            | URI_PARTS parts = _timeseries
            | DROP _timeseries, oYJdEiiJ, step, zEyDkwmbYa
            | LIMIT 1""").expectationChangesAt(DIMENSION_VALUES).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    public void testImplicitLastOverTimeOfLong() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        builder("PROMQL index=k8s step=1m bytes=(avg by (cluster) (network.bytes_in))").expectationChangesAt(DIMENSION_VALUES)
            .expectationChangesAt(ESQL_SUM_LONG_OVERFLOW_FIX)
            .expectationChangesAt(PACK_DIMS_AGG)
            .run();
    }

    public void testCaseInsensitivityAggregator() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        builder("PROMQL index=k8s step=1h bytes=(SUM by (pod) (network.bytes_in))").expectationChangesAt(DIMENSION_VALUES)
            .expectationChangesAt(ESQL_SUM_LONG_OVERFLOW_FIX)
            .expectationChangesAt(PACK_DIMS_AGG)
            .run();
    }

    public void testBinaryWithDifferentSelectors() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        builder("PROMQL index=k8s step=1m result=(sum(avg_over_time(network.cost[1m]) + avg_over_time(network.cost[10m])))")
            .expectationChangesAt(ESQL_SUM_LONG_OVERFLOW_FIX)
            .run();
    }

    public void testInstantQueryScalarTimeFn() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        assumeTrue("requires PromQL instant query support", EsqlCapabilities.Cap.PROMQL_INSTANT_QUERY.isEnabled());
        builder("PROMQL index=k8s time=\"2024-05-10T00:03:00.000Z\" result=(time())").run();
    }

    public void testTopk() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        assumeTrue("requires PromQL topk support", EsqlCapabilities.Cap.PROMQL_TOPK.isEnabled());
        builder("PROMQL index=k8s step=1h result=(topk(3, network.bytes_in))").expectationChangesAt(DIMENSION_VALUES)
            .expectationChangesAt(PACK_DIMS_AGG)
            .run();
    }

    public void testTopkByGrouping() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        assumeTrue("requires PromQL topk support", EsqlCapabilities.Cap.PROMQL_TOPK.isEnabled());
        builder("PROMQL index=k8s step=1h result=(topk(2, network.bytes_in) by (pod))").expectationChangesAt(DIMENSION_VALUES)
            .expectationChangesAt(PACK_DIMS_AGG)
            .run();
    }

    public void testTopkOverSumBy() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        assumeTrue("requires PromQL topk support", EsqlCapabilities.Cap.PROMQL_TOPK.isEnabled());
        assumeTrue("requires fix for topk over aggregated vectors", EsqlCapabilities.Cap.FIX_PROMQL_TOPK_OVER_AGGREGATE.isEnabled());
        builder("PROMQL index=k8s step=1h result=(topk(2, sum by (pod) (network.bytes_in)))").expectationChangesAt(DIMENSION_VALUES)
            .expectationChangesAt(ESQL_SUM_LONG_OVERFLOW_FIX)
            .expectationChangesAt(PACK_DIMS_AGG)
            .run();
    }

    public void testLimitk() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        assumeTrue("requires PromQL limitk support", EsqlCapabilities.Cap.PROMQL_LIMITK.isEnabled());
        builder("PROMQL index=k8s step=1h result=(limitk(3, network.bytes_in))").expectationChangesAt(DIMENSION_VALUES)
            .expectationChangesAt(PACK_DIMS_AGG)
            .run();
    }

    public void testLimitkByGrouping() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        assumeTrue("requires PromQL limitk support", EsqlCapabilities.Cap.PROMQL_LIMITK.isEnabled());
        builder("PROMQL index=k8s step=1h result=(limitk(2, network.bytes_in) by (pod))").expectationChangesAt(DIMENSION_VALUES)
            .expectationChangesAt(PACK_DIMS_AGG)
            .run();
    }

    public void testLimitkOverSumBy() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        assumeTrue("requires PromQL limitk support", EsqlCapabilities.Cap.PROMQL_LIMITK.isEnabled());
        assumeTrue("requires fix for topk over aggregated vectors", EsqlCapabilities.Cap.FIX_PROMQL_TOPK_OVER_AGGREGATE.isEnabled());
        builder("PROMQL index=k8s step=1h result=(limitk(2, sum by (pod) (network.bytes_in)))").expectationChangesAt(DIMENSION_VALUES)
            .expectationChangesAt(ESQL_SUM_LONG_OVERFLOW_FIX)
            .expectationChangesAt(PACK_DIMS_AGG)
            .run();
    }

    // Grouping on a derived label materializes it as a concrete column and pins where the relabel derivation sits relative
    // to the outer aggregate (columns-only identity out of by(dst)). Uses the flat-dimension k8s index so the source label
    // being read is materialized as a supported column - a passthrough dimension is unsupported in this static analyzer.
    public void testSumByDerivedLabel() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        assumeTrue("requires PromQL label functions", EsqlCapabilities.Cap.PROMQL_LABEL_FUNCTIONS.isEnabled());
        builder("""
            PROMQL index=k8s step=1h result=(
              sum by (tier) (label_replace(network.bytes_in, "tier", "$1", "region", "(.+)"))
            )""").stages(EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION))
            .expectationChangesAt(DIMENSION_VALUES)
            .expectationChangesAt(ESQL_SUM_LONG_OVERFLOW_FIX)
            .expectationChangesAt(PACK_DIMS_AGG)
            .run();
    }

    // A destination that overwrites a stored dimension: the derived column shadows the stored pod in the header and the
    // enclosing by(pod) groups on the derived value, while the derivation falls back to the stored pod on no-match.
    // Uses the flat-dimension k8s index (rather than the labels.* passthrough index) so the stored dimension being
    // overwritten is materialized as a supported column - a passthrough dimension is unsupported in this static analyzer.
    public void testLabelReplaceOverwritesStoredLabel() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        assumeTrue("requires PromQL label functions", EsqlCapabilities.Cap.PROMQL_LABEL_FUNCTIONS.isEnabled());
        builder("""
            PROMQL index=k8s step=1h result=(
              sum by (pod) (label_replace(network.bytes_in, "pod", "p-$1", "pod", "(.+)"))
            )""").stages(EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION))
            .expectationChangesAt(DIMENSION_VALUES)
            .expectationChangesAt(ESQL_SUM_LONG_OVERFLOW_FIX)
            .expectationChangesAt(PACK_DIMS_AGG)
            .run();
    }

    // No-match preserves the existing destination: the outer COALESCE falls back to the stored pod rather than "".
    // Uses the flat-dimension k8s index so the preserved stored destination is materialized as a supported column.
    public void testLabelReplaceNoMatchPreservesExisting() {
        assumeTrue("requires PromQL support", EsqlCapabilities.Cap.PROMQL_COMMAND_V0.isEnabled());
        assumeTrue("requires PromQL label functions", EsqlCapabilities.Cap.PROMQL_LABEL_FUNCTIONS.isEnabled());
        builder("""
            PROMQL index=k8s step=1h result=(
              sum by (pod) (label_replace(network.bytes_in, "pod", "x", "region", "nomatch"))
            )""").stages(EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION))
            .expectationChangesAt(DIMENSION_VALUES)
            .expectationChangesAt(ESQL_SUM_LONG_OVERFLOW_FIX)
            .expectationChangesAt(PACK_DIMS_AGG)
            .run();
    }
}
