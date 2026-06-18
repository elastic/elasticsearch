/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;

import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies that the time-series partial-aggregation pragmas are read independently from the regular aggregation
 * pragmas, keeping the time-series chunking knobs fully decoupled.
 */
public class QueryPragmasTests extends ESTestCase {

    public void testTimeSeriesEmitKeysThresholdPragmaOverridesDefault() {
        QueryPragmas pragmas = new QueryPragmas(
            Settings.builder().put(PlannerSettings.TIME_SERIES_PARTIAL_AGGREGATION_EMIT_KEYS_THRESHOLD.getKey(), 250).build()
        );
        assertThat(pragmas.timeSeriesPartialAggregationEmitKeysThreshold(100_000), equalTo(250));
    }

    public void testTimeSeriesEmitKeysThresholdPragmaFallsBackToProvidedDefault() {
        QueryPragmas pragmas = new QueryPragmas(Settings.EMPTY);
        int clusterDefault = between(1, 1_000_000);
        assertThat(pragmas.timeSeriesPartialAggregationEmitKeysThreshold(clusterDefault), equalTo(clusterDefault));
    }

    public void testTimeSeriesTargetChunkSizePragmaOverridesDefault() {
        QueryPragmas pragmas = new QueryPragmas(
            Settings.builder().put(PlannerSettings.TIME_SERIES_TARGET_CHUNK_SIZE.getKey(), 4_096).build()
        );
        assertThat(pragmas.timeSeriesTargetChunkSize(10_000), equalTo(4_096));
    }

    public void testTimeSeriesPragmasAreDecoupledFromRegularAggregationPragmas() {
        // Only the regular pragma is set: it reaches the regular accessor and leaves the time-series ones at their defaults.
        QueryPragmas regularOnly = new QueryPragmas(
            Settings.builder().put(PlannerSettings.PARTIAL_AGGREGATION_EMIT_KEYS_THRESHOLD.getKey(), 333).build()
        );
        assertThat(regularOnly.partialAggregationEmitKeysThreshold(100_000), equalTo(333));
        assertThat(regularOnly.timeSeriesPartialAggregationEmitKeysThreshold(100_000), equalTo(100_000));
        assertThat(regularOnly.timeSeriesTargetChunkSize(10_000), equalTo(10_000));

        // Only the time-series pragmas are set: they reach the time-series accessors and leave the regular one at its default.
        QueryPragmas timeSeriesOnly = new QueryPragmas(
            Settings.builder()
                .put(PlannerSettings.TIME_SERIES_PARTIAL_AGGREGATION_EMIT_KEYS_THRESHOLD.getKey(), 7)
                .put(PlannerSettings.TIME_SERIES_TARGET_CHUNK_SIZE.getKey(), 999)
                .build()
        );
        assertThat(timeSeriesOnly.timeSeriesPartialAggregationEmitKeysThreshold(100_000), equalTo(7));
        assertThat(timeSeriesOnly.timeSeriesTargetChunkSize(10_000), equalTo(999));
        assertThat(timeSeriesOnly.partialAggregationEmitKeysThreshold(100_000), equalTo(100_000));
    }
}
