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
import static org.hamcrest.Matchers.hasItem;

/**
 * Verifies that the time-series target-chunk-rows pragma is read independently from the regular aggregation pragmas,
 * keeping the time-series chunking knob fully decoupled.
 */
public class QueryPragmasTests extends ESTestCase {

    public void testTimeSeriesTargetChunkRowsPragmaOverridesDefault() {
        QueryPragmas pragmas = new QueryPragmas(
            Settings.builder().put(PlannerSettings.TIME_SERIES_TARGET_CHUNK_ROWS.getKey(), 4_096).build()
        );
        assertThat(pragmas.timeSeriesTargetChunkRows(10_000), equalTo(4_096));
    }

    public void testTimeSeriesTargetChunkRowsIsValidPragmaName() {
        assertThat(QueryPragmas.VALID_PRAGMA_NAMES, hasItem(PlannerSettings.TIME_SERIES_TARGET_CHUNK_ROWS.getKey()));
    }

    public void testTimeSeriesTargetChunkRowsPragmaFallsBackToProvidedDefault() {
        QueryPragmas pragmas = new QueryPragmas(Settings.EMPTY);
        int clusterDefault = between(1, 1_000_000);
        assertThat(pragmas.timeSeriesTargetChunkRows(clusterDefault), equalTo(clusterDefault));
    }

    public void testTimeSeriesPragmaIsDecoupledFromRegularAggregationPragmas() {
        // Only the regular pragma is set: it reaches the regular accessor and leaves the time-series one at its default.
        QueryPragmas regularOnly = new QueryPragmas(
            Settings.builder().put(PlannerSettings.PARTIAL_AGGREGATION_EMIT_KEYS_THRESHOLD.getKey(), 333).build()
        );
        assertThat(regularOnly.partialAggregationEmitKeysThreshold(100_000), equalTo(333));
        assertThat(regularOnly.timeSeriesTargetChunkRows(10_000), equalTo(10_000));

        // Only the time-series pragma is set: it reaches the time-series accessor and leaves the regular one at its default.
        QueryPragmas timeSeriesOnly = new QueryPragmas(
            Settings.builder().put(PlannerSettings.TIME_SERIES_TARGET_CHUNK_ROWS.getKey(), 999).build()
        );
        assertThat(timeSeriesOnly.timeSeriesTargetChunkRows(10_000), equalTo(999));
        assertThat(timeSeriesOnly.partialAggregationEmitKeysThreshold(100_000), equalTo(100_000));
    }
}
