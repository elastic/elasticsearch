/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.TimeSeriesAggregationOperator;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the dedicated time-series planner settings ({@code esql.time_series.*}): their defaults preserve the
 * pre-decoupling chunking behaviour, they are registered as cluster settings, and they update independently from
 * the regular aggregation settings.
 */
public class PlannerSettingsTests extends ESTestCase {

    public void testTimeSeriesDefaults() {
        PlannerSettings defaults = PlannerSettings.DEFAULTS;
        // Emit cadence reuses the regular-aggregation defaults.
        assertThat(defaults.timeSeriesPartialEmitKeysThreshold(), equalTo(100_000));
        assertThat(defaults.timeSeriesPartialEmitUniquenessThreshold(), equalTo(0.1));
        // Chunk size defaults to the emit-keys threshold; the hard ceiling is 2x that target.
        assertThat(defaults.timeSeriesTargetChunkSize(), equalTo(100_000));
        assertThat(TimeSeriesAggregationOperator.Factory.maxChunkRowsFor(defaults.timeSeriesTargetChunkSize()), equalTo(200_000));
    }

    public void testTimeSeriesSettingsAreRegistered() {
        var registeredKeys = PlannerSettings.settings().stream().map(Setting::getKey).toList();
        assertThat(
            registeredKeys,
            hasItems(
                PlannerSettings.TIME_SERIES_PARTIAL_AGGREGATION_EMIT_KEYS_THRESHOLD.getKey(),
                PlannerSettings.TIME_SERIES_PARTIAL_AGGREGATION_EMIT_UNIQUENESS_THRESHOLD.getKey(),
                PlannerSettings.TIME_SERIES_TARGET_CHUNK_SIZE.getKey()
            )
        );
    }

    public void testTimeSeriesSettingsAreDecoupledFromRegularAggregationSettings() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(PlannerSettings.settings()));
        // ClusterService is mocked because the Holder only reads getClusterSettings(); standing up a real
        // ClusterService would require a ThreadPool and lifecycle management without adding coverage.
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        PlannerSettings.Holder holder = new PlannerSettings.Holder(clusterService);

        assertThat(holder.get().timeSeriesTargetChunkSize(), equalTo(100_000));

        // Update one regular knob and two time-series knobs in a single cluster-settings change.
        clusterSettings.applySettings(
            Settings.builder()
                .put(PlannerSettings.PARTIAL_AGGREGATION_EMIT_KEYS_THRESHOLD.getKey(), 12_345)
                .put(PlannerSettings.TIME_SERIES_PARTIAL_AGGREGATION_EMIT_KEYS_THRESHOLD.getKey(), 7)
                .put(PlannerSettings.TIME_SERIES_TARGET_CHUNK_SIZE.getKey(), 999)
                .build()
        );

        PlannerSettings updated = holder.get();
        assertThat(updated.partialEmitKeysThreshold(), equalTo(12_345));
        assertThat(updated.timeSeriesPartialEmitKeysThreshold(), equalTo(7));
        assertThat(updated.timeSeriesTargetChunkSize(), equalTo(999));
        // A time-series knob whose key was not set keeps its default — the regular update did not bleed into it.
        assertThat(updated.timeSeriesPartialEmitUniquenessThreshold(), equalTo(0.1));
    }
}
