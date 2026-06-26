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
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the dedicated time-series planner setting ({@code esql.time_series.target_chunk_size}): its default, that
 * it is registered as a cluster setting, and that it updates independently from the regular aggregation settings.
 */
public class PlannerSettingsTests extends ESTestCase {

    public void testTimeSeriesTargetChunkSizeDefault() {
        assertThat(PlannerSettings.DEFAULTS.timeSeriesTargetChunkSize(), equalTo(100_000));
    }

    public void testTimeSeriesTargetChunkSizeIsRegistered() {
        var registeredKeys = PlannerSettings.settings().stream().map(Setting::getKey).toList();
        assertThat(registeredKeys, hasItems(PlannerSettings.TIME_SERIES_TARGET_CHUNK_SIZE.getKey()));
    }

    public void testTimeSeriesTargetChunkSizeIsDecoupledFromRegularAggregationSettings() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(PlannerSettings.settings()));
        // ClusterService is mocked because the Holder only reads getClusterSettings(); standing up a real
        // ClusterService would require a ThreadPool and lifecycle management without adding coverage.
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        PlannerSettings.Holder holder = new PlannerSettings.Holder(clusterService);

        assertThat(holder.get().timeSeriesTargetChunkSize(), equalTo(100_000));

        // Update a regular aggregation knob and the time-series chunk size in a single cluster-settings change.
        clusterSettings.applySettings(
            Settings.builder()
                .put(PlannerSettings.PARTIAL_AGGREGATION_EMIT_KEYS_THRESHOLD.getKey(), 12_345)
                .put(PlannerSettings.TIME_SERIES_TARGET_CHUNK_SIZE.getKey(), 999)
                .build()
        );

        PlannerSettings updated = holder.get();
        assertThat(updated.partialEmitKeysThreshold(), equalTo(12_345));
        assertThat("the time-series chunk size updates independently", updated.timeSeriesTargetChunkSize(), equalTo(999));
    }
}
