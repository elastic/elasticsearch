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
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;

import java.util.HashSet;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the dedicated time-series planner setting ({@code esql.time_series.target_chunk_rows}): its default, that
 * it is registered as a cluster setting, and that it updates independently from the regular aggregation settings.
 */
public class PlannerSettingsTests extends ESTestCase {

    public void testTimeSeriesTargetChunkRowsDefault() {
        assertThat(PlannerSettings.DEFAULTS.timeSeriesTargetChunkRows(), equalTo(100_000));
    }

    public void testTimeSeriesTargetChunkRowsIsRegistered() {
        var registeredKeys = PlannerSettings.settings().stream().map(Setting::getKey).toList();
        assertThat(registeredKeys, hasItems(PlannerSettings.TIME_SERIES_TARGET_CHUNK_ROWS.getKey()));
    }

    public void testTimeSeriesTargetChunkRowsIsDecoupledFromRegularAggregationSettings() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(PlannerSettings.settings()));
        // ClusterService is mocked because the Holder only reads getClusterSettings(); standing up a real
        // ClusterService would require a ThreadPool and lifecycle management without adding coverage.
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        PlannerSettings.Holder holder = new PlannerSettings.Holder(clusterService);

        assertThat(holder.get().timeSeriesTargetChunkRows(), equalTo(100_000));

        // Update a regular aggregation knob and the time-series chunk rows in a single cluster-settings change.
        clusterSettings.applySettings(
            Settings.builder()
                .put(PlannerSettings.PARTIAL_AGGREGATION_EMIT_KEYS_THRESHOLD.getKey(), 12_345)
                .put(PlannerSettings.TIME_SERIES_TARGET_CHUNK_ROWS.getKey(), 999)
                .build()
        );

        PlannerSettings updated = holder.get();
        assertThat(updated.partialEmitKeysThreshold(), equalTo(12_345));
        assertThat("the time-series chunk rows updates independently", updated.timeSeriesTargetChunkRows(), equalTo(999));
    }

    public void testLoadAllMaxFieldsDefault() {
        assertThat(PlannerSettings.DEFAULTS.loadAllMaxFields(), equalTo(1000));
    }

    /** The setting is only exposed where its capability is, so that it is not exposed where it has no effect. */
    public void testLoadAllMaxFieldsIsRegisteredExactlyWhereItsCapabilityIs() {
        var registeredKeys = PlannerSettings.settings().stream().map(Setting::getKey).toList();
        assertThat(
            registeredKeys.contains(PlannerSettings.LOAD_ALL_MAX_FIELDS.getKey()),
            equalTo(EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL_MAX_FIELDS_SETTING.isEnabled())
        );
    }

    /**
     * Runs the update the way {@code PUT _cluster/settings} does, which is where an update of a setting that is not dynamic is
     * rejected: {@code ClusterSettings#updateDynamicSettings}. No cluster service is needed for that.
     */
    public void testLoadAllMaxFieldsIsDynamic() {
        assumeTrue("the setting is registered", EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL_MAX_FIELDS_SETTING.isEnabled());
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(PlannerSettings.settings()));
        String key = PlannerSettings.LOAD_ALL_MAX_FIELDS.getKey();

        Settings.Builder target = Settings.builder();
        Settings.Builder updates = Settings.builder();
        assertTrue(clusterSettings.updateDynamicSettings(Settings.builder().put(key, 2500).build(), target, updates, "persistent"));
        assertThat(updates.build().get(key), equalTo("2500"));
    }

    public void testLoadAllMaxFieldsBounds() {
        String key = PlannerSettings.LOAD_ALL_MAX_FIELDS.getKey();
        assertThat(PlannerSettings.LOAD_ALL_MAX_FIELDS.get(Settings.builder().put(key, 0).build()), equalTo(0));
        assertThat(PlannerSettings.LOAD_ALL_MAX_FIELDS.get(Settings.builder().put(key, 100_000).build()), equalTo(100_000));

        IllegalArgumentException tooSmall = expectThrows(
            IllegalArgumentException.class,
            () -> PlannerSettings.LOAD_ALL_MAX_FIELDS.get(Settings.builder().put(key, -1).build())
        );
        assertThat(tooSmall.getMessage(), containsString("must be >= 0"));
        IllegalArgumentException tooBig = expectThrows(
            IllegalArgumentException.class,
            () -> PlannerSettings.LOAD_ALL_MAX_FIELDS.get(Settings.builder().put(key, 100_001).build())
        );
        assertThat(tooBig.getMessage(), containsString("must be <= 100000"));
    }
}
