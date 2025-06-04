/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.exception.ResourceAlreadyExistsException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class LookAHeadTimeTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(DataStreamsPlugin.class);
    }

    @After
    public void tearDown() throws Exception {
        updateClusterSettings(Settings.builder().put(DataStreamsPlugin.TIME_SERIES_POLL_INTERVAL.getKey(), (String) null).build());
        super.tearDown();
    }

    public void testTimeSeriesPollIntervalSetting() {
        var settings = Settings.builder().put(DataStreamsPlugin.TIME_SERIES_POLL_INTERVAL.getKey(), "9m").build();
        updateClusterSettings(settings);
    }

    public void testTimeSeriesPollIntervalSettingToLow() {
        var settings = Settings.builder().put(DataStreamsPlugin.TIME_SERIES_POLL_INTERVAL.getKey(), "1s").build();
        var e = expectThrows(IllegalArgumentException.class, () -> updateClusterSettings(settings));
        assertThat(e.getMessage(), equalTo("failed to parse value [1s] for setting [time_series.poll_interval], must be >= [1m]"));
    }

    public void testTimeSeriesPollIntervalSettingToHigh() {
        Settings settings = Settings.builder().put(DataStreamsPlugin.TIME_SERIES_POLL_INTERVAL.getKey(), "11m").build();
        var e = expectThrows(IllegalArgumentException.class, () -> updateClusterSettings(settings));
        assertThat(e.getMessage(), equalTo("failed to parse value [11m] for setting [time_series.poll_interval], must be <= [10m]"));
    }

    public void testLookAheadTimeSetting() {
        var settings = Settings.builder().put(DataStreamsPlugin.LOOK_AHEAD_TIME.getKey(), "10m").build();
        updateIndexSettings(settings);
    }

    public void testLookAheadTimeSettingToLow() {
        var settings = Settings.builder().put(DataStreamsPlugin.LOOK_AHEAD_TIME.getKey(), "1s").build();
        var e = expectThrows(IllegalArgumentException.class, () -> updateIndexSettings(settings));
        assertThat(e.getMessage(), equalTo("failed to parse value [1s] for setting [index.look_ahead_time], must be >= [1m]"));
    }

    public void testLookAheadTimeSettingToHigh() {
        var settings = Settings.builder().put(DataStreamsPlugin.LOOK_AHEAD_TIME.getKey(), "8d").build();
        var e = expectThrows(IllegalArgumentException.class, () -> updateIndexSettings(settings));
        assertThat(e.getMessage(), equalTo("failed to parse value [8d] for setting [index.look_ahead_time], must be <= [7d]"));
    }

    public void testLookBackTimeSettingToLow() {
        var settings = Settings.builder().put(DataStreamsPlugin.LOOK_BACK_TIME.getKey(), "1s").build();
        var e = expectThrows(IllegalArgumentException.class, () -> updateIndexSettings(settings));
        assertThat(e.getMessage(), equalTo("failed to parse value [1s] for setting [index.look_back_time], must be >= [1m]"));
    }

    public void testLookBackTimeSettingToHigh() {
        var settings = Settings.builder().put(DataStreamsPlugin.LOOK_BACK_TIME.getKey(), "8d").build();
        var e = expectThrows(IllegalArgumentException.class, () -> updateIndexSettings(settings));
        assertThat(e.getMessage(), equalTo("failed to parse value [8d] for setting [index.look_back_time], must be <= [7d]"));
    }

    public void testLookAheadTimeSettingLowerThanTimeSeriesPollIntervalSetting() {
        {
            var settings = Settings.builder()
                .put(DataStreamsPlugin.LOOK_AHEAD_TIME.getKey(), "3m")
                // default time_series.poll_interval is 5m
                .build();
            var e = expectThrows(IllegalArgumentException.class, () -> updateIndexSettings(settings));
            assertThat(
                e.getMessage(),
                equalTo(
                    "failed to parse value [3m] for setting [index.look_ahead_time], must be lower than setting "
                        + "[time_series.poll_interval] which is [5m]"
                )
            );
        }
        {
            var clusterSettings = Settings.builder().put(DataStreamsPlugin.TIME_SERIES_POLL_INTERVAL.getKey(), "3m").build();
            updateClusterSettings(clusterSettings);
            var indexSettings = Settings.builder().put(DataStreamsPlugin.LOOK_AHEAD_TIME.getKey(), "1m").build();
            var e = expectThrows(IllegalArgumentException.class, () -> updateIndexSettings(indexSettings));
            assertThat(
                e.getMessage(),
                equalTo(
                    "failed to parse value [1m] for setting [index.look_ahead_time], must be lower than setting "
                        + "[time_series.poll_interval] which is [3m]"
                )
            );
        }
    }

    public void testLookAheadTimeSettingHigherThanTimeSeriesPollIntervalSetting() {
        var clusterSettings = Settings.builder().put(DataStreamsPlugin.TIME_SERIES_POLL_INTERVAL.getKey(), "10m").build();
        updateClusterSettings(clusterSettings);
        var indexSettings = Settings.builder().put(DataStreamsPlugin.LOOK_AHEAD_TIME.getKey(), "100m").build();
        updateIndexSettings(indexSettings);
    }

    private void updateClusterSettings(Settings settings) {
        clusterAdmin().updateSettings(
            new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).persistentSettings(settings)
        ).actionGet();
    }

    private void updateIndexSettings(Settings settings) {
        try {
            createIndex("test");
        } catch (ResourceAlreadyExistsException e) {
            // ignore
        }
        indicesAdmin().updateSettings(new UpdateSettingsRequest(settings)).actionGet();
    }

}
