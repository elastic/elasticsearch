/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class DataStreamLifecycleSettingsTests extends ESTestCase {

    public void testDefaults() {
        DataStreamLifecycleSettings dataStreamLifecycleSettings = DataStreamLifecycleSettings.create(
            ClusterSettings.createBuiltInClusterSettings(),
            Settings.EMPTY
        );

        assertThat(dataStreamLifecycleSettings.getDefaultRetention(), nullValue());
        assertThat(dataStreamLifecycleSettings.getMaxRetention(), nullValue());
        assertThat(dataStreamLifecycleSettings.getGlobalRetention(false), nullValue());
        assertThat(
            dataStreamLifecycleSettings.getGlobalRetention(true),
            equalTo(DataStreamGlobalRetention.create(TimeValue.timeValueDays(30), null))
        );
        assertThat(dataStreamLifecycleSettings.isDefaultLifecycleForTimeSeriesEnabled(), equalTo(true));
    }

    public void testMonitorsDefaultRetention() {
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings();
        DataStreamLifecycleSettings dataStreamLifecycleSettings = DataStreamLifecycleSettings.create(clusterSettings, Settings.EMPTY);

        // Test valid update
        TimeValue newDefaultRetention = TimeValue.timeValueDays(randomIntBetween(1, 10));
        Settings newSettings = Settings.builder()
            .put(DataStreamLifecycleSettings.DATA_STREAMS_DEFAULT_RETENTION_SETTING.getKey(), newDefaultRetention.toHumanReadableString(0))
            .build();
        clusterSettings.applySettings(newSettings);

        assertThat(dataStreamLifecycleSettings.getDefaultRetention(), equalTo(newDefaultRetention));
        assertThat(
            dataStreamLifecycleSettings.getGlobalRetention(false),
            equalTo(DataStreamGlobalRetention.create(newDefaultRetention, null))
        );

        // Test invalid update
        Settings newInvalidSettings = Settings.builder()
            .put(DataStreamLifecycleSettings.DATA_STREAMS_DEFAULT_RETENTION_SETTING.getKey(), TimeValue.ZERO)
            .build();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(newInvalidSettings)
        );
        assertThat(
            exception.getCause().getMessage(),
            containsString("Setting 'data_streams.lifecycle.retention.default' should be greater than")
        );
        assertThat(
            dataStreamLifecycleSettings.getGlobalRetention(false),
            equalTo(DataStreamGlobalRetention.create(newDefaultRetention, null))
        );
    }

    public void testMonitorsMaxRetention() {
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings();
        DataStreamLifecycleSettings dataStreamLifecycleSettings = DataStreamLifecycleSettings.create(clusterSettings, Settings.EMPTY);

        // Test valid update
        TimeValue newMaxRetention = TimeValue.timeValueDays(randomIntBetween(10, 29));
        Settings newSettings = Settings.builder()
            .put(DataStreamLifecycleSettings.DATA_STREAMS_MAX_RETENTION_SETTING.getKey(), newMaxRetention.toHumanReadableString(0))
            .build();
        clusterSettings.applySettings(newSettings);

        assertThat(dataStreamLifecycleSettings.getMaxRetention(), equalTo(newMaxRetention));
        assertThat(dataStreamLifecycleSettings.getGlobalRetention(false), equalTo(DataStreamGlobalRetention.create(null, newMaxRetention)));
        assertThat(dataStreamLifecycleSettings.getGlobalRetention(true), equalTo(DataStreamGlobalRetention.create(null, newMaxRetention)));

        newMaxRetention = TimeValue.timeValueDays(100);
        newSettings = Settings.builder()
            .put(DataStreamLifecycleSettings.DATA_STREAMS_MAX_RETENTION_SETTING.getKey(), newMaxRetention.toHumanReadableString(0))
            .build();
        clusterSettings.applySettings(newSettings);
        assertThat(
            dataStreamLifecycleSettings.getGlobalRetention(true),
            equalTo(DataStreamGlobalRetention.create(TimeValue.timeValueDays(30), newMaxRetention))
        );

        // Test invalid update
        Settings newInvalidSettings = Settings.builder()
            .put(DataStreamLifecycleSettings.DATA_STREAMS_MAX_RETENTION_SETTING.getKey(), TimeValue.ZERO)
            .build();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(newInvalidSettings)
        );
        assertThat(
            exception.getCause().getMessage(),
            containsString("Setting 'data_streams.lifecycle.retention.max' should be greater than")
        );
        assertThat(dataStreamLifecycleSettings.getGlobalRetention(false), equalTo(DataStreamGlobalRetention.create(null, newMaxRetention)));
    }

    public void testMonitorsDefaultFailuresRetention() {
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings();
        DataStreamLifecycleSettings dataStreamLifecycleSettings = DataStreamLifecycleSettings.create(clusterSettings, Settings.EMPTY);

        // Test valid update
        TimeValue newDefaultRetention = TimeValue.timeValueDays(randomIntBetween(1, 10));
        Settings newSettings = Settings.builder()
            .put(DataStreamLifecycleSettings.FAILURE_STORE_DEFAULT_RETENTION_SETTING.getKey(), newDefaultRetention.toHumanReadableString(0))
            .build();
        clusterSettings.applySettings(newSettings);

        assertThat(dataStreamLifecycleSettings.getDefaultRetention(true), equalTo(newDefaultRetention));
        assertThat(
            dataStreamLifecycleSettings.getGlobalRetention(true),
            equalTo(DataStreamGlobalRetention.create(newDefaultRetention, null))
        );

        // Test update default failures retention to infinite retention
        newDefaultRetention = TimeValue.MINUS_ONE;
        newSettings = Settings.builder()
            .put(DataStreamLifecycleSettings.FAILURE_STORE_DEFAULT_RETENTION_SETTING.getKey(), newDefaultRetention.toHumanReadableString(0))
            .build();
        clusterSettings.applySettings(newSettings);

        assertThat(dataStreamLifecycleSettings.getDefaultRetention(true), nullValue());
        assertThat(dataStreamLifecycleSettings.getGlobalRetention(true), nullValue());

        // Test invalid update
        Settings newInvalidSettings = Settings.builder()
            .put(DataStreamLifecycleSettings.FAILURE_STORE_DEFAULT_RETENTION_SETTING.getKey(), TimeValue.ZERO)
            .build();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(newInvalidSettings)
        );
        assertThat(
            exception.getCause().getMessage(),
            containsString("Setting 'data_streams.lifecycle.retention.failures_default' should be greater than")
        );
        assertThat(dataStreamLifecycleSettings.getGlobalRetention(true), nullValue());
    }

    public void testCombinationValidation() {
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings();
        DataStreamLifecycleSettings dataStreamLifecycleSettings = DataStreamLifecycleSettings.create(clusterSettings, Settings.EMPTY);

        // Test invalid update
        Settings newInvalidSettings = Settings.builder()
            .put(DataStreamLifecycleSettings.DATA_STREAMS_DEFAULT_RETENTION_SETTING.getKey(), TimeValue.timeValueDays(90))
            .put(DataStreamLifecycleSettings.DATA_STREAMS_MAX_RETENTION_SETTING.getKey(), TimeValue.timeValueDays(30))
            .build();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(newInvalidSettings)
        );
        assertThat(
            exception.getCause().getMessage(),
            containsString(
                "Setting [data_streams.lifecycle.retention.default=90d] cannot be greater than [data_streams.lifecycle.retention.max=30d]"
            )
        );

        // Test valid update even if the failures default is greater than max.
        Settings newValidSettings = Settings.builder()
            .put(DataStreamLifecycleSettings.FAILURE_STORE_DEFAULT_RETENTION_SETTING.getKey(), TimeValue.timeValueDays(90))
            .put(DataStreamLifecycleSettings.DATA_STREAMS_MAX_RETENTION_SETTING.getKey(), TimeValue.timeValueDays(30))
            .build();
        clusterSettings.applySettings(newValidSettings);
        assertThat(dataStreamLifecycleSettings.getDefaultRetention(true), equalTo(TimeValue.timeValueDays(90)));
        assertThat(
            dataStreamLifecycleSettings.getGlobalRetention(true),
            equalTo(DataStreamGlobalRetention.create(null, TimeValue.timeValueDays(30)))
        );
    }

    public void testDefaultLifecycleForTimeSeriesRespectiveToDlmOnly() {
        DataStreamLifecycleSettings dataStreamLifecycleSettings = DataStreamLifecycleSettings.create(
            ClusterSettings.createBuiltInClusterSettings(),
            Settings.builder().put(DataStreamLifecycle.DATA_STREAMS_LIFECYCLE_ONLY_SETTING_NAME, true).build()
        );
        // In DLM only mode all data streams have an initial lifecycle, there is no need for this flag.
        assertThat(dataStreamLifecycleSettings.isDefaultLifecycleForTimeSeriesEnabled(), equalTo(false));

        dataStreamLifecycleSettings = DataStreamLifecycleSettings.create(
            ClusterSettings.createBuiltInClusterSettings(),
            Settings.builder().put(DataStreamLifecycle.DATA_STREAMS_LIFECYCLE_ONLY_SETTING_NAME, false).build()
        );
        assertThat(dataStreamLifecycleSettings.isDefaultLifecycleForTimeSeriesEnabled(), equalTo(true));
    }

    public void testDefaultLifecycleForTimeSeriesSettingMonitored() {
        // Simulate ILM being loaded: register the setting in ClusterSettings.
        Set<Setting<?>> settingsSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingsSet.add(DataStreamLifecycleSettings.DEFAULT_LIFECYCLE_FOR_TIME_SERIES_SETTING);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, settingsSet);

        DataStreamLifecycleSettings dataStreamLifecycleSettings = DataStreamLifecycleSettings.create(clusterSettings, Settings.EMPTY);

        assertThat(dataStreamLifecycleSettings.isDefaultLifecycleForTimeSeriesEnabled(), equalTo(true));

        // Dynamically disable.
        clusterSettings.applySettings(
            Settings.builder().put(DataStreamLifecycleSettings.DEFAULT_LIFECYCLE_FOR_TIME_SERIES_SETTING_NAME, false).build()
        );
        assertThat(dataStreamLifecycleSettings.isDefaultLifecycleForTimeSeriesEnabled(), equalTo(false));

        // Dynamically re-enable.
        clusterSettings.applySettings(
            Settings.builder().put(DataStreamLifecycleSettings.DEFAULT_LIFECYCLE_FOR_TIME_SERIES_SETTING_NAME, true).build()
        );
        assertThat(dataStreamLifecycleSettings.isDefaultLifecycleForTimeSeriesEnabled(), equalTo(true));
    }
}
