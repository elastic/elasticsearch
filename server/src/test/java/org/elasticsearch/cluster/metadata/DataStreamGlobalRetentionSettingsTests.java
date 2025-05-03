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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class DataStreamGlobalRetentionSettingsTests extends ESTestCase {

    public void testDefaults() {
        DataStreamGlobalRetentionSettings globalRetentionSettings = DataStreamGlobalRetentionSettings.create(
            ClusterSettings.createBuiltInClusterSettings()
        );

        assertThat(globalRetentionSettings.getDefaultRetention(), nullValue());
        assertThat(globalRetentionSettings.getMaxRetention(), nullValue());
        assertThat(globalRetentionSettings.get(false), nullValue());
        assertThat(globalRetentionSettings.get(true), equalTo(DataStreamGlobalRetention.create(TimeValue.timeValueDays(30), null)));
    }

    public void testMonitorsDefaultRetention() {
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings();
        DataStreamGlobalRetentionSettings globalRetentionSettings = DataStreamGlobalRetentionSettings.create(clusterSettings);

        // Test valid update
        TimeValue newDefaultRetention = TimeValue.timeValueDays(randomIntBetween(1, 10));
        Settings newSettings = Settings.builder()
            .put(
                DataStreamGlobalRetentionSettings.DATA_STREAMS_DEFAULT_RETENTION_SETTING.getKey(),
                newDefaultRetention.toHumanReadableString(0)
            )
            .build();
        clusterSettings.applySettings(newSettings);

        assertThat(globalRetentionSettings.getDefaultRetention(), equalTo(newDefaultRetention));
        assertThat(globalRetentionSettings.get(false), equalTo(DataStreamGlobalRetention.create(newDefaultRetention, null)));

        // Test invalid update
        Settings newInvalidSettings = Settings.builder()
            .put(DataStreamGlobalRetentionSettings.DATA_STREAMS_DEFAULT_RETENTION_SETTING.getKey(), TimeValue.ZERO)
            .build();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(newInvalidSettings)
        );
        assertThat(
            exception.getCause().getMessage(),
            containsString("Setting 'data_streams.lifecycle.retention.default' should be greater than")
        );
        assertThat(globalRetentionSettings.get(false), equalTo(DataStreamGlobalRetention.create(newDefaultRetention, null)));
    }

    public void testMonitorsMaxRetention() {
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings();
        DataStreamGlobalRetentionSettings globalRetentionSettings = DataStreamGlobalRetentionSettings.create(clusterSettings);

        // Test valid update
        TimeValue newMaxRetention = TimeValue.timeValueDays(randomIntBetween(10, 29));
        Settings newSettings = Settings.builder()
            .put(DataStreamGlobalRetentionSettings.DATA_STREAMS_MAX_RETENTION_SETTING.getKey(), newMaxRetention.toHumanReadableString(0))
            .build();
        clusterSettings.applySettings(newSettings);

        assertThat(globalRetentionSettings.getMaxRetention(), equalTo(newMaxRetention));
        assertThat(globalRetentionSettings.get(false), equalTo(DataStreamGlobalRetention.create(null, newMaxRetention)));
        assertThat(globalRetentionSettings.get(true), equalTo(DataStreamGlobalRetention.create(null, newMaxRetention)));

        newMaxRetention = TimeValue.timeValueDays(100);
        newSettings = Settings.builder()
            .put(DataStreamGlobalRetentionSettings.DATA_STREAMS_MAX_RETENTION_SETTING.getKey(), newMaxRetention.toHumanReadableString(0))
            .build();
        clusterSettings.applySettings(newSettings);
        assertThat(
            globalRetentionSettings.get(true),
            equalTo(DataStreamGlobalRetention.create(TimeValue.timeValueDays(30), newMaxRetention))
        );

        // Test invalid update
        Settings newInvalidSettings = Settings.builder()
            .put(DataStreamGlobalRetentionSettings.DATA_STREAMS_MAX_RETENTION_SETTING.getKey(), TimeValue.ZERO)
            .build();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(newInvalidSettings)
        );
        assertThat(
            exception.getCause().getMessage(),
            containsString("Setting 'data_streams.lifecycle.retention.max' should be greater than")
        );
        assertThat(globalRetentionSettings.get(false), equalTo(DataStreamGlobalRetention.create(null, newMaxRetention)));
    }

    public void testMonitorsDefaultFailuresRetention() {
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings();
        DataStreamGlobalRetentionSettings globalRetentionSettings = DataStreamGlobalRetentionSettings.create(clusterSettings);

        // Test valid update
        TimeValue newDefaultRetention = TimeValue.timeValueDays(randomIntBetween(1, 10));
        Settings newSettings = Settings.builder()
            .put(
                DataStreamGlobalRetentionSettings.FAILURE_STORE_DEFAULT_RETENTION_SETTING.getKey(),
                newDefaultRetention.toHumanReadableString(0)
            )
            .build();
        clusterSettings.applySettings(newSettings);

        assertThat(globalRetentionSettings.getDefaultRetention(true), equalTo(newDefaultRetention));
        assertThat(globalRetentionSettings.get(true), equalTo(DataStreamGlobalRetention.create(newDefaultRetention, null)));

        // Test update default failures retention to infinite retention
        newDefaultRetention = TimeValue.MINUS_ONE;
        newSettings = Settings.builder()
            .put(
                DataStreamGlobalRetentionSettings.FAILURE_STORE_DEFAULT_RETENTION_SETTING.getKey(),
                newDefaultRetention.toHumanReadableString(0)
            )
            .build();
        clusterSettings.applySettings(newSettings);

        assertThat(globalRetentionSettings.getDefaultRetention(true), nullValue());
        assertThat(globalRetentionSettings.get(true), nullValue());

        // Test invalid update
        Settings newInvalidSettings = Settings.builder()
            .put(DataStreamGlobalRetentionSettings.FAILURE_STORE_DEFAULT_RETENTION_SETTING.getKey(), TimeValue.ZERO)
            .build();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(newInvalidSettings)
        );
        assertThat(
            exception.getCause().getMessage(),
            containsString("Setting 'data_streams.lifecycle.retention.failures_default' should be greater than")
        );
        assertThat(globalRetentionSettings.get(true), nullValue());
    }

    public void testCombinationValidation() {
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings();
        DataStreamGlobalRetentionSettings dataStreamGlobalRetentionSettings = DataStreamGlobalRetentionSettings.create(clusterSettings);

        // Test invalid update
        Settings newInvalidSettings = Settings.builder()
            .put(DataStreamGlobalRetentionSettings.DATA_STREAMS_DEFAULT_RETENTION_SETTING.getKey(), TimeValue.timeValueDays(90))
            .put(DataStreamGlobalRetentionSettings.DATA_STREAMS_MAX_RETENTION_SETTING.getKey(), TimeValue.timeValueDays(30))
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
            .put(DataStreamGlobalRetentionSettings.FAILURE_STORE_DEFAULT_RETENTION_SETTING.getKey(), TimeValue.timeValueDays(90))
            .put(DataStreamGlobalRetentionSettings.DATA_STREAMS_MAX_RETENTION_SETTING.getKey(), TimeValue.timeValueDays(30))
            .build();
        clusterSettings.applySettings(newValidSettings);
        assertThat(dataStreamGlobalRetentionSettings.getDefaultRetention(true), equalTo(TimeValue.timeValueDays(90)));
        assertThat(
            dataStreamGlobalRetentionSettings.get(true),
            equalTo(DataStreamGlobalRetention.create(null, TimeValue.timeValueDays(30)))
        );
    }
}
