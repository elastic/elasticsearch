/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
            ClusterSettings.createBuiltInClusterSettings(),
            DataStreamFactoryRetention.emptyFactoryRetention()
        );

        assertThat(globalRetentionSettings.getDefaultRetention(), nullValue());
        assertThat(globalRetentionSettings.getMaxRetention(), nullValue());

        // Fallback to factory settings
        TimeValue maxFactoryValue = randomPositiveTimeValue();
        TimeValue defaultFactoryValue = randomPositiveTimeValue();
        DataStreamGlobalRetentionSettings withFactorySettings = DataStreamGlobalRetentionSettings.create(
            ClusterSettings.createBuiltInClusterSettings(),
            new DataStreamFactoryRetention() {
                @Override
                public TimeValue getMaxRetention() {
                    return maxFactoryValue;
                }

                @Override
                public TimeValue getDefaultRetention() {
                    return defaultFactoryValue;
                }

                @Override
                public void init(ClusterSettings clusterSettings) {

                }
            }
        );

        assertThat(withFactorySettings.getDefaultRetention(), equalTo(defaultFactoryValue));
        assertThat(withFactorySettings.getMaxRetention(), equalTo(maxFactoryValue));
    }

    public void testMonitorsDefaultRetention() {
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings();
        DataStreamGlobalRetentionSettings globalRetentionSettings = DataStreamGlobalRetentionSettings.create(
            clusterSettings,
            DataStreamFactoryRetention.emptyFactoryRetention()
        );

        // Test valid update
        TimeValue newDefaultRetention = TimeValue.timeValueDays(randomIntBetween(1, 10));
        Settings newSettings = Settings.builder()
            .put(
                DataStreamGlobalRetentionSettings.DATA_STREAMS_DEFAULT_RETENTION_SETTING.getKey(),
                newDefaultRetention.toHumanReadableString(0)
            )
            .build();
        clusterSettings.applySettings(newSettings);

        assertThat(newDefaultRetention, equalTo(globalRetentionSettings.getDefaultRetention()));

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
    }

    public void testMonitorsMaxRetention() {
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings();
        DataStreamGlobalRetentionSettings globalRetentionSettings = DataStreamGlobalRetentionSettings.create(
            clusterSettings,
            DataStreamFactoryRetention.emptyFactoryRetention()
        );

        // Test valid update
        TimeValue newMaxRetention = TimeValue.timeValueDays(randomIntBetween(10, 30));
        Settings newSettings = Settings.builder()
            .put(DataStreamGlobalRetentionSettings.DATA_STREAMS_MAX_RETENTION_SETTING.getKey(), newMaxRetention.toHumanReadableString(0))
            .build();
        clusterSettings.applySettings(newSettings);

        assertThat(newMaxRetention, equalTo(globalRetentionSettings.getMaxRetention()));

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
    }

    public void testCombinationValidation() {
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings();
        DataStreamGlobalRetentionSettings.create(clusterSettings, DataStreamFactoryRetention.emptyFactoryRetention());

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
    }
}
