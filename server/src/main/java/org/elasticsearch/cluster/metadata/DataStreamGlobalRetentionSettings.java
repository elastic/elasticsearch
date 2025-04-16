/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This class holds the data stream global retention settings. It defines, validates and monitors the settings.
 * <p>
 * The global retention settings apply to non-system data streams that are managed by the data stream lifecycle. They consist of:
 * - The default retention which applies to data streams that do not have a retention defined.
 * - The max retention which applies to all data streams that do not have retention or their retention has exceeded this value.
 * - The failure store default retention which applies only to the failure indices of data streams that have no failure retention defined.
 */
public class DataStreamGlobalRetentionSettings {

    private static final Logger logger = LogManager.getLogger(DataStreamGlobalRetentionSettings.class);
    public static final TimeValue MIN_RETENTION_VALUE = TimeValue.timeValueSeconds(10);

    public static final Setting<TimeValue> DATA_STREAMS_DEFAULT_RETENTION_SETTING = Setting.timeSetting(
        "data_streams.lifecycle.retention.default",
        TimeValue.MINUS_ONE,
        new Setting.Validator<>() {
            @Override
            public void validate(TimeValue value) {}

            @Override
            public void validate(final TimeValue settingValue, final Map<Setting<?>, Object> settings) {
                TimeValue defaultRetention = getSettingValueOrNull(settingValue);
                TimeValue maxRetention = getSettingValueOrNull((TimeValue) settings.get(DATA_STREAMS_MAX_RETENTION_SETTING));
                validateIsolatedRetentionValue(defaultRetention, DATA_STREAMS_DEFAULT_RETENTION_SETTING.getKey());
                validateGlobalRetentionConfiguration(defaultRetention, maxRetention);
            }

            @Override
            public Iterator<Setting<?>> settings() {
                final List<Setting<?>> settings = List.of(DATA_STREAMS_MAX_RETENTION_SETTING);
                return settings.iterator();
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> FAILURE_STORE_DEFAULT_RETENTION_SETTING = Setting.timeSetting(
        "data_streams.lifecycle.retention.failures_default",
        DataStreamGlobalRetention.FAILURES_DEFAULT_VALUE,
        new Setting.Validator<>() {
            @Override
            public void validate(TimeValue value) {}

            @Override
            public void validate(final TimeValue settingValue, final Map<Setting<?>, Object> settings) {
                TimeValue defaultRetention = getSettingValueOrNull(settingValue);
                // Currently, we do not validate the default for the failure store against the max because
                // we start with a default value that might conflict the max retention.
                validateIsolatedRetentionValue(defaultRetention, FAILURE_STORE_DEFAULT_RETENTION_SETTING.getKey());
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> DATA_STREAMS_MAX_RETENTION_SETTING = Setting.timeSetting(
        "data_streams.lifecycle.retention.max",
        TimeValue.MINUS_ONE,
        new Setting.Validator<>() {
            @Override
            public void validate(TimeValue value) {}

            @Override
            public void validate(final TimeValue settingValue, final Map<Setting<?>, Object> settings) {
                TimeValue defaultRetention = getSettingValueOrNull((TimeValue) settings.get(DATA_STREAMS_DEFAULT_RETENTION_SETTING));
                TimeValue maxRetention = getSettingValueOrNull(settingValue);
                validateIsolatedRetentionValue(maxRetention, DATA_STREAMS_MAX_RETENTION_SETTING.getKey());
                validateGlobalRetentionConfiguration(defaultRetention, maxRetention);
            }

            @Override
            public Iterator<Setting<?>> settings() {
                final List<Setting<?>> settings = List.of(DATA_STREAMS_DEFAULT_RETENTION_SETTING);
                return settings.iterator();
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    @Nullable
    private volatile TimeValue defaultRetention;
    @Nullable
    private volatile TimeValue maxRetention;
    @Nullable
    volatile TimeValue failuresDefaultRetention;

    private DataStreamGlobalRetentionSettings() {

    }

    @Nullable
    public TimeValue getMaxRetention() {
        return maxRetention;
    }

    @Nullable
    public TimeValue getDefaultRetention() {
        return defaultRetention;
    }

    @Nullable
    public TimeValue getFailuresDefaultRetention() {
        return failuresDefaultRetention;
    }

    public boolean areDefined() {
        return getDefaultRetention() != null || getMaxRetention() != null || getFailuresDefaultRetention() != null;
    }

    /**
     * Creates an instance and initialises the cluster settings listeners
     * @param clusterSettings it will register the cluster settings listeners to monitor for changes
     */
    public static DataStreamGlobalRetentionSettings create(ClusterSettings clusterSettings) {
        DataStreamGlobalRetentionSettings dataStreamGlobalRetentionSettings = new DataStreamGlobalRetentionSettings();
        clusterSettings.initializeAndWatch(DATA_STREAMS_DEFAULT_RETENTION_SETTING, dataStreamGlobalRetentionSettings::setDefaultRetention);
        clusterSettings.initializeAndWatch(DATA_STREAMS_MAX_RETENTION_SETTING, dataStreamGlobalRetentionSettings::setMaxRetention);
        clusterSettings.initializeAndWatch(
            FAILURE_STORE_DEFAULT_RETENTION_SETTING,
            dataStreamGlobalRetentionSettings::setFailuresDefaultRetention
        );
        return dataStreamGlobalRetentionSettings;
    }

    private void setMaxRetention(TimeValue maxRetention) {
        this.maxRetention = getSettingValueOrNull(maxRetention);
        logger.info("Updated global max retention to [{}]", this.maxRetention == null ? null : maxRetention.getStringRep());
    }

    private void setDefaultRetention(TimeValue defaultRetention) {
        this.defaultRetention = getSettingValueOrNull(defaultRetention);
        logger.info("Updated global default retention to [{}]", this.defaultRetention == null ? null : defaultRetention.getStringRep());
    }

    private void setFailuresDefaultRetention(TimeValue failuresDefaultRetention) {
        this.failuresDefaultRetention = getSettingValueOrNull(failuresDefaultRetention);
        logger.info(
            "Updated failures default retention to [{}]",
            this.failuresDefaultRetention == null ? null : failuresDefaultRetention.getStringRep()
        );
    }

    private static void validateIsolatedRetentionValue(@Nullable TimeValue retention, String settingName) {
        if (retention != null && retention.getMillis() < MIN_RETENTION_VALUE.getMillis()) {
            throw new IllegalArgumentException(
                "Setting '" + settingName + "' should be greater than " + MIN_RETENTION_VALUE.getStringRep()
            );
        }
    }

    private static void validateGlobalRetentionConfiguration(@Nullable TimeValue defaultRetention, @Nullable TimeValue maxRetention) {
        if (defaultRetention != null && maxRetention != null && defaultRetention.getMillis() > maxRetention.getMillis()) {
            throw new IllegalArgumentException(
                "Setting ["
                    + DATA_STREAMS_DEFAULT_RETENTION_SETTING.getKey()
                    + "="
                    + defaultRetention.getStringRep()
                    + "] cannot be greater than ["
                    + DATA_STREAMS_MAX_RETENTION_SETTING.getKey()
                    + "="
                    + maxRetention.getStringRep()
                    + "]."
            );
        }
    }

    @Nullable
    public DataStreamGlobalRetention get() {
        if (areDefined() == false) {
            return null;
        }
        return new DataStreamGlobalRetention(getDefaultRetention(), getMaxRetention(), getFailuresDefaultRetention());
    }

    /**
     * Time value settings do not accept null as a value. To represent an undefined retention as a setting we use the value
     * of <code>-1</code> and this method converts this to null.
     *
     * @param value the retention as parsed from the setting
     * @return the value when it is not -1 and null otherwise
     */
    @Nullable
    private static TimeValue getSettingValueOrNull(TimeValue value) {
        return value == null || value.equals(TimeValue.MINUS_ONE) ? null : value;
    }
}
