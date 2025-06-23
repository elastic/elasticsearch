/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.List;

public class RetrySettings {

    static final Setting<TimeValue> RETRY_INITIAL_DELAY_SETTING = Setting.timeSetting(
        "xpack.inference.http.retry.initial_delay",
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    static final Setting<TimeValue> RETRY_MAX_DELAY_BOUND_SETTING = Setting.timeSetting(
        "xpack.inference.http.retry.max_delay_bound",
        TimeValue.timeValueSeconds(5),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    static final Setting<TimeValue> RETRY_TIMEOUT_SETTING = Setting.timeSetting(
        "xpack.inference.http.retry.timeout",
        TimeValue.timeValueSeconds(30),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    static final Setting<DebugFrequencyMode> RETRY_DEBUG_FREQUENCY_MODE_SETTING = Setting.enumSetting(
        DebugFrequencyMode.class,
        "xpack.inference.http.retry.debug_frequency_mode",
        DebugFrequencyMode.OFF,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    static final Setting<TimeValue> RETRY_DEBUG_FREQUENCY_AMOUNT_SETTING = Setting.timeSetting(
        "xpack.inference.http.retry.debug_frequency_amount",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile TimeValue initialDelay;
    private volatile TimeValue maxDelayBound;
    private volatile TimeValue timeout;
    private volatile DebugFrequencyMode debugMode;
    private volatile TimeValue debugFrequency;

    public RetrySettings(Settings settings, ClusterService clusterService) {
        initialDelay = RETRY_INITIAL_DELAY_SETTING.get(settings);
        maxDelayBound = RETRY_MAX_DELAY_BOUND_SETTING.get(settings);
        timeout = RETRY_TIMEOUT_SETTING.get(settings);
        debugMode = RETRY_DEBUG_FREQUENCY_MODE_SETTING.get(settings);
        debugFrequency = RETRY_DEBUG_FREQUENCY_AMOUNT_SETTING.get(settings);

        addSettingsUpdateConsumers(clusterService);
    }

    private void addSettingsUpdateConsumers(ClusterService clusterService) {
        clusterService.getClusterSettings().addSettingsUpdateConsumer(RETRY_INITIAL_DELAY_SETTING, this::setInitialDelay);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(RETRY_MAX_DELAY_BOUND_SETTING, this::setMaxDelayBound);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(RETRY_TIMEOUT_SETTING, this::setTimeout);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(RETRY_DEBUG_FREQUENCY_MODE_SETTING, this::setDebugMode);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(RETRY_DEBUG_FREQUENCY_AMOUNT_SETTING, this::setDebugFrequencyAmount);
    }

    private void setInitialDelay(TimeValue initialDelay) {
        this.initialDelay = initialDelay;
    }

    private void setMaxDelayBound(TimeValue maxDelayBound) {
        this.maxDelayBound = maxDelayBound;
    }

    private void setTimeout(TimeValue timeout) {
        this.timeout = timeout;
    }

    private void setDebugMode(DebugFrequencyMode debugMode) {
        this.debugMode = debugMode;
    }

    private void setDebugFrequencyAmount(TimeValue debugFrequency) {
        this.debugFrequency = debugFrequency;
    }

    public static List<Setting<?>> getSettingsDefinitions() {
        return List.of(
            RETRY_INITIAL_DELAY_SETTING,
            RETRY_MAX_DELAY_BOUND_SETTING,
            RETRY_TIMEOUT_SETTING,
            RETRY_DEBUG_FREQUENCY_MODE_SETTING,
            RETRY_DEBUG_FREQUENCY_AMOUNT_SETTING
        );
    }

    TimeValue getInitialDelay() {
        return initialDelay;
    }

    TimeValue getMaxDelayBound() {
        return maxDelayBound;
    }

    TimeValue getTimeout() {
        return timeout;
    }

    DebugFrequencyMode getDebugMode() {
        return debugMode;
    }

    TimeValue getDebugFrequency() {
        return debugFrequency;
    }

    enum DebugFrequencyMode {
        /**
         * Indicates that the debug messages should be logged every time
         */
        ON,
        /**
         * Indicates that the debug messages should never be logged
         */
        OFF,
        /**
         * Indicates that the debug messages should be logged on an interval
         */
        INTERVAL
    }
}
