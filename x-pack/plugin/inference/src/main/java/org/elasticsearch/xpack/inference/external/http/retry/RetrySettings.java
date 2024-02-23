/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.List;

public class RetrySettings {

    public static final Setting<TimeValue> RETRY_INITIAL_DELAY_SETTING = Setting.timeSetting(
        "xpack.inference.http.retry.initial_delay",
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> RETRY_MAX_DELAY_BOUND_SETTING = Setting.timeSetting(
        "xpack.inference.http.retry.max_delay_bound",
        TimeValue.timeValueSeconds(5),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> RETRY_TIMEOUT_SETTING = Setting.timeSetting(
        "xpack.inference.http.retry.timeout",
        TimeValue.timeValueSeconds(30),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final InternalSettings internalSettings;

    public RetrySettings(Settings settings) {
        var initialDelay = RETRY_INITIAL_DELAY_SETTING.get(settings);
        var maxDelayBound = RETRY_MAX_DELAY_BOUND_SETTING.get(settings);
        var timeoutValue = RETRY_TIMEOUT_SETTING.get(settings);
        this.internalSettings = new InternalSettings(initialDelay, maxDelayBound, timeoutValue);
    }

    public record InternalSettings(TimeValue initialDelay, TimeValue maxDelayBound, TimeValue timeoutValue) {}

    public InternalSettings getSettings() {
        return internalSettings;
    }

    public static List<Setting<?>> getSettingsDefinitions() {
        return List.of(RETRY_INITIAL_DELAY_SETTING, RETRY_MAX_DELAY_BOUND_SETTING, RETRY_TIMEOUT_SETTING);
    }
}
