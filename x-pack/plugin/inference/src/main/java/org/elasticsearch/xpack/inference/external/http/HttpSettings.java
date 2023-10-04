/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;

import java.util.List;

public class HttpSettings {
    public static final Setting<ByteSizeValue> MAX_HTTP_RESPONSE_SIZE = Setting.byteSizeSetting(
        "xpack.inference.http.max_response_size",
        new ByteSizeValue(10, ByteSizeUnit.MB),   // default
        ByteSizeValue.ONE, // min
        new ByteSizeValue(50, ByteSizeUnit.MB),   // max
        Setting.Property.NodeScope
    );
    public static final Setting<Integer> MAX_CONNECTIONS = Setting.intSetting(
        "xpack.inference.http.max_connections",
        500,
        1,
        // TODO pick a reasonable value here
        1000,
        Setting.Property.NodeScope
    );

    private static final TimeValue DEFAULT_CONNECTION_EVICTION_THREAD_SLEEP_TIME = TimeValue.timeValueSeconds(10);

    public static final Setting<TimeValue> CONNECTION_EVICTION_THREAD_SLEEP_TIME_SETTING = Setting.timeSetting(
        "xpack.inference.http.connection_eviction_sleep_time",
        DEFAULT_CONNECTION_EVICTION_THREAD_SLEEP_TIME,
        Setting.Property.NodeScope
    );

    private static final TimeValue DEFAULT_CONNECTION_EVICTION_MAX_IDLE_TIME_SETTING = DEFAULT_CONNECTION_EVICTION_THREAD_SLEEP_TIME;
    public static final Setting<TimeValue> CONNECTION_EVICTION_MAX_IDLE_TIME_SETTING = Setting.timeSetting(
        "xpack.inference.http.connection_eviction_max_idle_time",
        DEFAULT_CONNECTION_EVICTION_MAX_IDLE_TIME_SETTING,
        Setting.Property.NodeScope
    );

    public static List<Setting<?>> getSettings() {
        return List.of(
            MAX_HTTP_RESPONSE_SIZE,
            MAX_CONNECTIONS,
            CONNECTION_EVICTION_THREAD_SLEEP_TIME_SETTING,
            CONNECTION_EVICTION_MAX_IDLE_TIME_SETTING
        );
    }

    private HttpSettings() {}
}
