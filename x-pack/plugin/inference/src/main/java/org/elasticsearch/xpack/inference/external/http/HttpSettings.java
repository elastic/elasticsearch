/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;

import java.util.List;

public class HttpSettings {
    // These settings are default scope for testing
    static final Setting<ByteSizeValue> MAX_HTTP_RESPONSE_SIZE = Setting.byteSizeSetting(
        "xpack.inference.http.max_response_size",
        new ByteSizeValue(10, ByteSizeUnit.MB),   // default
        ByteSizeValue.ONE, // min
        new ByteSizeValue(50, ByteSizeUnit.MB),   // max
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    static final Setting<Integer> MAX_CONNECTIONS = Setting.intSetting(
        "xpack.inference.http.max_connections",
        500,
        1,
        // TODO pick a reasonable value here
        1000,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final TimeValue DEFAULT_CONNECTION_EVICTION_THREAD_INTERVAL_TIME = TimeValue.timeValueSeconds(10);

    static final Setting<TimeValue> CONNECTION_EVICTION_THREAD_INTERVAL_SETTING = Setting.timeSetting(
        "xpack.inference.http.connection_eviction_interval",
        DEFAULT_CONNECTION_EVICTION_THREAD_INTERVAL_TIME,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final TimeValue DEFAULT_CONNECTION_EVICTION_MAX_IDLE_TIME_SETTING = DEFAULT_CONNECTION_EVICTION_THREAD_INTERVAL_TIME;
    static final Setting<TimeValue> CONNECTION_EVICTION_MAX_IDLE_TIME_SETTING = Setting.timeSetting(
        "xpack.inference.http.connection_eviction_max_idle_time",
        DEFAULT_CONNECTION_EVICTION_MAX_IDLE_TIME_SETTING,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile ByteSizeValue maxResponseSize;
    private volatile int maxConnections;
    private volatile TimeValue evictionInterval;
    private volatile TimeValue evictionMaxIdle;

    public HttpSettings(Settings settings, ClusterService clusterService) {
        this.maxResponseSize = MAX_HTTP_RESPONSE_SIZE.get(settings);
        this.maxConnections = MAX_CONNECTIONS.get(settings);
        this.evictionInterval = CONNECTION_EVICTION_THREAD_INTERVAL_SETTING.get(settings);
        this.evictionMaxIdle = CONNECTION_EVICTION_MAX_IDLE_TIME_SETTING.get(settings);

        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_HTTP_RESPONSE_SIZE, this::setMaxResponseSize);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_CONNECTIONS, this::setMaxConnections);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(CONNECTION_EVICTION_THREAD_INTERVAL_SETTING, this::setEvictionInterval);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CONNECTION_EVICTION_MAX_IDLE_TIME_SETTING, this::setEvictionMaxIdle);
    }

    public ByteSizeValue getMaxResponseSize() {
        return maxResponseSize;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public TimeValue getEvictionInterval() {
        return evictionInterval;
    }

    public TimeValue getEvictionMaxIdle() {
        return evictionMaxIdle;
    }

    private void setMaxResponseSize(ByteSizeValue maxResponseSize) {
        this.maxResponseSize = maxResponseSize;
    }

    private void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    private void setEvictionInterval(TimeValue evictionInterval) {
        this.evictionInterval = evictionInterval;
    }

    private void setEvictionMaxIdle(TimeValue evictionMaxIdle) {
        this.evictionMaxIdle = evictionMaxIdle;
    }

    public static List<Setting<?>> getSettings() {
        return List.of(
            MAX_HTTP_RESPONSE_SIZE,
            MAX_CONNECTIONS,
            CONNECTION_EVICTION_THREAD_INTERVAL_SETTING,
            CONNECTION_EVICTION_MAX_IDLE_TIME_SETTING
        );
    }
}
