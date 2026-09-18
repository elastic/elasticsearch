/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.oauth2;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.List;
import java.util.Objects;

/**
 * Holds dynamic cluster settings that control the network timeouts used by {@link OAuth2TokenFetcher}
 * when talking to an IdP token endpoint. Both settings are {@link Setting.Property#Dynamic}, so they
 * take effect on the next token fetch without a node restart.
 *
 * <p>Construct one instance per node (singleton) and pass it down to every {@link OAuth2TokenFetcher}.
 * The constructor registers update consumers with the {@link ClusterService}; creating multiple
 * instances for the same node would register duplicate consumers.
 */
public class OAuth2ClusterSettings {

    static final TimeValue DEFAULT_CONNECT_TIMEOUT = TimeValue.timeValueSeconds(5);

    // The time we wait for a TCP connection to be established to the IdP token endpoint.
    public static final Setting<TimeValue> CONNECT_TIMEOUT = Setting.timeSetting(
        "xpack.inference.oauth2.connect_timeout",
        DEFAULT_CONNECT_TIMEOUT,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    static final TimeValue DEFAULT_READ_TIMEOUT = TimeValue.timeValueSeconds(10);

    // The maximum time to wait for data to arrive on the socket (per-read inactivity / SO_TIMEOUT).
    public static final Setting<TimeValue> READ_TIMEOUT = Setting.timeSetting(
        "xpack.inference.oauth2.read_timeout",
        DEFAULT_READ_TIMEOUT,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile int connectTimeoutMillis;
    private volatile int readTimeoutMillis;

    public OAuth2ClusterSettings(Settings settings, ClusterService clusterService) {
        Objects.requireNonNull(settings);
        Objects.requireNonNull(clusterService);

        connectTimeoutMillis = Math.toIntExact(CONNECT_TIMEOUT.get(settings).getMillis());
        readTimeoutMillis = Math.toIntExact(READ_TIMEOUT.get(settings).getMillis());

        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(CONNECT_TIMEOUT, v -> connectTimeoutMillis = Math.toIntExact(v.getMillis()));
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(READ_TIMEOUT, v -> readTimeoutMillis = Math.toIntExact(v.getMillis()));
    }

    /**
     * Returns the connect timeout in milliseconds.
     * The value reflects the latest dynamic cluster-settings update.
     */
    public int connectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    /**
     * Returns the read (socket inactivity) timeout in milliseconds.
     * The value reflects the latest dynamic cluster-settings update.
     */
    public int readTimeoutMillis() {
        return readTimeoutMillis;
    }

    public static List<Setting<?>> getSettingsDefinitions() {
        return List.of(CONNECT_TIMEOUT, READ_TIMEOUT);
    }
}
