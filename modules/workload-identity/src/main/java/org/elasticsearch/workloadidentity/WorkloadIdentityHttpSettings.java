/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;

import java.util.List;

/**
 * Node-scope settings for the workload-identity-issuer HTTP transport.
 *
 * <p>The pool-size pair drives the underlying
 * {@link org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager}; the
 * eviction-interval/max-idle pair drives
 * {@link org.elasticsearch.workloadidentity.common.HttpConnectionEvictor}; connect
 * and request timeouts are applied per-request via Apache HC's
 * {@link org.apache.http.client.config.RequestConfig}.
 */
public final class WorkloadIdentityHttpSettings {

    private static final String HTTP_PREFIX = WorkloadIdentityIssuerSettings.SETTING_PREFIX + "http.";

    /**
     * Maximum total connections the {@link org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager}
     * can lease across all routes. Workload-identity traffic talks to a single host, so the per-route
     * limit (below) is typically the binding constraint.
     */
    public static final Setting<Integer> MAX_TOTAL_CONNECTIONS = Setting.intSetting(
        HTTP_PREFIX + "max_total_connections",
        20,
        1,
        Setting.Property.NodeScope
    );

    /** Maximum connections leased to a single route. */
    public static final Setting<Integer> MAX_ROUTE_CONNECTIONS = Setting.intSetting(
        HTTP_PREFIX + "max_route_connections",
        20,
        1,
        Setting.Property.NodeScope
    );

    // Bounds shared by CONNECT_TIMEOUT and REQUEST_TIMEOUT below. The 1ms floor rules out Apache
    // HC's "0 == wait forever" sentinel (an unreachable issuer would otherwise silently wedge
    // every code path that needs a workload-identity token). The 1-hour ceiling matches the
    // precedent in modules/repository-url's URLHttpClientSettings and is well below the
    // Integer.MAX_VALUE-ms cap imposed by Apache HC 4.x's RequestConfig int-millisecond API,
    // so the Math.toIntExact() conversion in HttpsWorkloadIdentityIssuerClient cannot overflow.
    private static final TimeValue MIN_TIMEOUT = TimeValue.timeValueMillis(1);
    private static final TimeValue MAX_TIMEOUT = TimeValue.timeValueMinutes(60);

    /** Connection establish timeout, applied as the Apache HC connect timeout per request. */
    public static final Setting<TimeValue> CONNECT_TIMEOUT = Setting.timeSetting(
        HTTP_PREFIX + "connect_timeout",
        TimeValue.timeValueSeconds(5),
        MIN_TIMEOUT,
        MAX_TIMEOUT,
        Setting.Property.NodeScope
    );

    /**
     * Per-request socket / read timeout. Bounds how long any single token request can wait for the
     * issuer to respond once the connection is established.
     */
    public static final Setting<TimeValue> REQUEST_TIMEOUT = Setting.timeSetting(
        HTTP_PREFIX + "request_timeout",
        TimeValue.timeValueSeconds(10),
        MIN_TIMEOUT,
        MAX_TIMEOUT,
        Setting.Property.NodeScope
    );

    private static final TimeValue DEFAULT_EVICTION_INTERVAL = TimeValue.timeValueMinutes(1);

    /**
     * Interval at which {@link org.elasticsearch.workloadidentity.common.HttpConnectionEvictor}
     * runs to close expired and idle connections in the pool.
     */
    public static final Setting<TimeValue> CONNECTION_EVICTION_INTERVAL = Setting.timeSetting(
        HTTP_PREFIX + "connection_eviction_interval",
        DEFAULT_EVICTION_INTERVAL,
        Setting.Property.NodeScope
    );

    /**
     * Maximum time a connection can sit idle in the pool before the evictor closes it. Defaults
     * to the eviction interval, so any connection idle for at least one tick is reclaimed on the
     * next pass.
     */
    public static final Setting<TimeValue> CONNECTION_MAX_IDLE_TIME = Setting.timeSetting(
        HTTP_PREFIX + "connection_max_idle_time",
        DEFAULT_EVICTION_INTERVAL,
        Setting.Property.NodeScope
    );

    /**
     * Hard cap on response body size before parsing. The workload-identity-issuer returns a single
     * JWT and a couple of integer fields; a few hundred KB is more than enough headroom while
     * still preventing an upstream from forcing this client to buffer unbounded bytes.
     */
    public static final Setting<ByteSizeValue> MAX_RESPONSE_SIZE = Setting.byteSizeSetting(
        HTTP_PREFIX + "max_response_size",
        ByteSizeValue.of(1, ByteSizeUnit.MB),
        ByteSizeValue.of(1, ByteSizeUnit.KB),
        ByteSizeValue.of(10, ByteSizeUnit.MB),
        Setting.Property.NodeScope
    );

    private WorkloadIdentityHttpSettings() {}

    /** @return list of node-scope settings registered by this class. */
    public static List<Setting<?>> getSettings() {
        return List.of(
            MAX_TOTAL_CONNECTIONS,
            MAX_ROUTE_CONNECTIONS,
            CONNECT_TIMEOUT,
            REQUEST_TIMEOUT,
            CONNECTION_EVICTION_INTERVAL,
            CONNECTION_MAX_IDLE_TIME,
            MAX_RESPONSE_SIZE
        );
    }
}
