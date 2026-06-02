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
 * eviction-interval/max-idle pair drives {@link HttpConnectionEvictor}; connect
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

    /** Connection establish timeout, applied as the Apache HC connect timeout per request. */
    public static final Setting<TimeValue> CONNECT_TIMEOUT = Setting.timeSetting(
        HTTP_PREFIX + "connect_timeout",
        TimeValue.timeValueSeconds(5),
        TimeValue.timeValueMillis(1),
        TimeValue.timeValueMinutes(60),
        Setting.Property.NodeScope
    );

    /**
     * Per-request socket / read timeout. Bounds how long any single token request can wait for the
     * issuer to respond once the connection is established.
     */
    public static final Setting<TimeValue> REQUEST_TIMEOUT = Setting.timeSetting(
        HTTP_PREFIX + "request_timeout",
        TimeValue.timeValueSeconds(10),
        TimeValue.timeValueMillis(1),
        TimeValue.timeValueMinutes(60),
        Setting.Property.NodeScope
    );

    private static final TimeValue DEFAULT_EVICTION_INTERVAL = TimeValue.timeValueMinutes(1);

    /**
     * Interval at which {@link HttpConnectionEvictor} runs to close expired and idle connections
     * in the pool.
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

    private static final String RETRY_PREFIX = HTTP_PREFIX + "retry.";

    /**
     * Initial delay before the second token-request attempt, used as the seed for
     * {@link org.elasticsearch.action.support.RetryableAction}'s jittered exponential backoff.
     * Each subsequent retry doubles the bound (capped at {@link #RETRY_MAX_DELAY_BOUND}) and the
     * actual delay is sampled uniformly within that bound.
     */
    public static final Setting<TimeValue> RETRY_INITIAL_DELAY = Setting.timeSetting(
        RETRY_PREFIX + "initial_delay",
        TimeValue.timeValueMillis(200),
        TimeValue.timeValueMillis(1),
        TimeValue.timeValueMinutes(1),
        Setting.Property.NodeScope
    );

    /**
     * Upper bound on the per-retry delay produced by the exponential backoff. Bounds the spacing
     * between retries when the issuer is in a long-running degraded state, so retries do not
     * grow toward the {@link #RETRY_TIMEOUT} budget unboundedly.
     */
    public static final Setting<TimeValue> RETRY_MAX_DELAY_BOUND = Setting.timeSetting(
        RETRY_PREFIX + "max_delay_bound",
        TimeValue.timeValueSeconds(5),
        TimeValue.timeValueMillis(1),
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope
    );

    /**
     * Wall-clock budget for the entire retrying token request, including all in-flight time and
     * inter-attempt sleeps. Once exceeded, the most recent failure is surfaced to the listener
     * with earlier attempts attached as suppressed exceptions. Must be at least as large as
     * {@link #REQUEST_TIMEOUT}; the {@link HttpsWorkloadIdentityIssuerClient} constructor enforces
     * that at startup.
     */
    public static final Setting<TimeValue> RETRY_TIMEOUT = Setting.timeSetting(
        RETRY_PREFIX + "timeout",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueMillis(1),
        TimeValue.timeValueMinutes(60),
        Setting.Property.NodeScope
    );

    /**
     * Hard cap on the total number of attempts (including the first). Acts as a count-based
     * backstop alongside the wall-clock {@link #RETRY_TIMEOUT}; whichever fires first stops the
     * retrying action.
     */
    public static final Setting<Integer> RETRY_MAX_ATTEMPTS = Setting.intSetting(
        RETRY_PREFIX + "max_attempts",
        3,
        1,
        100,
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
            MAX_RESPONSE_SIZE,
            RETRY_INITIAL_DELAY,
            RETRY_MAX_DELAY_BOUND,
            RETRY_TIMEOUT,
            RETRY_MAX_ATTEMPTS
        );
    }
}
