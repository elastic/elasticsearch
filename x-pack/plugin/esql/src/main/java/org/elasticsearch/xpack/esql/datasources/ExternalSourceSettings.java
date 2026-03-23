/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Setting;

import java.util.List;

/**
 * Cluster settings for controlling ESQL external source cloud API rate limiting.
 * All settings are dynamic (can be changed without restart) and node-scoped.
 * <p>
 * These settings control three layers of defense against cloud API throttling:
 * <ul>
 *   <li><b>Concurrency limiting</b>: Caps concurrent in-flight cloud API requests per node</li>
 *   <li><b>Throttle retry budget</b>: Higher retry count for 429/503 throttling errors</li>
 *   <li><b>Adaptive backoff</b>: Dynamically increases delays when throttling is detected</li>
 * </ul>
 */
public final class ExternalSourceSettings {

    private ExternalSourceSettings() {}

    /**
     * Maximum number of concurrent cloud API requests per storage scheme per node.
     * Set to 0 to disable concurrency limiting entirely.
     * Default: 50. Cloud APIs typically handle 50 concurrent requests per IP easily;
     * increase for high-throughput clusters, decrease if experiencing throttling.
     */
    public static final Setting<Integer> MAX_CONCURRENT_REQUESTS = Setting.intSetting(
        "esql.external.max_concurrent_requests",
        50,
        0,
        500,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Maximum retry attempts for throttling errors (HTTP 429, 503, SlowDown).
     * Throttling is always transient and expected under load — a higher budget than
     * for other transient errors (connection reset, timeout) prevents unnecessary failures.
     * Default: 10.
     */
    public static final Setting<Integer> THROTTLE_RETRY_LIMIT = Setting.intSetting(
        "esql.external.throttle_retry_limit",
        10,
        1,
        100,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Enables adaptive backoff for cloud API throttling. When enabled, throttle retry
     * delays are scaled by a shared multiplier that increases when throttling is detected
     * and decays over time during sustained success. This prevents thundering herd
     * recovery where many retrying threads hit the API simultaneously.
     * Default: true.
     */
    public static final Setting<Boolean> ADAPTIVE_BACKOFF_ENABLED = Setting.boolSetting(
        "esql.external.adaptive_backoff_enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static List<Setting<?>> settings() {
        return List.of(MAX_CONCURRENT_REQUESTS, THROTTLE_RETRY_LIMIT, ADAPTIVE_BACKOFF_ENABLED);
    }
}
