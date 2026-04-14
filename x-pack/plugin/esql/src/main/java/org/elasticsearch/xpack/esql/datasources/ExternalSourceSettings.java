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
 * Only two operational knobs are exposed: a concurrency cap to prevent throttling
 * in the first place, and a total retry duration budget to bound the damage when
 * throttling is persistent. Internal details (throttle retry count = 10,
 * adaptive backoff = always enabled, exponential delays) use hardcoded defaults
 * that match industry practice.
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
     * Maximum total time (in seconds) to spend retrying throttled cloud API requests
     * before giving up. Bounds the cumulative retry duration regardless of the retry count,
     * ensuring queries fail cleanly when throttling is persistent rather than blocking
     * until the HTTP request timeout fires.
     * Default: 30 seconds. Set to 0 to disable the duration budget (retry count only).
     */
    public static final Setting<Integer> THROTTLE_MAX_RETRY_DURATION = Setting.intSetting(
        "esql.external.throttle_max_retry_duration",
        30,
        0,
        300,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static List<Setting<?>> settings() {
        return List.of(MAX_CONCURRENT_REQUESTS, THROTTLE_MAX_RETRY_DURATION);
    }
}
