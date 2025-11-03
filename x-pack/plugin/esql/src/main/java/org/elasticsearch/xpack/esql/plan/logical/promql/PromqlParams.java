/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import java.time.Duration;
import java.time.Instant;

/**
 * Container for PromQL command parameters:
 * <ul>
 *     <li>time for instant queries</li>
 *     <li>start, end, step for range queries</li>
 * </ul>
 * These can be specified in the {@linkplain org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand PROMQL command} like so:
 * <pre>
 *     # instant query
 *     PROMQL time `2025-10-31T00:00:00Z` (avg(foo))
 *     # range query with explicit start and end
 *     PROMQL start `2025-10-31T00:00:00Z` end `2025-10-31T01:00:00Z` step 1m (avg(foo))
 *     # range query with implicit time bounds, doesn't support calling {@code start()} or {@code end()} functions
 *     PROMQL step 5m (avg(foo))
 * </pre>
 *
 * @see <a href="https://prometheus.io/docs/prometheus/latest/querying/api/#expression-queries">PromQL API documentation</a>
 */
public record PromqlParams(Instant time, Instant start, Instant end, Duration step) {

    public boolean isInstantQuery() {
        return time != null;
    }

    public boolean isRangeQuery() {
        return step != null;
    }
}
