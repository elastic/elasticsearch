/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Telemetry when bulk-by-scroll tasks fail because the keep-alive of the underlying search contexts expire
 */
public class BulkByScrollSearchContextMetrics {

    public static final String SEARCH_CONTEXT_KEEPALIVE_EXPIRED_COUNTER = "es.bulk_by_scroll.search_context.keepalive_expired.total";

    /**
     * The kind of async bulk-by-scroll operation running
     */
    public static final String ATTRIBUTE_NAME_TASK_KIND = "es_bulk_by_scroll_task_kind";

    /**
     * Local cluster vs remote source cluster for reindex.
     * */
    public static final String ATTRIBUTE_NAME_SEARCH_SOURCE = "es_bulk_by_scroll_search_source";
    public static final String ATTRIBUTE_VALUE_SEARCH_SOURCE_LOCAL = "local";
    public static final String ATTRIBUTE_VALUE_SEARCH_SOURCE_REMOTE = "remote";

    private final LongCounter searchContextKeepaliveExpiredCounter;

    public BulkByScrollSearchContextMetrics(MeterRegistry meterRegistry) {
        this.searchContextKeepaliveExpiredCounter = meterRegistry.registerLongCounter(
            SEARCH_CONTEXT_KEEPALIVE_EXPIRED_COUNTER,
            "Bulk-by-scroll tasks whose missing scroll/PIT contexts expired before the next refresh (heuristic)",
            "unit"
        );
    }

    /** Which API started the bulk-by-scroll worker */
    public enum TaskKind {
        REINDEX,
        UPDATE_BY_QUERY,
        DELETE_BY_QUERY;

        /** Lowercase enum name for metric attributes ({@code reindex}, {@code update_by_query}, …). */
        public String attributeValue() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * Updates the metrics when a task fails because the keep-alive expired. The {@link TaskKind} and {@code remoteSource} become
     * metric attributes for filtering on the reindexing dashboard
     */
    public void recordKeepaliveExpiry(TaskKind taskKind, boolean remoteSearch) {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ATTRIBUTE_NAME_TASK_KIND, taskKind.attributeValue());
        attributes.put(
            ATTRIBUTE_NAME_SEARCH_SOURCE,
            remoteSearch ? ATTRIBUTE_VALUE_SEARCH_SOURCE_REMOTE : ATTRIBUTE_VALUE_SEARCH_SOURCE_LOCAL
        );
        searchContextKeepaliveExpiredCounter.incrementBy(1, attributes);
    }
}
