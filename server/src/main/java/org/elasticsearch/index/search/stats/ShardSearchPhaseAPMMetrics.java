/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search.stats;

import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class ShardSearchPhaseAPMMetrics implements SearchOperationListener {

    public static final String QUERY_SEARCH_PHASE_METRIC = "es.search.shards.phases.query.duration.histogram";
    public static final String FETCH_SEARCH_PHASE_METRIC = "es.search.shards.phases.fetch.duration.histogram";

    public static final String SYSTEM_THREAD_ATTRIBUTE_NAME = "system_thread";

    private final LongHistogram queryPhaseMetric;
    private final LongHistogram fetchPhaseMetric;

    // Avoid allocating objects in the search path and multithreading clashes
    private static final ThreadLocal<Map<String, Object>> THREAD_LOCAL_ATTRS = ThreadLocal.withInitial(() -> new HashMap<>(1));

    public ShardSearchPhaseAPMMetrics(MeterRegistry meterRegistry) {
        this.queryPhaseMetric = meterRegistry.registerLongHistogram(
            QUERY_SEARCH_PHASE_METRIC,
            "Query search phase execution times at the shard level, expressed as a histogram",
            "ms"
        );
        this.fetchPhaseMetric = meterRegistry.registerLongHistogram(
            FETCH_SEARCH_PHASE_METRIC,
            "Fetch search phase execution times at the shard level, expressed as a histogram",
            "ms"
        );
    }

    @Override
    public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
        recordPhaseLatency(queryPhaseMetric, tookInNanos);
    }

    @Override
    public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
        recordPhaseLatency(fetchPhaseMetric, tookInNanos);
    }

    private static void recordPhaseLatency(LongHistogram histogramMetric, long tookInNanos) {
        Map<String, Object> attrs = ShardSearchPhaseAPMMetrics.THREAD_LOCAL_ATTRS.get();
        boolean isSystem = ((EsExecutors.EsThread) Thread.currentThread()).isSystem();
        attrs.put(SYSTEM_THREAD_ATTRIBUTE_NAME, isSystem);
        histogramMetric.record(TimeUnit.NANOSECONDS.toMillis(tookInNanos), attrs);
    }
}
