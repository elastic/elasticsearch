/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search.stats;

import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class ShardSearchAPMMetrics implements SearchOperationListener {

    public static final String SEARCH_PHASES_DURATION_METRIC = "es.search.shards.phases.duration.histogram";

    public static final String PHASE_ATTRIBUTE_NAME = "phase";
    public static final String SYSTEM_THREAD_ATTRIBUTE_NAME = "system_thread";
    public static final String QUERY_PHASE = "query_phase";
    public static final String FETCH_PHASE = "fetch_phase";

    private final LongHistogram actionLatencies;

    public ShardSearchAPMMetrics(MeterRegistry meterRegistry) {
        this.actionLatencies = meterRegistry.registerLongHistogram(
            SEARCH_PHASES_DURATION_METRIC,
            "Search actions execution times at the shard level, expressed as a histogram",
            "ms"
        );
    }

    @Override
    public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
        recordPhaseLatency(QUERY_PHASE, TimeUnit.NANOSECONDS.toMillis(tookInNanos));
    }

    @Override
    public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
        recordPhaseLatency(FETCH_PHASE, TimeUnit.NANOSECONDS.toMillis(tookInNanos));
    }

    private void recordPhaseLatency(String phaseName, long tookInMillis) {
        boolean isSystem = ThreadPool.isSystemThreadPool();
        Map<String, Object> attributes = Map.of(PHASE_ATTRIBUTE_NAME, phaseName, SYSTEM_THREAD_ATTRIBUTE_NAME, isSystem);
        actionLatencies.record(tookInMillis, attributes);
    }
}
