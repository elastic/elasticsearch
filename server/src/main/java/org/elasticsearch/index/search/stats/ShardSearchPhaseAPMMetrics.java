/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search.stats;

import org.elasticsearch.action.search.SearchRequestAttributesExtractor;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class ShardSearchPhaseAPMMetrics implements SearchOperationListener {

    public static final String QUERY_SEARCH_PHASE_METRIC = "es.search.shards.phases.query.duration.histogram";
    public static final String FETCH_SEARCH_PHASE_METRIC = "es.search.shards.phases.fetch.duration.histogram";

    private final LongHistogram queryPhaseMetric;
    private final LongHistogram fetchPhaseMetric;

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
        recordPhaseLatency(queryPhaseMetric, tookInNanos, searchContext.request());
    }

    @Override
    public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
        recordPhaseLatency(fetchPhaseMetric, tookInNanos, searchContext.request());
    }

    private static void recordPhaseLatency(LongHistogram histogramMetric, long tookInNanos, ShardSearchRequest request) {
        Map<String, Object> attributes = SearchRequestAttributesExtractor.extractAttributes(request);
        histogramMetric.record(TimeUnit.NANOSECONDS.toMillis(tookInNanos), attributes);
    }
}
