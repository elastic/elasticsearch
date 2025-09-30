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
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class ShardSearchPhaseAPMMetrics implements SearchOperationListener {

    public static final String QUERY_SEARCH_PHASE_METRIC = "es.search.shards.phases.query.duration.histogram";
    public static final String FETCH_SEARCH_PHASE_METRIC = "es.search.shards.phases.fetch.duration.histogram";
    public static final String FETCH_SUBPHASE_METRIC_FORMAT = "es.search.shards.phases.fetch.subphase.%s.duration.histogram";

    private final LongHistogram queryPhaseMetric;
    private final LongHistogram fetchPhaseMetric;
    private final Map<String, LongHistogram> fetchSubPhaseMetrics;

    public ShardSearchPhaseAPMMetrics(MeterRegistry meterRegistry, List<String> fetchSubPhaseNames) {
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
        this.fetchSubPhaseMetrics = fetchSubPhaseNames.stream()
            .collect(
                Collectors.toMap(
                    name -> name,
                    name -> meterRegistry.registerLongHistogram(
                        String.format(Locale.ROOT, FETCH_SUBPHASE_METRIC_FORMAT, name),
                        "Fetch sub-phase " + name + " execution times at the shard level, expressed as a histogram",
                        "ms"
                    )
                )
            );
    }

    @Override
    public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
        SearchExecutionContext searchExecutionContext = searchContext.getSearchExecutionContext();
        Long rangeTimestampFrom = searchExecutionContext.getRangeTimestampFrom();
        recordPhaseLatency(queryPhaseMetric, tookInNanos, searchContext.request(), rangeTimestampFrom);
    }

    @Override
    public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
        SearchExecutionContext searchExecutionContext = searchContext.getSearchExecutionContext();
        Long rangeTimestampFrom = searchExecutionContext.getRangeTimestampFrom();
        recordPhaseLatency(fetchPhaseMetric, tookInNanos, searchContext.request(), rangeTimestampFrom);
    }

    @Override
    public void onFetchSubPhase(SearchContext searchContext, String subPhaseName, long tookInNanos) {
        SearchExecutionContext searchExecutionContext = searchContext.getSearchExecutionContext();
        Long rangeTimestampFrom = searchExecutionContext.getRangeTimestampFrom();
        LongHistogram histogramMetric = fetchSubPhaseMetrics.get(subPhaseName);
        if (histogramMetric != null) {
            recordPhaseLatency(histogramMetric, tookInNanos, searchContext.request(), rangeTimestampFrom);
        }
    }

    private static void recordPhaseLatency(
        LongHistogram histogramMetric,
        long tookInNanos,
        ShardSearchRequest request,
        Long rangeTimestampFrom
    ) {
        Map<String, Object> attributes = SearchRequestAttributesExtractor.extractAttributes(
            request,
            rangeTimestampFrom,
            request.nowInMillis()
        );
        histogramMetric.record(TimeUnit.NANOSECONDS.toMillis(tookInNanos), attributes);
    }
}
