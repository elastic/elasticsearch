/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.stateless.cache.SearchCommitPrefetcher.PrefetchResult;

import java.util.Map;

/// Metrics describing how search shards handle new commit notifications with respect to prefetching:
///
/// - [#COMMIT_PREFETCH_TOTAL_METRIC]: one sample per notification, tagged with the [#TYPE_ATTRIBUTE] (the [PrefetchResult]), including the
///   notifications for which prefetching was skipped;
/// - [#COMMIT_PREFETCH_DURATION_HISTOGRAM_METRIC]: how long a prefetch took, from its start until all the ranges are in the cache. Unlike the
///   notification time measured on indexing shards, it is not capped by the timeout used to release the VBCC.
public record SearchCommitPrefetcherMetrics(LongCounter prefetchCounter, LongHistogram prefetchDurationHistogram) {

    public static final String COMMIT_PREFETCH_TOTAL_METRIC = "es.stateless.search.commit_prefetch.total";
    public static final String COMMIT_PREFETCH_DURATION_HISTOGRAM_METRIC = "es.stateless.search.commit_prefetch.duration.histogram";

    /// The [PrefetchResult#value()] of the notification.
    public static final String TYPE_ATTRIBUTE = "type";
    /// Whether the BCC of the notified commit was already uploaded, i.e. whether the prefetch could read from the object store.
    public static final String UPLOADED_ATTRIBUTE = "uploaded";

    public static final SearchCommitPrefetcherMetrics NOOP = new SearchCommitPrefetcherMetrics(
        MeterRegistry.NOOP.registerLongCounter(COMMIT_PREFETCH_TOTAL_METRIC + ".noop", "noop", "count"),
        MeterRegistry.NOOP.registerLongHistogram(COMMIT_PREFETCH_DURATION_HISTOGRAM_METRIC + ".noop", "noop", "ms")
    );

    public static SearchCommitPrefetcherMetrics register(MeterRegistry registry) {
        return new SearchCommitPrefetcherMetrics(
            registry.registerLongCounter(COMMIT_PREFETCH_TOTAL_METRIC, "Number of commit notifications handled by the prefetcher", "count"),
            registry.registerLongHistogram(
                COMMIT_PREFETCH_DURATION_HISTOGRAM_METRIC,
                "Time in milliseconds taken to prefetch the data of a commit notification",
                "ms"
            )
        );
    }

    public void recordPrefetchResult(PrefetchResult result, boolean uploaded) {
        prefetchCounter.incrementBy(1L, Map.of(TYPE_ATTRIBUTE, result.value(), UPLOADED_ATTRIBUTE, uploaded));
    }

    public void recordPrefetchDuration(PrefetchResult result, boolean uploaded, long durationInMillis) {
        prefetchDurationHistogram.record(durationInMillis, Map.of(TYPE_ATTRIBUTE, result.value(), UPLOADED_ATTRIBUTE, uploaded));
    }
}
