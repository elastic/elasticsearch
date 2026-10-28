/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;

/**
 * Metrics measuring cold segments on search nodes. A cold segment is a part of a commit's data that has been notified to the search
 * shard but has not yet been uploaded to the object store and therefore are not candidate to prefetching. After a refresh these segments
 * are exposed and can be used by searches, and until the commit's Batched Compound Commit (BCC) is uploaded to the object store they are
 * served from the indexing shard (read cold, before they are cached).
 * <p>
 * Two sizes are recorded per such commit, both in bytes and tagged with the same {@link #PREFETCHING_ATTRIBUTE}:
 * <ul>
 *     <li>{@link #COLD_SEGMENTS_TOTAL_SIZE_HISTOGRAM_METRIC}: the total size of the commit's internal files (new data embedded in the
 *     commit), excluding the header and other non-file content;</li>
 *     <li>{@link #COLD_SEGMENTS_MERGE_SIZE_HISTOGRAM_METRIC}: the subset of new data that has been produced by a merge. A merge introduces
 *     no new documents, so reading merged segments cold before they are uploaded and prewarmed only adds cold reads without much
 *     benefit.</li>
 * </ul>
 */
public record SearchColdSegmentsMetrics(LongHistogram coldSegmentsTotalSizeHistogram, LongHistogram coldSegmentsMergeSizeHistogram) {

    public static final String COLD_SEGMENTS_TOTAL_SIZE_HISTOGRAM_METRIC = "es.search_shard.cold_segments.total_size_in_bytes.histogram";
    public static final String COLD_SEGMENTS_MERGE_SIZE_HISTOGRAM_METRIC = "es.search_shard.cold_segments.merge_size_in_bytes.histogram";

    /**
     * Attribute (dimension) attached to each sample, describing the prefetch outcome for the commit's cold segments (see
     * {@link org.elasticsearch.xpack.stateless.cache.SearchCommitPrefetcher.PrefetchResult}). It lets us tell apart cold segments that
     * prefetching warmed (foreground/background) from those left cold because prefetching was skipped.
     */
    public static final String PREFETCHING_ATTRIBUTE = "prefetching";

    public static final SearchColdSegmentsMetrics NOOP = new SearchColdSegmentsMetrics(
        MeterRegistry.NOOP.registerLongHistogram(COLD_SEGMENTS_TOTAL_SIZE_HISTOGRAM_METRIC + ".noop", "noop", "bytes"),
        MeterRegistry.NOOP.registerLongHistogram(COLD_SEGMENTS_MERGE_SIZE_HISTOGRAM_METRIC + ".noop", "noop", "bytes")
    );

    public static SearchColdSegmentsMetrics register(MeterRegistry registry) {
        LongHistogram coldSegmentsTotalSizeHistogram = registry.registerLongHistogram(
            COLD_SEGMENTS_TOTAL_SIZE_HISTOGRAM_METRIC,
            "Total size in bytes of a commit's internal files (header and other non-file content excluded) notified to a search shard "
                + "while its BCC was not yet uploaded",
            "bytes"
        );
        LongHistogram coldSegmentsMergeSizeHistogram = registry.registerLongHistogram(
            COLD_SEGMENTS_MERGE_SIZE_HISTOGRAM_METRIC,
            "Size in bytes of merge-produced segment files notified to a search shard while their BCC was not yet uploaded",
            "bytes"
        );
        return new SearchColdSegmentsMetrics(coldSegmentsTotalSizeHistogram, coldSegmentsMergeSizeHistogram);
    }

    /**
     * Records, for a single commit's cold segments (notified to a search shard before its BCC was uploaded), both the total
     * {@code coldTotalBytes} of the commit's internal files (header and other non-file content excluded) and the {@code coldMergeBytes}
     * subset produced by a merge, tagged with the {@code reason} (the prefetch outcome).
     */
    public void recordColdSegments(long coldTotalBytes, long coldMergeBytes, String reason) {
        final Map<String, Object> attributes = Map.of(PREFETCHING_ATTRIBUTE, reason);
        coldSegmentsTotalSizeHistogram.record(coldTotalBytes, attributes);
        coldSegmentsMergeSizeHistogram.record(coldMergeBytes, attributes);
    }
}
