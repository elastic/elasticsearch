/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.search.SearchRequestAttributesExtractor;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Container class for aggregated metrics about search responses.
 */
public class SearchResponseMetrics {

    public enum ResponseCountTotalStatus {
        SUCCESS("success"),
        PARTIAL_FAILURE("partial_failure"),
        FAILURE("failure");

        private final String displayName;

        ResponseCountTotalStatus(String displayName) {
            this.displayName = displayName;
        }

        public String getDisplayName() {
            return displayName;
        }
    }

    public static final String RESPONSE_COUNT_TOTAL_STATUS_ATTRIBUTE_NAME = "response_status";

    public static final String TOOK_DURATION_TOTAL_HISTOGRAM_NAME = "es.search_response.took_durations.histogram";
    public static final String RESPONSE_COUNT_TOTAL_COUNTER_NAME = "es.search_response.response_count.total";
    public static final String STORE_BYTES_READ_HISTOGRAM_NAME = "es.search_response.store_bytes_read.histogram";
    private static final String SEARCH_PHASE_METRIC_FORMAT = "es.search_response.took_durations.%s.histogram";
    public static final String CAN_MATCH_SHARD_RESULT_BYTES_HISTOGRAM_NAME = "es.search.coord.can_match.result.bytes.histogram";
    public static final String DFS_SHARD_RESULT_BYTES_HISTOGRAM_NAME = "es.search.coord.dfs.result.bytes.histogram";
    public static final String DFS_QUERY_SHARD_RESULT_BYTES_HISTOGRAM_NAME = "es.search.coord.dfs_query.result.bytes.histogram";
    public static final String QUERY_SHARD_RESULT_BYTES_HISTOGRAM_NAME = "es.search.coord.query.result.bytes.histogram";
    public static final String RANK_FEATURE_SHARD_RESULT_BYTES_HISTOGRAM_NAME = "es.search.coord.rank_feature.result.bytes.histogram";
    public static final String FETCH_SHARD_RESULT_BYTES_HISTOGRAM_NAME = "es.search.coord.fetch.result.bytes.histogram";

    public static final String CAN_MATCH_SHARD_REQUEST_BYTES_HISTOGRAM_NAME = "es.search.coord.can_match.request.bytes.histogram";
    public static final String DFS_SHARD_REQUEST_BYTES_HISTOGRAM_NAME = "es.search.coord.dfs.request.bytes.histogram";
    public static final String DFS_QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME = "es.search.coord.dfs_query.request.bytes.histogram";
    public static final String QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME = "es.search.coord.query.request.bytes.histogram";
    public static final String RANK_FEATURE_SHARD_REQUEST_BYTES_HISTOGRAM_NAME = "es.search.coord.rank_feature.request.bytes.histogram";
    public static final String FETCH_SHARD_REQUEST_BYTES_HISTOGRAM_NAME = "es.search.coord.fetch.request.bytes.histogram";

    private final LongHistogram tookDurationTotalMillisHistogram;
    private final LongCounter responseCountTotalCounter;
    private final LongHistogram storeBytesReadHistogram;

    private final Map<String, LongHistogram> phaseNameToDurationHistogram;
    private final Map<String, LongHistogram> phaseNameToShardResultBytesHistogram;
    private final Map<String, LongHistogram> phaseNameToShardRequestBytesHistogram;

    public SearchResponseMetrics(MeterRegistry meterRegistry) {
        this.tookDurationTotalMillisHistogram = meterRegistry.registerLongHistogram(
            TOOK_DURATION_TOTAL_HISTOGRAM_NAME,
            "The SearchResponse.took durations in milliseconds, expressed as a histogram",
            "millis"
        );
        this.responseCountTotalCounter = meterRegistry.registerLongCounter(
            RESPONSE_COUNT_TOTAL_COUNTER_NAME,
            "The cumulative total of search responses with an attribute to describe "
                + "success, partial failure, or failure, expressed as a single total counter and individual "
                + "attribute counters",
            "count"
        );
        this.storeBytesReadHistogram = meterRegistry.registerLongHistogram(
            STORE_BYTES_READ_HISTOGRAM_NAME,
            "The number of bytes read from the store while serving a search request, expressed as a histogram",
            "bytes"
        );

        phaseNameToDurationHistogram = Map.of(
            "can_match",
            meterRegistry.registerLongHistogram(
                String.format(Locale.ROOT, SEARCH_PHASE_METRIC_FORMAT, "can_match"),
                "The search phase can_match duration in milliseconds at the coordinator, expressed as a histogram",
                "millis"
            ),
            "dfs",
            meterRegistry.registerLongHistogram(
                String.format(Locale.ROOT, SEARCH_PHASE_METRIC_FORMAT, "dfs"),
                "The search phase dfs duration in milliseconds at the coordinator, expressed as a histogram",
                "millis"
            ),
            "dfs_query",
            meterRegistry.registerLongHistogram(
                String.format(Locale.ROOT, SEARCH_PHASE_METRIC_FORMAT, "dfs_query"),
                "The search phase dfs_query duration in milliseconds at the coordinator, expressed as a histogram",
                "millis"
            ),
            "fetch",
            meterRegistry.registerLongHistogram(
                String.format(Locale.ROOT, SEARCH_PHASE_METRIC_FORMAT, "fetch"),
                "The search phase fetch duration in milliseconds at the coordinator, expressed as a histogram",
                "millis"
            ),
            "open_pit",
            meterRegistry.registerLongHistogram(
                String.format(Locale.ROOT, SEARCH_PHASE_METRIC_FORMAT, "open_pit"),
                "The search phase open_pit duration in milliseconds at the coordinator, expressed as a histogram",
                "millis"
            ),
            "query",
            meterRegistry.registerLongHistogram(
                String.format(Locale.ROOT, SEARCH_PHASE_METRIC_FORMAT, "query"),
                "The search phase query duration in milliseconds at the coordinator, expressed as a histogram",
                "millis"
            )
        );
        phaseNameToShardResultBytesHistogram = Map.of(
            "can_match",
            meterRegistry.registerLongHistogram(
                CAN_MATCH_SHARD_RESULT_BYTES_HISTOGRAM_NAME,
                "Total bytes of shard results received by the coordinator for the can_match phase, "
                    + "per search request, expressed as a histogram",
                "bytes"
            ),
            "dfs",
            meterRegistry.registerLongHistogram(
                DFS_SHARD_RESULT_BYTES_HISTOGRAM_NAME,
                "Total bytes of shard results received by the coordinator for the dfs phase, "
                    + "per search request, expressed as a histogram",
                "bytes"
            ),
            "dfs_query",
            meterRegistry.registerLongHistogram(
                DFS_QUERY_SHARD_RESULT_BYTES_HISTOGRAM_NAME,
                "Total bytes of shard results received by the coordinator for the dfs_query phase, "
                    + "per search request, expressed as a histogram",
                "bytes"
            ),
            "query",
            meterRegistry.registerLongHistogram(
                QUERY_SHARD_RESULT_BYTES_HISTOGRAM_NAME,
                "Total bytes of shard results received by the coordinator for the query phase, "
                    + "per search request, expressed as a histogram",
                "bytes"
            ),
            "rank-feature",
            meterRegistry.registerLongHistogram(
                RANK_FEATURE_SHARD_RESULT_BYTES_HISTOGRAM_NAME,
                "Total bytes of shard results received by the coordinator for the rank-feature phase, "
                    + "per search request, expressed as a histogram",
                "bytes"
            ),
            "fetch",
            meterRegistry.registerLongHistogram(
                FETCH_SHARD_RESULT_BYTES_HISTOGRAM_NAME,
                "Total bytes of shard results received by the coordinator for the fetch phase, "
                    + "per search request, expressed as a histogram",
                "bytes"
            )
        );
        phaseNameToShardRequestBytesHistogram = Map.of(
            "can_match",
            meterRegistry.registerLongHistogram(
                CAN_MATCH_SHARD_REQUEST_BYTES_HISTOGRAM_NAME,
                "Total bytes of shard requests sent by the coordinator for the can_match phase, "
                    + "per search request, expressed as a histogram",
                "bytes"
            ),
            "dfs",
            meterRegistry.registerLongHistogram(
                DFS_SHARD_REQUEST_BYTES_HISTOGRAM_NAME,
                "Total bytes of shard requests sent by the coordinator for the dfs phase, "
                    + "per search request, expressed as a histogram",
                "bytes"
            ),
            "dfs_query",
            meterRegistry.registerLongHistogram(
                DFS_QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME,
                "Total bytes of shard requests sent by the coordinator for the dfs_query phase, "
                    + "per search request, expressed as a histogram",
                "bytes"
            ),
            "query",
            meterRegistry.registerLongHistogram(
                QUERY_SHARD_REQUEST_BYTES_HISTOGRAM_NAME,
                "Total bytes of shard requests sent by the coordinator for the query phase, "
                    + "per search request, expressed as a histogram",
                "bytes"
            ),
            "rank-feature",
            meterRegistry.registerLongHistogram(
                RANK_FEATURE_SHARD_REQUEST_BYTES_HISTOGRAM_NAME,
                "Total bytes of shard requests sent by the coordinator for the rank-feature phase, "
                    + "per search request, expressed as a histogram",
                "bytes"
            ),
            "fetch",
            meterRegistry.registerLongHistogram(
                FETCH_SHARD_REQUEST_BYTES_HISTOGRAM_NAME,
                "Total bytes of shard requests sent by the coordinator for the fetch phase, "
                    + "per search request, expressed as a histogram",
                "bytes"
            )
        );
    }

    public long recordTookTimeForSearchScroll(long tookTime) {
        tookDurationTotalMillisHistogram.record(tookTime, SearchRequestAttributesExtractor.SEARCH_SCROLL_ATTRIBUTES);
        return tookTime;
    }

    public long recordTookTime(long tookTime, Long timeRangeFilterFromMillis, long nowInMillis, Map<String, Object> attributes) {
        SearchRequestAttributesExtractor.addTimeRangeAttribute(timeRangeFilterFromMillis, nowInMillis, attributes);
        tookDurationTotalMillisHistogram.record(tookTime, attributes);
        return tookTime;
    }

    public void incrementResponseCount(ResponseCountTotalStatus responseCountTotalStatus) {
        responseCountTotalCounter.incrementBy(
            1L,
            Map.of(RESPONSE_COUNT_TOTAL_STATUS_ATTRIBUTE_NAME, responseCountTotalStatus.getDisplayName())
        );
    }

    public void incrementResponseCount(ResponseCountTotalStatus responseCountTotalStatus, Map<String, Object> attributes) {
        Map<String, Object> attributesWithStatus = new HashMap<>(attributes);
        attributesWithStatus.put(RESPONSE_COUNT_TOTAL_STATUS_ATTRIBUTE_NAME, responseCountTotalStatus.getDisplayName());
        responseCountTotalCounter.incrementBy(1L, attributesWithStatus);
    }

    public void recordSearchPhaseDuration(String phaseName, long tookInNanos, Map<String, Object> attributes) {
        LongHistogram queryPhaseDurationHistogram = phaseNameToDurationHistogram.get(phaseName);
        assert queryPhaseDurationHistogram != null;
        queryPhaseDurationHistogram.record(TimeUnit.NANOSECONDS.toMillis(tookInNanos), attributes);
    }

    public void recordStoreBytesRead(long bytesRead, Map<String, Object> attributes) {
        storeBytesReadHistogram.record(bytesRead, attributes);
    }

    /**
     * Records the total bytes of shard results received by the coordinator for the given search phase,
     * as a single observation representing one search request.
     */
    public void recordSearchPhaseShardResultBytes(String phaseName, long bytes, Map<String, Object> attributes) {
        LongHistogram histogram = phaseNameToShardResultBytesHistogram.get(phaseName);
        assert histogram != null : "unknown phase: " + phaseName;
        if (histogram != null) {
            histogram.record(bytes, attributes);
        }
    }

    /**
     * Records the total bytes of shard requests sent by the coordinator for the given search phase,
     * as a single observation representing one search request.
     */
    public void recordSearchPhaseShardRequestBytes(String phaseName, long bytes, Map<String, Object> attributes) {
        LongHistogram histogram = phaseNameToShardRequestBytesHistogram.get(phaseName);
        assert histogram != null : "unknown phase: " + phaseName;
        if (histogram != null) {
            histogram.record(bytes, attributes);
        }
    }
}
