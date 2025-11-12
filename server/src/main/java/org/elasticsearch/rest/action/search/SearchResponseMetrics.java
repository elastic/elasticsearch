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
    private static final String SEARCH_PHASE_METRIC_FORMAT = "es.search_response.took_durations.%s.histogram";

    private final LongHistogram tookDurationTotalMillisHistogram;
    private final LongCounter responseCountTotalCounter;

    private final Map<String, LongHistogram> phaseNameToDurationHistogram;

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
}
