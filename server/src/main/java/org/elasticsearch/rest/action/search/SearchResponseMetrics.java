/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.usage.SearchUsage;

import java.util.HashMap;
import java.util.Map;

/**
 * Container class for aggregated metrics about search responses.
 */
public class SearchResponseMetrics {

    private static final Logger logger = LogManager.getLogger(SearchResponseMetrics.class);
    public static final String QUERY_USAGE_SUFFIX = "_query";
    public static final String SECTION_USAGE_SUFFIX = "_section";
    public static final String RESCORER_USAGE_SUFFIX = "_rescorer";

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

    private final LongHistogram tookDurationTotalMillisHistogram;
    private final LongCounter responseCountTotalCounter;

    public SearchResponseMetrics(MeterRegistry meterRegistry) {
        this(
            meterRegistry.registerLongHistogram(
                TOOK_DURATION_TOTAL_HISTOGRAM_NAME,
                "The SearchResponse.took durations in milliseconds, expressed as a histogram",
                "millis"
            ),
            meterRegistry.registerLongCounter(
                RESPONSE_COUNT_TOTAL_COUNTER_NAME,
                "The cumulative total of search responses with an attribute to describe "
                    + "success, partial failure, or failure, expressed as a single total counter and individual "
                    + "attribute counters",
                "count"
            )
        );
    }

    private SearchResponseMetrics(LongHistogram tookDurationTotalMillisHistogram, LongCounter responseCountTotalCounter) {
        this.tookDurationTotalMillisHistogram = tookDurationTotalMillisHistogram;
        this.responseCountTotalCounter = responseCountTotalCounter;
    }

    public long recordTookTime(long tookTime, SearchUsage searchUsage) {
        tookDurationTotalMillisHistogram.record(tookTime, getAttributes(searchUsage));
        return tookTime;
    }

    public void incrementResponseCount(ResponseCountTotalStatus responseCountTotalStatus, SearchUsage searchUsage) {
        responseCountTotalCounter.incrementBy(1L, getAttributes(searchUsage, responseCountTotalStatus));
    }

    private Map<String, Object> getAttributes(SearchUsage searchUsage) {
        return getAttributes(searchUsage, null);
    }

    private Map<String, Object> getAttributes(SearchUsage searchUsage, ResponseCountTotalStatus status) {
        if (searchUsage == null && status == null) {
            return Map.of();
        }

        Map<String, Object> attributes = new HashMap<>(2);
        if (status != null) {
            attributes.put(RESPONSE_COUNT_TOTAL_STATUS_ATTRIBUTE_NAME, status.getDisplayName());
        }
        if (searchUsage != null) {
            searchUsage.getQueryUsage().forEach(q -> attributes.put(q + QUERY_USAGE_SUFFIX, true));
            searchUsage.getSectionsUsage().forEach(q -> attributes.put(q + SECTION_USAGE_SUFFIX, true));
            searchUsage.getRescorerUsage().forEach(q -> attributes.put(q + RESCORER_USAGE_SUFFIX, true));
        }

        logger.info("Attributes :" + attributes);
        return attributes;
    }
}
