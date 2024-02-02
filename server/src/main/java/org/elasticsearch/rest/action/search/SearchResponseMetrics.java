/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

public class SearchResponseMetrics {
    private static final Logger logger = LogManager.getLogger(SearchResponseMetrics.class);
    public static final String TOOK_DURATION_TOTAL_HISTOGRAM_NAME = "es.search_response.took_durations.histogram";
    public static final String FAILED_SHARDS_HISTOGRAM_NAME = "es.search_response.failed_shards.histogram";

    private final LongHistogram tookDurationTotalMillisHistogram;
    private final LongHistogram failedShardsHistogram;

    public SearchResponseMetrics(MeterRegistry meterRegistry) {
        this(
            meterRegistry.registerLongHistogram(
                TOOK_DURATION_TOTAL_HISTOGRAM_NAME,
                "The SearchResponse.took durations in milliseconds, expressed as a histogram",
                "millis"
            ),
            meterRegistry.registerLongHistogram(
                FAILED_SHARDS_HISTOGRAM_NAME,
                "Number of failed shards per non-errored search, expressed as a histogram",
                "count"
            )
        );
    }

    private SearchResponseMetrics(LongHistogram tookDurationTotalMillisHistogram, LongHistogram failedShardsHistogram) {
        this.tookDurationTotalMillisHistogram = tookDurationTotalMillisHistogram;
        this.failedShardsHistogram = failedShardsHistogram;
    }

    public long recordTookTime(long tookTime) {
        tookDurationTotalMillisHistogram.record(tookTime);
        return tookTime;
    }

    public void recordFailedShardsCount(long numFailedShards) {
        failedShardsHistogram.record(numFailedShards);
    }
}
