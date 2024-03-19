/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

public class SearchResponseMetrics {

    public static final String TOOK_DURATION_TOTAL_HISTOGRAM_NAME = "es.search_response.took_durations.histogram";

    public static final String TIMEOUT_NAME = "es.search_response.timeout.total";

    private final LongHistogram tookDurationTotalMillisHistogram;

    private final LongCounter searchTimouts;

    public SearchResponseMetrics(MeterRegistry meterRegistry) {
        this(
            meterRegistry.registerLongHistogram(
                TOOK_DURATION_TOTAL_HISTOGRAM_NAME,
                "The SearchResponse.took durations in milliseconds, expressed as a histogram",
                "millis"
            ),
            meterRegistry.registerLongCounter(
                TIMEOUT_NAME,
                "Count of SearchResponse instances that have reported timeout",
                "count"
            )
        );
    }

    private SearchResponseMetrics(LongHistogram tookDurationTotalMillisHistogram, LongCounter searchTimouts) {
        this.tookDurationTotalMillisHistogram = tookDurationTotalMillisHistogram;
        this.searchTimouts = searchTimouts;
    }

    public void recordTookTime(long tookTime) {
        tookDurationTotalMillisHistogram.record(tookTime);
    }

    public void countTimeout() {
        this.searchTimouts.increment();
    }
}
