/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

public class SearchResponseMetrics {

    public static final String TOOK_DURATION_TOTAL_HISTOGRAM_NAME = "es.search_response.took_durations.histogram";

    private final LongHistogram tookDurationTotalMillisHistogram;

    public SearchResponseMetrics(MeterRegistry meterRegistry) {
        this(
            meterRegistry.registerLongHistogram(
                TOOK_DURATION_TOTAL_HISTOGRAM_NAME,
                "The SearchResponse.took durations in milliseconds, expressed as a histogram",
                "millis"
            )
        );
    }

    private SearchResponseMetrics(LongHistogram tookDurationTotalMillisHistogram) {
        this.tookDurationTotalMillisHistogram = tookDurationTotalMillisHistogram;
    }

    public long recordTookTime(long tookTime) {
        tookDurationTotalMillisHistogram.record(tookTime);
        return tookTime;
    }
}
