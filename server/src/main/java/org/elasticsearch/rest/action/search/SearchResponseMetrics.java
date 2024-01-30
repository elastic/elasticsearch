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

    public static final String TOOK_MEASUREMENT_NUM_COUNT_NAME = "es.search_response.took_measurements.total";
    public static final String TOOK_DURATION_TOTAL_NAME = "es.search_response.took_duration.total";
    public static final String TOOK_DURATION_TOTAL_HISTOGRAM_NAME = "es.search_response.took_durations.histogram";

    private final LongCounter tookMeasurementsNumCount;
    private final LongCounter tookDurationTotalMillisCount;
    private final LongHistogram tookDurationTotalMillisHistogram;

    public SearchResponseMetrics(MeterRegistry meterRegistry) {
        this(
            meterRegistry.registerLongCounter(
                TOOK_MEASUREMENT_NUM_COUNT_NAME,
                "The total number of times that SearchResponse.took measurements were recorded, "
                    + "expressed as a counter. Used for calculating averages.",
                "count"
            ),
            meterRegistry.registerLongCounter(
                TOOK_DURATION_TOTAL_NAME,
                "The total value of all SearchResponse.took durations in milliseconds, expressed as a counter",
                "millis"
            ),
            meterRegistry.registerLongHistogram(
                TOOK_DURATION_TOTAL_HISTOGRAM_NAME,
                "The SearchResponse.took durations in milliseconds, expressed as a histogram",
                "millis"
            )
        );
    }

    SearchResponseMetrics(
        LongCounter tookMeasurementsNumCount,
        LongCounter tookDurationTotalMillisCount,
        LongHistogram tookDurationTotalMillisHistogram
    ) {
        this.tookMeasurementsNumCount = tookMeasurementsNumCount;
        this.tookDurationTotalMillisCount = tookDurationTotalMillisCount;
        this.tookDurationTotalMillisHistogram = tookDurationTotalMillisHistogram;
    }

    public long recordTookTime(long tookTime) {
        tookMeasurementsNumCount.increment();
        tookDurationTotalMillisCount.incrementBy(tookTime);
        tookDurationTotalMillisHistogram.record(tookTime);
        return tookTime;
    }
}
