/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

public class SearchRestMetrics {
    private final LongCounter searchRequestCount;
    private final LongCounter searchDurationTotalMillisCount;
    private final LongHistogram searchDurationsMillisHistogram;

    public SearchRestMetrics(MeterRegistry meterRegistry) {
        this(
            meterRegistry.registerLongCounter(
                "es.rest.search_requests.count",
                "The total number of search requests expressed as a counter",
                "count"
            ),
            meterRegistry.registerLongCounter(
                "es.rest.search_requests_times.total",
                "The total time of search requests in milliseconds expressed as a counter",
                "millis"
            ),
            meterRegistry.registerLongHistogram(
                "es.rest.search_requests_times.histogram",
                "The timing percentiles of search requests in milliseconds expressed as a histogram",
                "millis"
            )
        );
    }

    SearchRestMetrics(
        LongCounter searchRequestCount,
        LongCounter searchDurationTotalMillisCount,
        LongHistogram searchDurationsMillisHistogram
    ) {
        this.searchRequestCount = searchRequestCount;
        this.searchDurationTotalMillisCount = searchDurationTotalMillisCount;
        this.searchDurationsMillisHistogram = searchDurationsMillisHistogram;
    }

    public LongCounter getSearchRequestCount() {
        return searchRequestCount;
    }

    public LongCounter getSearchDurationTotalMillisCount() {
        return searchDurationTotalMillisCount;
    }

    public LongHistogram getSearchDurationsMillisHistogram() {
        return searchDurationsMillisHistogram;
    }
}
