/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;

/**
 * A record APM metrics that concern failure stores.
 */
public record FailureStoreMetrics(LongCounter failureCounter, LongCounter redirectCounter) {

    public static final FailureStoreMetrics NOOP = new FailureStoreMetrics(MeterRegistry.NOOP);

    public static final String METRIC_FAILURE_TOTAL = "es.ingest.failures.total";
    public static final String METRIC_REDIRECT_TOTAL = "es.ingest.failure_redirects.total";

    public FailureStoreMetrics(MeterRegistry meterRegistry) {
        this(
            meterRegistry.registerLongCounter(METRIC_FAILURE_TOTAL, "ingest failure counter", "unit"),
            meterRegistry.registerLongCounter(METRIC_REDIRECT_TOTAL, "ingest failure redirect counter", "unit")
        );
    }

    public static void incrementForIndex(LongCounter counter, String index) {
        counter.incrementBy(1, Map.of("index", index));
    }
}
