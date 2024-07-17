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
 * APM metrics that concern failure stores.
 */
public class FailureStoreMetrics {

    public static final FailureStoreMetrics NOOP = new FailureStoreMetrics(MeterRegistry.NOOP);

    public static final String METRIC_TOTAL = "es.data_stream.ingest.documents.total";
    public static final String METRIC_FAILURE_STORE = "es.data_stream.ingest.documents.failure_store.total";
    public static final String METRIC_REJECTED = "es.data_stream.ingest.documents.rejected.total";

    private final LongCounter totalCounter;
    private final LongCounter failureStoreCounter;
    private final LongCounter rejectedCounter;

    public FailureStoreMetrics(MeterRegistry meterRegistry) {
        totalCounter = meterRegistry.registerLongCounter(METRIC_TOTAL, "total number of documents that were sent to a data stream", "unit");
        failureStoreCounter = meterRegistry.registerLongCounter(
            METRIC_FAILURE_STORE,
            "number of documents that got redirected to the failure store",
            "unit"
        );
        rejectedCounter = meterRegistry.registerLongCounter(METRIC_REJECTED, "number of documents that were rejected", "unit");
    }

    public void incrementTotal(String dataStream) {
        totalCounter.incrementBy(1, Map.of("data_stream", dataStream));
    }

    public void incrementFailureStore(String dataStream, String errorType, ErrorLocation errorLocation) {
        failureStoreCounter.incrementBy(
            1,
            Map.of("data_stream", dataStream, "error_type", errorType, "error_location", errorLocation.name())
        );
    }

    public void incrementRejected(String dataStream, String errorType, ErrorLocation errorLocation, boolean failureStore) {
        rejectedCounter.incrementBy(
            1,
            Map.of(
                "data_stream",
                dataStream,
                "error_type",
                errorType,
                "error_location",
                errorLocation.name(),
                "failure_store",
                failureStore
            )
        );
    }

    public enum ErrorLocation {
        PIPELINE,
        SHARD;
    }
}
