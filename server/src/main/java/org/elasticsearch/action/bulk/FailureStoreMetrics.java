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
 * A class containing APM metrics for failure stores. See the JavaDoc on the individual methods for an explanation on what they're tracking.
 * General notes:
 * <ul>
 *     <li>When a document is rerouted in a pipeline, the destination data stream is used for the metric attribute(s).</li>
 * </ul>
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

    /**
     * This counter tracks the number of documents that we <i>tried</i> to index into a data stream. This includes documents
     * that were dropped by a pipeline. This counter will only be incremented once for every incoming document (even when it gets
     * redirected to the failure store and/or gets rejected).
     * @param dataStream the name of the data stream
     */
    public void incrementTotal(String dataStream) {
        totalCounter.incrementBy(1, Map.of("data_stream", dataStream));
    }

    /**
     * This counter tracks the number of documents that we <i>tried</i> to store into a failure store. This includes both pipeline and
     * shard-level failures.
     * @param dataStream the name of the data stream
     * @param errorType the error type (i.e. the name of the exception that was thrown)
     * @param errorLocation where this failure occurred
     */
    public void incrementFailureStore(String dataStream, String errorType, ErrorLocation errorLocation) {
        failureStoreCounter.incrementBy(
            1,
            Map.of("data_stream", dataStream, "error_type", errorType, "error_location", errorLocation.name())
        );
    }

    /**
     * This counter tracks the number of documents that failed to get stored in Elasticsearch. Meaning, any document that did not get
     * stored in the data stream or in its failure store.
     * @param dataStream the name of the data stream
     * @param errorType the error type (i.e. the name of the exception that was thrown)
     * @param errorLocation where this failure occurred
     * @param failureStore whether this failure occurred while trying to ingest into a failure store (<code>true</code>) or in the data
     * stream itself (<code>false</code>)
     */
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
