/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.metric.MetricAttributes;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class MergeMetricsTests extends ESTestCase {

    public void testFailureCarriesIndexModeAndErrorType() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        MergeMetrics metrics = new MergeMetrics(registry);
        IndexMode indexMode = randomFrom(IndexMode.values());
        Throwable error = randomFrom(new IOException(randomAlphaOfLength(5)), new IllegalStateException(randomAlphaOfLength(5)));

        metrics.onFailure(indexMode, error);

        Map<String, Object> expected = Map.of(
            MetricAttributes.ES_INDEX_MODE,
            indexMode.getName(),
            MetricAttributes.ERROR_TYPE,
            error.getClass().getSimpleName()
        );
        Measurement failure = single(registry, InstrumentType.LONG_COUNTER, MergeMetrics.MERGE_FAILURE_TOTAL);
        assertThat(failure.getLong(), equalTo(1L));
        assertThat(failure.attributes(), equalTo(expected));
        assertThat(registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, MergeMetrics.MERGE_ABORTED_TOTAL), hasSize(0));
        assertThat(
            registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, MergeMetrics.MERGE_FAILURE_BLOBSTORE_TRANSIENT_TOTAL),
            hasSize(0)
        );
    }

    public void testBlobStoreTransientFailureCountedInBothCounters() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        MergeMetrics metrics = new MergeMetrics(registry);
        IndexMode indexMode = randomFrom(IndexMode.values());
        // shaped like the traces from logs-program#232 and #239: the blob cache future wait wraps the blob store client error
        String clientPackage = randomFrom(MergeMetrics.BLOB_STORE_CLIENT_PACKAGES);
        Throwable clientError = randomFrom(
            new AlreadyClosedException("client is already closed"),
            new IllegalStateException("storage.googleapis.com")
        );
        clientError.setStackTrace(new StackTraceElement[] { new StackTraceElement(clientPackage + "Client", "read", "Client.java", 1) });
        Throwable error = randomBoolean() ? new IOException(new ExecutionException(clientError)) : clientError;

        metrics.onFailure(indexMode, error);

        Measurement failure = single(registry, InstrumentType.LONG_COUNTER, MergeMetrics.MERGE_FAILURE_TOTAL);
        Measurement blobStoreFailure = single(registry, InstrumentType.LONG_COUNTER, MergeMetrics.MERGE_FAILURE_BLOBSTORE_TRANSIENT_TOTAL);
        assertThat(failure.getLong(), equalTo(1L));
        assertThat(blobStoreFailure.getLong(), equalTo(1L));
        assertThat(blobStoreFailure.attributes(), equalTo(failure.attributes()));
        assertThat(failure.attributes().get(MetricAttributes.ES_INDEX_MODE), equalTo(indexMode.getName()));
        assertThat(registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, MergeMetrics.MERGE_ABORTED_TOTAL), hasSize(0));
    }

    public void testClosedIndexWriterIsNotABlobStoreFailure() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        MergeMetrics metrics = new MergeMetrics(registry);
        Throwable error = new AlreadyClosedException("this IndexWriter is closed");
        error.setStackTrace(new StackTraceElement[] { new StackTraceElement("org.apache.lucene.index.IndexWriter", "ensureOpen", "", 1) });

        metrics.onFailure(randomFrom(IndexMode.values()), randomBoolean() ? new IOException(new ExecutionException(error)) : error);

        single(registry, InstrumentType.LONG_COUNTER, MergeMetrics.MERGE_FAILURE_TOTAL);
        assertThat(
            registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, MergeMetrics.MERGE_FAILURE_BLOBSTORE_TRANSIENT_TOTAL),
            hasSize(0)
        );
    }

    public void testAbortedCarriesIndexMode() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        MergeMetrics metrics = new MergeMetrics(registry);
        IndexMode indexMode = randomFrom(IndexMode.values());

        metrics.onAborted(indexMode);

        Map<String, Object> expected = Map.of(MetricAttributes.ES_INDEX_MODE, indexMode.getName());
        Measurement abort = single(registry, InstrumentType.LONG_COUNTER, MergeMetrics.MERGE_ABORTED_TOTAL);
        assertThat(abort.getLong(), equalTo(1L));
        assertThat(abort.attributes(), equalTo(expected));
        assertThat(registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, MergeMetrics.MERGE_FAILURE_TOTAL), hasSize(0));
        assertThat(
            registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, MergeMetrics.MERGE_FAILURE_BLOBSTORE_TRANSIENT_TOTAL),
            hasSize(0)
        );
    }

    private static Measurement single(RecordingMeterRegistry registry, InstrumentType type, String name) {
        List<Measurement> measurements = registry.getRecorder().getMeasurements(type, name);
        assertThat(name, measurements, hasSize(1));
        return measurements.get(0);
    }
}
