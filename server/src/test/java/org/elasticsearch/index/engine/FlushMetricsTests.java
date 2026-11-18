/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.metric.MetricAttributes;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class FlushMetricsTests extends ESTestCase {

    private final RecordingMeterRegistry registry = new RecordingMeterRegistry();
    private final FlushMetrics metrics = new FlushMetrics(registry);

    /**
     * Shaped like a commit that reads an uploaded segment through the blob cache on a stateless indexing shard: the engine wraps the
     * failure, and the blob cache wraps the blob store client error.
     */
    public void testBlobStoreClientFailureCountedInBothCounters() {
        IOException clientError = new IOException("storage.googleapis.com");
        String clientPackage = randomFrom(MergeMetrics.BLOB_STORE_CLIENT_PACKAGES);
        clientError.setStackTrace(new StackTraceElement[] { new StackTraceElement(clientPackage + "Client", "read", "Client.java", 1) });
        IOException cacheError = new IOException(new ExecutionException(clientError));
        IndexMode indexMode = randomFrom(IndexMode.values());

        metrics.onFailure(indexMode, new FlushFailedEngineException(new ShardId("index", "_na_", 0), cacheError));

        Measurement failure = single(FlushMetrics.FLUSH_FAILURE_TOTAL);
        Measurement blobStoreFailure = single(FlushMetrics.FLUSH_FAILURE_BLOBSTORE_TRANSIENT_TOTAL);
        assertThat(blobStoreFailure.attributes(), equalTo(failure.attributes()));
        assertThat(failure.attributes().get(MetricAttributes.ES_INDEX_MODE), equalTo(indexMode.getName()));
        assertThat(failure.attributes().get(MetricAttributes.ERROR_TYPE), equalTo("IOException"));
    }

    public void testOtherFailureIsNotABlobStoreFailure() {
        metrics.onFailure(
            randomFrom(IndexMode.values()),
            new FlushFailedEngineException(new ShardId("index", "_na_", 0), new IOException("disk failure"))
        );

        single(FlushMetrics.FLUSH_FAILURE_TOTAL);
        assertThat(
            registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, FlushMetrics.FLUSH_FAILURE_BLOBSTORE_TRANSIENT_TOTAL),
            hasSize(0)
        );
    }

    private Measurement single(String name) {
        List<Measurement> measurements = registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, name);
        assertThat(name, measurements, hasSize(1));
        assertThat(measurements.get(0).getLong(), equalTo(1L));
        return measurements.get(0);
    }
}
