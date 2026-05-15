/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableLongPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.resources.Resource;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.apm.RecordingOtelMeter;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.elasticsearch.telemetry.InstrumentType.LONG_COUNTER;
import static org.elasticsearch.telemetry.InstrumentType.LONG_GAUGE;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class QueueingMetricExporterTests extends ESTestCase {

    private QueueingMetricExporter exporter;
    private FakeMetricExporter delegate;
    private RecordingOtelMeter meter;

    @Before
    public void setup() {
        meter = new RecordingOtelMeter();
        delegate = new FakeMetricExporter();
        Settings settings = Settings.builder().put("telemetry.otel.metrics.export_queue_size", 2).build();
        exporter = new QueueingMetricExporter(delegate, settings, meter);
    }

    @After
    public void shutdownExporter() {
        if (exporter != null) {
            exporter.shutdown();
        }
    }

    public void testExportReturnsImmediatelyAndIsDrainedByWorker() throws Exception {
        CompletableResultCode result = exporter.export(List.of(metric("m1")));

        assertTrue(result.isDone());
        assertTrue(result.isSuccess());

        assertBusy(() -> assertThat(delegate.exportedNames().size(), greaterThanOrEqualTo(1)));
        assertBusy(() -> assertThat(counterMeasurements("es.apm.metrics.export_queue.drained"), not(empty())));
    }

    public void testFullQueueDropsIncomingBatchAndPreservesQueued() throws Exception {
        delegate.block();

        exporter.export(List.of(metric("m0")));
        assertBusy(() -> assertThat(currentQueueDepth(), equalTo(0L)));
        exporter.export(List.of(metric("m1")));
        exporter.export(List.of(metric("m2")));
        assertThat(currentQueueDepth(), equalTo(2L));

        CompletableResultCode dropped = exporter.export(List.of(metric("m3")));
        assertTrue("export() must always report success while the queue is open", dropped.isSuccess());
        assertThat("queue depth unchanged: newest was dropped, not accepted", currentQueueDepth(), equalTo(2L));

        delegate.release();
        assertBusy(() -> assertThat(delegate.exportedNames(), equalTo(List.of("m0", "m1", "m2"))));
        assertThat(counterMeasurements("es.apm.metrics.export_queue.dropped"), hasSize(1));
    }

    public void testFlushWaitsForInflightWorkerExport() throws Exception {
        delegate.block();
        exporter.export(List.of(metric("m1")));
        assertBusy(() -> assertThat(delegate.inflight(), equalTo(1)));

        CompletableFuture<Void> flushing = CompletableFuture.runAsync(exporter::flush);

        // flush() must still be blocked while the delegate is gated.
        Thread.sleep(100);
        assertFalse("flush() returned without waiting for the in-flight export", flushing.isDone());

        delegate.release();
        assertBusy(() -> assertTrue("flush() did not return after the delegate was released", flushing.isDone()));
    }

    private List<Measurement> counterMeasurements(String name) {
        return meter.getRecorder().getMeasurements(LONG_COUNTER, name);
    }

    private long currentQueueDepth() {
        meter.collectMetrics();
        List<Measurement> measurements = meter.getRecorder().getMeasurements(LONG_GAUGE, "es.apm.metrics.export_queue.depth");
        return measurements.isEmpty() ? 0L : measurements.getLast().getLong();
    }

    private static MetricData metric(String name) {
        return ImmutableMetricData.createLongGauge(
            Resource.getDefault(),
            InstrumentationScopeInfo.create("test"),
            name,
            "test metric",
            "1",
            ImmutableGaugeData.create(List.of(ImmutableLongPointData.create(0L, 0L, Attributes.empty(), 1L)))
        );
    }

}
