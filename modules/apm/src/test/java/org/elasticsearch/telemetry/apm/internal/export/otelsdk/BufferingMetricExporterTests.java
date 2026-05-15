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
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.serializer.MetricDataSerializer;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.telemetry.InstrumentType.LONG_COUNTER;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class BufferingMetricExporterTests extends ESTestCase {

    private RecordingOtelMeter meter;
    private Path bufferDir;
    private FakeMetricExporter delegate;
    private BufferingMetricExporter exporter;

    @Before
    public void setupCommon() throws Exception {
        meter = new RecordingOtelMeter();
        bufferDir = createTempDir("telemetry-buffer");
        delegate = new FakeMetricExporter();
    }

    @After
    public void shutdownExporter() {
        if (exporter != null) {
            exporter.shutdown();
        }
    }

    private void build(Settings overrides) {
        Settings merged = Settings.builder()
            .put("telemetry.otel.metrics.disk_buffer_size", "10mb")
            .put("telemetry.otel.metrics.buffer_ttl", "5m")
            .put(overrides)
            .build();
        exporter = new BufferingMetricExporter(delegate, merged, bufferDir, meter);
    }

    private CompletableResultCode exportAndWait(String name) {
        CompletableResultCode r = exporter.export(List.of(metric(name)));
        r.join(1, TimeUnit.SECONDS);
        return r;
    }

    public void testFailBufferRecoverAndDrain() throws Exception {
        build(Settings.EMPTY);
        delegate.setShouldFail(true);
        assertTrue("buffered batch must report success", exportAndWait("buf").isSuccess());
        assertThat(listBufferFiles(), equalTo(1));
        assertThat(counter("writes"), hasSize(1));

        delegate.setShouldFail(false);
        exportAndWait("trigger");

        assertBusy(() -> assertThat(listBufferFiles(), equalTo(0)));
        assertThat(delegate.exportedNames(), hasItems("buf", "trigger"));
        assertThat(counter("replays"), hasSize(1));
    }

    public void testDiskCapDropsNewBatchesAndReportsFailure() throws Exception {
        build(Settings.builder().put("telemetry.otel.metrics.disk_buffer_size", "1b").build());

        delegate.setShouldFail(true);
        assertTrue(exportAndWait("first").isSuccess());
        assertFalse("dropped batch must report failure", exportAndWait("second").isSuccess());
        assertThat(listBufferFiles(), equalTo(1));
        assertThat(counter("drops_full"), hasSize(1));
    }

    public void testTtlEvictsExpiredFilesInsteadOfReplaying() throws Exception {
        build(Settings.builder().put("telemetry.otel.metrics.buffer_ttl", "100ms").build());

        delegate.setShouldFail(true);
        exportAndWait("expires");
        assertThat(listBufferFiles(), equalTo(1));

        safeSleep(400);

        // Reset the exported list so we can assert the expired batch isn't replayed.
        delegate.clearExported();
        delegate.setShouldFail(false);
        exportAndWait("trigger");

        assertBusy(() -> assertThat(listBufferFiles(), equalTo(0)));
        assertThat(counter("evictions_ttl"), hasSize(1));
        assertThat(counter("replays"), empty());
        assertThat(delegate.exportedNames(), not(hasItem("expires")));
    }

    public void testPoisonedFileIsDeletedOnFirstDrain() throws Exception {
        Files.writeString(bufferDir.resolve("metrics-1.bin"), "not valid");
        build(Settings.EMPTY);

        exportAndWait("trigger");

        assertBusy(() -> assertThat(listBufferFiles(), equalTo(0)));
    }

    public void testDrainStopsAfterFirstTransientFailure() throws Exception {
        for (int i = 1; i <= 4; i++) {
            try (var out = Files.newOutputStream(bufferDir.resolve("metrics-" + i + ".bin"))) {
                MetricDataSerializer.serialize(List.of(metric("m" + i)), out);
            }
        }
        // Succeeds for the test trigger then fails on every subsequent replay so the drain aborts after the first attempt.
        delegate = new FakeMetricExporter() {
            final AtomicInteger calls = new AtomicInteger();

            @Override
            public CompletableResultCode export(Collection<MetricData> metrics) {
                return calls.getAndIncrement() == 0 ? CompletableResultCode.ofSuccess() : CompletableResultCode.ofFailure();
            }
        };
        build(Settings.EMPTY);

        exportAndWait("trigger");

        assertBusy(() -> assertThat(listBufferFiles(), equalTo(4)));
    }

    public void testShutdownDrainsDiskWhileDelegateIsHealthy() throws Exception {
        build(Settings.EMPTY);
        delegate.setShouldFail(true);
        exportAndWait("m1");
        assertThat(listBufferFiles(), equalTo(1));

        delegate.setShouldFail(false);
        delegate.clearExported();
        exporter.shutdown();
        exporter = null;

        assertThat(delegate.exportedNames(), hasItem("m1"));
        assertThat(listBufferFiles(), equalTo(0));
        assertThat(counter("replays"), hasSize(1));
    }

    public void testShutdownLeavesFilesIfDelegateBroken() throws Exception {
        build(Settings.EMPTY);
        delegate.setShouldFail(true);
        exportAndWait("m1");
        assertThat(listBufferFiles(), equalTo(1));

        exporter.shutdown();
        exporter = null;

        // Delegate still failing: drain attempt fails and the file persists for next startup.
        assertThat("buffered file must remain on disk for next startup", listBufferFiles(), equalTo(1));
    }

    private List<Measurement> counter(String suffix) {
        return meter.getRecorder().getMeasurements(LONG_COUNTER, "es.apm.metrics.disk_buffer." + suffix);
    }

    private int listBufferFiles() throws Exception {
        int n = 0;
        try (var stream = Files.newDirectoryStream(bufferDir, "metrics-*.bin")) {
            for (Path ignored : stream) {
                n++;
            }
        }
        return n;
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
