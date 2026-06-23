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

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.apm.RecordingOtelMeterProvider;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.telemetry.InstrumentType.LONG_COUNTER;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class BufferingMetricExporterTests extends ESTestCase {

    private RecordingOtelMeterProvider meterProvider;
    private Path bufferDir;
    private FakeMetricExporter delegate;
    private BufferingMetricExporter exporter;

    @Before
    public void setupCommon() throws Exception {
        meterProvider = new RecordingOtelMeterProvider();
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
            // Short rotation/read times so the suite runs in seconds rather than minutes.
            .put("telemetry.otel.metrics.disk_buffer_write_window", "100ms")
            .put("telemetry.otel.metrics.disk_buffer_read_min_age", "200ms")
            .put(overrides)
            .build();
        exporter = new BufferingMetricExporter(delegate, merged, bufferDir, () -> meterProvider);
    }

    private CompletableResultCode exportAndWait(String name) {
        return exporter.export(List.of(metric(name))).join(1, TimeUnit.SECONDS);
    }

    public void testFailBufferRecoverAndDrain() throws Exception {
        build(Settings.EMPTY);
        delegate.setShouldFail(true);
        assertTrue("buffered batch must report success", exportAndWait("buf").isSuccess());
        assertBusy(() -> assertThat(countBufferFiles(), greaterThanOrEqualTo(1)));
        assertThat(counter("writes"), hasSize(1));

        delegate.setShouldFail(false);
        safeSleep(300); // past the test-injected minFileAgeForRead (200ms)
        exportAndWait("trigger");

        assertBusy(() -> assertThat(countBufferFiles(), equalTo(0)));
        assertThat(delegate.exportedNames(), hasItems("buf", "trigger"));
        assertBusy(() -> assertThat(counter("replays"), hasSize(1)));
    }

    public void testDiskCapRotatesOldestToMakeRoom() throws Exception {
        build(Settings.builder().put("telemetry.otel.metrics.disk_buffer_size", "1kb").build());

        delegate.setShouldFail(true);
        assertTrue(exportAndWait("first").isSuccess());
        assertTrue(exportAndWait("second").isSuccess());
        assertThat(counter("writes"), hasSize(2));
        assertThat(countBufferFiles(), equalTo(1));
    }

    public void testTtlExpiredFilesAreNotReplayed() throws Exception {
        // TTL must be greater than the test-injected minFileAgeForRead (200ms)
        build(Settings.builder().put("telemetry.otel.metrics.buffer_ttl", "300ms").build());

        delegate.setShouldFail(true);
        exportAndWait("expires");
        assertBusy(() -> assertThat(countBufferFiles(), greaterThanOrEqualTo(1)));

        safeSleep(500);

        delegate.clearExported();
        delegate.setShouldFail(false);
        exportAndWait("trigger");
        assertThat(counter("replays"), empty());
        assertThat(delegate.exportedNames(), not(hasItem("expires")));
    }

    public void testShutdownLeavesFilesIfDelegateBroken() throws Exception {
        build(Settings.EMPTY);
        delegate.setShouldFail(true);
        exportAndWait("m1");
        assertBusy(() -> assertThat(countBufferFiles(), greaterThanOrEqualTo(1)));

        exporter.shutdown();
        exporter = null;

        assertThat("buffered file must remain on disk for next startup", countBufferFiles(), greaterThanOrEqualTo(1));
    }

    private List<Measurement> counter(String suffix) {
        return meterProvider.meter().getRecorder().getMeasurements(LONG_COUNTER, "es.apm.metrics.disk_buffer." + suffix);
    }

    private int countBufferFiles() throws Exception {
        if (Files.isDirectory(bufferDir) == false) {
            return 0;
        }
        int n = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(bufferDir)) {
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
