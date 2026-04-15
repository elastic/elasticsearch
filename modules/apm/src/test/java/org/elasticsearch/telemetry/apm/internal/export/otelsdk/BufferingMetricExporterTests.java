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
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableLongPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.resources.Resource;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class BufferingMetricExporterTests extends ESTestCase {

    private BufferingMetricExporter exporter;

    @After
    public void shutdownExporter() {
        if (exporter != null) {
            exporter.shutdown();
        }
    }

    public void testDiskDropsWhenOverLimit() throws Exception {
        var bufferDir = createTempDir("telemetry-buffer");
        var delegate = new FakeMetricExporter();
        var settings = Settings.builder()
            .put("telemetry.otel.metrics.disk_buffer_size", "1b")
            .put("telemetry.otel.metrics.buffer_ttl", "5m")
            .build();
        exporter = new BufferingMetricExporter(delegate, settings, () -> bufferDir);

        delegate.shouldFail = true;
        exporter.export(List.of(metric("m1", 1)));
        exporter.flush();

        exporter.export(List.of(metric("m2", 2)));
        exporter.flush();

        assertThat(listBufferFiles(bufferDir), equalTo(1));
    }

    public void testTtlEviction() throws Exception {
        var bufferDir = createTempDir("telemetry-buffer");
        var delegate = new FakeMetricExporter();
        var settings = Settings.builder()
            .put("telemetry.otel.metrics.disk_buffer_size", "10mb")
            .put("telemetry.otel.metrics.buffer_ttl", "1ms")
            .build();
        exporter = new BufferingMetricExporter(delegate, settings, () -> bufferDir);

        delegate.shouldFail = true;
        exporter.export(List.of(metric("old", 1)));
        exporter.flush();
        assertThat(listBufferFiles(bufferDir), equalTo(1));

        safeSleep(10);

        // Still failing — drain cannot delete files, only TTL eviction can
        exporter.export(List.of(metric("new", 2)));
        exporter.flush();

        // The first file was evicted by TTL; only the second remains
        assertThat(listBufferFiles(bufferDir), equalTo(1));
    }

    public void testShutdownDrainsDisk() throws Exception {
        var bufferDir = createTempDir("telemetry-buffer");
        var delegate = new FakeMetricExporter();
        var settings = Settings.builder()
            .put("telemetry.otel.metrics.disk_buffer_size", "10mb")
            .put("telemetry.otel.metrics.buffer_ttl", "5m")
            .build();
        exporter = new BufferingMetricExporter(delegate, settings, () -> bufferDir);

        delegate.shouldFail = true;
        exporter.export(List.of(metric("m1", 1)));
        exporter.flush();

        delegate.shouldFail = false;
        delegate.exported.clear();
        exporter.shutdown();

        boolean found = delegate.exported.stream().anyMatch(m -> "m1".equals(m.getName()));
        assertTrue("m1 should have been replayed during shutdown", found);
    }

    private static MetricData metric(String name, long value) {
        return ImmutableMetricData.createLongGauge(
            Resource.getDefault(),
            InstrumentationScopeInfo.create("test"),
            name,
            "test metric",
            "1",
            ImmutableGaugeData.create(
                List.of(ImmutableLongPointData.create(System.nanoTime() - 10_000_000_000L, System.nanoTime(), Attributes.empty(), value))
            )
        );
    }

    private static int listBufferFiles(Path dir) throws Exception {
        int count = 0;
        try (var stream = Files.newDirectoryStream(dir, "metrics-*.bin")) {
            for (Path ignored : stream) {
                count++;
            }
        }
        return count;
    }

    private static class FakeMetricExporter implements MetricExporter {
        final List<MetricData> exported = new ArrayList<>();
        volatile boolean shouldFail = false;

        @Override
        public CompletableResultCode export(Collection<MetricData> metrics) {
            exported.addAll(metrics);
            return shouldFail ? CompletableResultCode.ofFailure() : CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode flush() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode shutdown() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
            return AggregationTemporality.DELTA;
        }
    }
}
