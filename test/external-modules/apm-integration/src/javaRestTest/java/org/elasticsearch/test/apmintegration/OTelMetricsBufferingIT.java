/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import io.opentelemetry.sdk.common.Clock;

import org.elasticsearch.client.Request;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * End-to-end coverage for {@code BufferingMetricExporter}: persists batches when OTLP is
 * unreachable and replays them after recovery, preserving original collection timestamps.
 */
public class OTelMetricsBufferingIT extends AbstractMetricsIT {

    public static RecordingApmServer recordingApmServer = new RecordingApmServer();

    public static ElasticsearchCluster cluster = AbstractMetricsIT.baseClusterBuilder()
        .systemProperty("telemetry.otel.metrics.enabled", "true")
        .systemProperty("telemetry.metrics.otel_jvm.enabled", "true")
        .setting("telemetry.export.endpoint", () -> recordingApmServer.getGrpcEndpoint())
        .setting("telemetry.metrics.buffer.disk_size", "10mb")
        .setting("telemetry.metrics.buffer.ttl", "5m")
        // interval > send_timeout > initial_backoff so a failing export fully fails within an interval and the
        // PeriodicMetricReader does not skip a cycle.
        .setting("telemetry.export.interval", "1000ms")
        .setting("telemetry.export.send_timeout", "600ms")
        .build();

    // make it bigger than export.send_timeout to allow the OTLP exporter to fully fail, and delegate to the disk buffering exporter
    private static final TimeValue SLEEP_BETWEEN_RUNS = TimeValue.timeValueMillis(800);

    // Poll past the fixed production read-min-age window (33s) before a buffered batch becomes drainable.
    private static final int BUFFER_DRAIN_TIMEOUT = 60;

    // use the same clock implementation as the OTel SDK itself
    private static final Clock otelClock = Clock.getDefault();

    @ClassRule
    public static TestRule ruleChain = AbstractMetricsIT.buildRuleChain(recordingApmServer, cluster);

    @Override
    protected RecordingApmServer apmServer() {
        return recordingApmServer;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testOutageBuffersToDiskAndDrainsOnRecovery() throws Exception {
        waitForMetricCollectionGreen();

        long outageStartEpochNanos = otelClock.now();
        recordingApmServer.setResponseCode(503);

        // Buffer several batches to exercise the drain loop: deltaPreferred() resets the delta after each disk write,
        // so each iteration contributes one batch and the sleep lets each export fully fail before the next.
        final int BUFFER_BATCHES = 3;
        for (int i = 0; i < BUFFER_BATCHES; i++) {
            client().performRequest(new Request("GET", "/_use_apm_metrics"));
            client().performRequest(new Request("GET", "/_flush_telemetry"));
            Thread.sleep(SLEEP_BETWEEN_RUNS.millis());
        }

        long outageEndEpochNanos = otelClock.now();

        AtomicLong writtenFiles = new AtomicLong();
        AtomicLong replayedFiles = new AtomicLong();
        AtomicBoolean outageWindowBatchReplayed = new AtomicBoolean();

        recordingApmServer.addMessageConsumer(msg -> {
            if (msg instanceof ReceivedTelemetry.ReceivedMetricSet m && "elasticsearch".equals(m.instrumentationScopeName())) {
                long timestamp = m.collectionTime();
                if (timestamp >= outageStartEpochNanos && timestamp <= outageEndEpochNanos) {
                    outageWindowBatchReplayed.set(true);
                }

                writtenFiles.getAndAdd(longSample(m, "es.apm.metrics.disk_buffer.writes"));
                replayedFiles.getAndAdd(longSample(m, "es.apm.metrics.disk_buffer.replays"));
            }
        });

        recordingApmServer.clearResponseCode();

        client().performRequest(new Request("GET", "/_flush_telemetry"));

        // Assert only the end-to-end property: batches buffered to disk during the outage are replayed after
        // recovery. Exact writes==replays accounting is covered deterministically by BufferingMetricExporterTests
        // with injected millisecond rotation windows. Asserting strict equality here is racy because the
        // PeriodicMetricReader keeps adding writes throughout the outage and the final write can land just before
        // recovery, so its file does not age into the 33s readable window until after the earlier batches drain.
        assertBusy(() -> {
            long written = writtenFiles.get();
            long replayed = replayedFiles.get();
            assertTrue(
                "expected disk-buffered batches to be replayed after recovery "
                    + "(es.apm.metrics.disk_buffer.writes peaked at "
                    + written
                    + ", es.apm.metrics.disk_buffer.replays peaked at "
                    + replayed
                    + ")",
                written > 0 && replayed > 0
            );
        }, BUFFER_DRAIN_TIMEOUT, TimeUnit.SECONDS);
        assertBusy(
            () -> assertTrue("expected a replayed metricset carrying an outage-window timestamp", outageWindowBatchReplayed.get()),
            BUFFER_DRAIN_TIMEOUT,
            TimeUnit.SECONDS
        );
    }

    private static void waitForMetricCollectionGreen() throws IOException, InterruptedException {
        CountDownLatch baselineLatch = new CountDownLatch(1);
        recordingApmServer.addMessageConsumer(msg -> {
            if (msg instanceof ReceivedTelemetry.ReceivedMetricSet m && "elasticsearch".equals(m.instrumentationScopeName())) {
                baselineLatch.countDown();
            }
        });
        client().performRequest(new Request("GET", "/_flush_telemetry"));
        assertTrue("Timed out waiting for baseline metrics", baselineLatch.await(TELEMETRY_TIMEOUT, TimeUnit.SECONDS));
    }
}
