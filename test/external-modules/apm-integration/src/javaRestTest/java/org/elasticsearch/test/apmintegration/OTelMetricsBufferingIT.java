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
import java.util.concurrent.atomic.AtomicLong;

/**
 * End-to-end coverage for {@code BufferingMetricExporter}: persists batches when OTLP is
 * unreachable and replays them after recovery, preserving original collection timestamps.
 */
public class OTelMetricsBufferingIT extends AbstractMetricsIT {

    public static RecordingApmServer recordingApmServer = new RecordingApmServer();

    public static ElasticsearchCluster cluster = AbstractMetricsIT.baseClusterBuilder()
        .systemProperty("telemetry.otel.metrics.enabled", "true")
        .setting("telemetry.otel.metrics.endpoint", () -> "http://" + recordingApmServer.getHttpAddress() + "/v1/metrics")
        .setting("telemetry.otel.metrics.disk_buffer_size", "10mb")
        .setting("telemetry.otel.metrics.buffer_ttl", "5m")
        // Tight write/read windows so buffered files become drainable within the test budget.
        .setting("telemetry.otel.metrics.disk_buffer_write_window", "100ms")
        .setting("telemetry.otel.metrics.disk_buffer_read_min_age", "200ms")
        // metrics.interval must be > otlp.send_timeout so that the OTLP exporter can fully fail, and so that PeriodicMetricReader does not
        // skip an export cycle
        .setting("telemetry.otel.metrics.interval", "500ms")
        .setting("telemetry.otel.otlp.send_timeout", "300ms")
        // initial_backoff that is way smaller than otlp.send_timeout allows the exporter to fully exhaust the retries
        .setting("telemetry.otel.otlp.retry.initial_backoff", "100ms")
        .build();

    // make it bigger than otlp.send_timeout to allow the OTLP exporter to fully fail, and delegate to the disk buffering exporter
    private static final TimeValue SLEEP_BETWEEN_RUNS = TimeValue.timeValueMillis(400);

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

        // Produce BUFFER_BATCHES files to verify the drain loop iterates beyond the first.
        // One file per iteration is all we can get: deltaPreferred() resets the SDK delta after
        // each successful disk write, so subsequent flushes without new metric values produce nothing.
        // The sleep ensures each file has aged past disk_buffer_read_min_age before the drain.
        final int BUFFER_BATCHES = 3;
        for (int i = 0; i < BUFFER_BATCHES; i++) {
            client().performRequest(new Request("GET", "/_use_apm_metrics"));
            client().performRequest(new Request("GET", "/_flush_telemetry"));
            Thread.sleep(SLEEP_BETWEEN_RUNS.millis());
        }

        long outageEndEpochNanos = otelClock.now();

        CountDownLatch backlogReplayed = new CountDownLatch(1);
        CountDownLatch outageWindowBatchReplayed = new CountDownLatch(1);
        AtomicLong writtenFiles = new AtomicLong();
        AtomicLong replayedFiles = new AtomicLong();

        recordingApmServer.addMessageConsumer(msg -> {
            if (msg instanceof ReceivedTelemetry.ReceivedMetricSet m && "elasticsearch".equals(m.instrumentationScopeName())) {
                long timestamp = m.collectionTime();
                if (timestamp >= outageStartEpochNanos && timestamp <= outageEndEpochNanos) {
                    outageWindowBatchReplayed.countDown();
                }

                writtenFiles.getAndAdd(longSample(m, "es.apm.metrics.disk_buffer.writes"));
                replayedFiles.getAndAdd(longSample(m, "es.apm.metrics.disk_buffer.replays"));
                if (writtenFiles.get() > 0 && writtenFiles.get() == replayedFiles.get()) {
                    backlogReplayed.countDown();
                }
            }
        });

        recordingApmServer.clearResponseCode();

        client().performRequest(new Request("GET", "/_flush_telemetry"));

        assertTrue(
            "expected the drain loop to replay all disk-buffered batches after recovery "
                + "(es.apm.metrics.disk_buffer.writes peaked at "
                + writtenFiles.get()
                + ", es.apm.metrics.disk_buffer.replays peaked at "
                + replayedFiles.get()
                + ")",
            backlogReplayed.await(TELEMETRY_TIMEOUT, TimeUnit.SECONDS)
        );
        assertTrue(
            "expected a replayed metricset carrying an outage-window timestamp",
            outageWindowBatchReplayed.await(TELEMETRY_TIMEOUT, TimeUnit.SECONDS)
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
