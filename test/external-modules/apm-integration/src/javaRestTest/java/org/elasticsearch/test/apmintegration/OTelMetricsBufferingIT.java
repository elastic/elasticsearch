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
        .setting("telemetry.export.endpoint", () -> "http://" + recordingApmServer.getHttpAddress())
        .setting("telemetry.metrics.buffer.disk_size", "10mb")
        .setting("telemetry.metrics.buffer.ttl", "5m")
        // export.interval must be > export.send_timeout so that the OTLP exporter can fully fail, and so that PeriodicMetricReader does
        // not skip an export cycle
        .setting("telemetry.export.interval", "500ms")
        .setting("telemetry.export.send_timeout", "300ms")
        .build();

    // make it bigger than export.send_timeout to allow the OTLP exporter to fully fail, and delegate to the disk buffering exporter
    private static final TimeValue SLEEP_BETWEEN_RUNS = TimeValue.timeValueMillis(400);

    // The disk buffer uses fixed production rotation windows (read-min-age 33s), so a buffered batch only becomes
    // drainable ~33s after it is written. Poll for more than that window for the post-recovery drain to complete.
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

        // Buffer several batches during the outage to exercise the drain loop over more than one stored entry.
        // deltaPreferred() resets the SDK delta after each successful disk write, so each iteration contributes one
        // batch; the sleep lets each export fully fail before the next. They become drainable once the fixed
        // production read-min-age window elapses after recovery.
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

        assertBusy(() -> {
            long written = writtenFiles.get();
            long replayed = replayedFiles.get();
            assertTrue(
                "expected the drain loop to replay all disk-buffered batches after recovery "
                    + "(es.apm.metrics.disk_buffer.writes peaked at "
                    + written
                    + ", es.apm.metrics.disk_buffer.replays peaked at "
                    + replayed
                    + ")",
                written > 0 && written == replayed
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
