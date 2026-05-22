/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * End-to-end coverage for the OTel SDK export pipeline:
 * <ul>
 *   <li>{@code BufferingMetricExporter}: persists batches when OTLP is unreachable and replays
 *       them after recovery, preserving original collection timestamps.</li>
 *   <li>{@code QueueingMetricExporter}: when APM is slow enough to back up the worker, the queue
 *       drops the <em>newest</em> incoming batches.</li>
 * </ul>
 */
public class OTelMetricsBufferingIT extends AbstractMetricsIT {

    public static RecordingApmServer recordingApmServer = new RecordingApmServer();

    public static ElasticsearchCluster cluster = AbstractMetricsIT.baseClusterBuilder()
        .systemProperty("telemetry.otel.metrics.enabled", "true")
        .setting("telemetry.otel.metrics.endpoint", () -> "http://" + recordingApmServer.getHttpAddress() + "/v1/metrics")
        .setting("telemetry.otel.metrics.interval", "1s")
        .setting("telemetry.otel.metrics.disk_buffer_size", "10mb")
        .setting("telemetry.otel.metrics.buffer_ttl", "5m")
        .setting("telemetry.otel.metrics.export_queue_size", "2")
        .setting("telemetry.otel.metrics.otlp.request_timeout", "1s")
        .build();

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

        long outageStartEpochMs = System.currentTimeMillis();
        recordingApmServer.setResponseCode(503);

        client().performRequest(new Request("GET", "/_use_apm_metrics"));
        client().performRequest(new Request("GET", "/_flush_telemetry"));

        // Sustained outage: ~6 collection cycles at 1s interval, well over the OTLP retry chain.
        // Long enough to accumulate a real backlog so the drain loop has to iterate multiple files.
        Thread.sleep(6000);

        long outageStartEpochNanos = outageStartEpochMs * 1_000_000L;
        long outageEndEpochNanos = System.currentTimeMillis() * 1_000_000L;

        CountDownLatch backlogReplayed = new CountDownLatch(1);
        CountDownLatch outageWindowBatchReplayed = new CountDownLatch(1);
        AtomicLong maxReplaysSeen = new AtomicLong();
        recordingApmServer.addMessageConsumer(msg -> {
            if (msg instanceof ReceivedTelemetry.ReceivedMetricSet m && "elasticsearch".equals(m.instrumentationScopeName())) {
                long timestamp = m.collectionTime();
                if (timestamp >= outageStartEpochNanos && timestamp <= outageEndEpochNanos) {
                    outageWindowBatchReplayed.countDown();
                }
                long replays = longSample(m, "es.apm.metrics.disk_buffer.replays");
                if (replays > 0) {
                    maxReplaysSeen.accumulateAndGet(replays, Math::max);
                }
                // Require ≥2 to verify the drain loop iterated through more than a single file.
                if (maxReplaysSeen.get() >= 2) {
                    backlogReplayed.countDown();
                }
            }
        });

        recordingApmServer.setResponseCode(0);

        client().performRequest(new Request("GET", "/_flush_telemetry"));

        assertTrue(
            "expected the drain loop to replay at least two disk-buffered batches after recovery "
                + "(es.apm.metrics.disk_buffer.replays peaked at "
                + maxReplaysSeen.get()
                + ")",
            backlogReplayed.await(TELEMETRY_TIMEOUT, TimeUnit.SECONDS)
        );
        assertTrue(
            "expected a replayed metricset carrying an outage-window timestamp",
            outageWindowBatchReplayed.await(TELEMETRY_TIMEOUT, TimeUnit.SECONDS)
        );
    }

    /**
     * APM flaps between healthy and 503 several times. Each down phase must persist failed batches
     * to disk, and each up phase must drain part of the backlog. This exercises the rapid
     * alternation of writes and drains on the single-thread disk executor that static fail/succeed
     * unit tests don't reproduce.
     */
    public void testIntermittentFailuresPersistAndDrain() throws Exception {
        waitForMetricCollectionGreen();

        CountDownLatch writesObserved = new CountDownLatch(1);
        CountDownLatch replaysObserved = new CountDownLatch(1);
        recordingApmServer.addMessageConsumer(msg -> {
            if (msg instanceof ReceivedTelemetry.ReceivedMetricSet m && "elasticsearch".equals(m.instrumentationScopeName())) {
                if (positiveLongSample(m, "es.apm.metrics.disk_buffer.writes")) writesObserved.countDown();
                if (positiveLongSample(m, "es.apm.metrics.disk_buffer.replays")) replaysObserved.countDown();
            }
        });

        // 3 cycles of ~2s down / ~2s up. With a 1s collection interval, each phase covers
        // multiple ticks, so every down phase contributes at least one disk write and every
        // up phase contributes at least one drain.
        for (int i = 0; i < 3; i++) {
            recordingApmServer.setResponseCode(503);
            Thread.sleep(2000);
            recordingApmServer.setResponseCode(0);
            Thread.sleep(2000);
        }

        // Final flush after recovery to make sure the counters we asserted on are exported.
        client().performRequest(new Request("GET", "/_flush_telemetry"));

        assertTrue(
            "expected at least one disk write across the flapping period " + "(es.apm.metrics.disk_buffer.writes never reached >0)",
            writesObserved.await(TELEMETRY_TIMEOUT, TimeUnit.SECONDS)
        );
        assertTrue(
            "expected at least one drained batch across the flapping period " + "(es.apm.metrics.disk_buffer.replays never reached >0)",
            replaysObserved.await(TELEMETRY_TIMEOUT, TimeUnit.SECONDS)
        );
    }

    /**
     * Slow APM (above the OTLP request timeout) → queue fills → drop-newest kicks in.
     * Asserts that {@code es.apm.metrics.export_queue.dropped} reaches a non-zero value once APM
     * recovers and the counter can be exported.
     */
    public void testQueueDropsNewestUnderSlowApm() throws Exception {
        waitForMetricCollectionGreen();

        // 2s > the configured 1s OTLP request_timeout, to trigger retry logic and eventually fill the export queue
        recordingApmServer.setResponseDelayMillis(2000);
        Thread.sleep(8_000);

        CountDownLatch droppedObserved = new CountDownLatch(1);
        recordingApmServer.addMessageConsumer(msg -> {
            if (msg instanceof ReceivedTelemetry.ReceivedMetricSet m
                && "elasticsearch".equals(m.instrumentationScopeName())
                && positiveLongSample(m, "es.apm.metrics.export_queue.dropped")) {
                droppedObserved.countDown();
            }
        });

        recordingApmServer.setResponseDelayMillis(0);
        client().performRequest(new Request("GET", "/_flush_telemetry"));

        assertTrue(
            "expected the queue to drop at least one incoming batch under sustained slow APM "
                + "(es.apm.metrics.export_queue.dropped never reached >0)",
            droppedObserved.await(TELEMETRY_TIMEOUT, TimeUnit.SECONDS)
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
