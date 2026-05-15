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

        // Long enough to span at least one full retry chain (~3s) plus a couple of collection cycles,
        // so the disk buffer is guaranteed to have absorbed at least one batch before recovery.
        Thread.sleep(3000);

        long outageStartEpochNanos = outageStartEpochMs * 1_000_000L;
        long outageEndEpochNanos = System.currentTimeMillis() * 1_000_000L;

        CountDownLatch replaysObserved = new CountDownLatch(1);
        CountDownLatch outageWindowBatchReplayed = new CountDownLatch(1);
        recordingApmServer.addMessageConsumer(msg -> {
            if (msg instanceof ReceivedTelemetry.ReceivedMetricSet m && "elasticsearch".equals(m.instrumentationScopeName())) {
                long timestamp = m.collectionTime();
                if (timestamp >= outageStartEpochNanos && timestamp <= outageEndEpochNanos) {
                    outageWindowBatchReplayed.countDown();
                }
                if (positiveLongSample(m, "es.apm.metrics.disk_buffer.replays")) replaysObserved.countDown();
            }
        });

        recordingApmServer.setResponseCode(0);

        client().performRequest(new Request("GET", "/_flush_telemetry"));

        assertTrue(
            "expected at least one disk-buffered batch to be replayed after recovery "
                + "(es.apm.metrics.disk_buffer.replays never reached >0; this implies no write happened either)",
            replaysObserved.await(TELEMETRY_TIMEOUT, TimeUnit.SECONDS)
        );
        assertTrue(
            "expected a replayed metricset carrying an outage-window timestamp",
            outageWindowBatchReplayed.await(TELEMETRY_TIMEOUT, TimeUnit.SECONDS)
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

    private static boolean positiveLongSample(ReceivedTelemetry.ReceivedMetricSet metricSet, String metricName) {
        return metricSet.samples().get(metricName) instanceof ReceivedTelemetry.ValueSample(Number value) && value.longValue() > 0;
    }
}
