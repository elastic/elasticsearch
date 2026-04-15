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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Integration test verifying that metrics are buffered to disk when the OTLP endpoint is failing,
 * and replayed with correct original timestamps when the endpoint recovers.
 */
public class OTelMetricsBufferingIT extends AbstractMetricsIT {

    public static ElasticsearchCluster cluster = AbstractMetricsIT.baseClusterBuilder()
        .systemProperty("telemetry.otel.metrics.enabled", "true")
        .setting("telemetry.otel.metrics.endpoint", () -> "http://" + recordingApmServer.getHttpAddress() + "/v1/metrics")
        .setting("telemetry.otel.metrics.interval", "1s")
        .setting("telemetry.otel.metrics.disk_buffer_size", "10mb")
        .setting("telemetry.otel.metrics.buffer_ttl", "5m")
        .build();

    @ClassRule
    public static TestRule ruleChain = AbstractMetricsIT.buildRuleChain(recordingApmServer, cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    /**
     * Proves that metrics collected during an outage are persisted to disk and replayed with their
     * original collection timestamps after recovery.
     */
    public void testMetricsBufferedDuringOutageAndReplayedOnRecovery() throws Exception {
        CountDownLatch baselineLatch = new CountDownLatch(1);
        recordingApmServer.addMessageConsumer(msg -> {
            if (msg instanceof ReceivedTelemetry.ReceivedMetricSet m && "elasticsearch".equals(m.instrumentationScopeName())) {
                baselineLatch.countDown();
            }
        });

        client().performRequest(new Request("GET", "/_use_apm_metrics"));
        client().performRequest(new Request("GET", "/_flush_telemetry"));
        assertTrue("Timed out waiting for baseline metrics", baselineLatch.await(TELEMETRY_TIMEOUT, TimeUnit.SECONDS));

        long outageStartEpochMs = System.currentTimeMillis();
        recordingApmServer.setResponseCode(503);

        client().performRequest(new Request("GET", "/_use_apm_metrics"));
        client().performRequest(new Request("GET", "/_flush_telemetry"));

        Thread.sleep(5000);

        long outageStartEpochNanos = outageStartEpochMs * 1_000_000L;
        long outageEndEpochNanos = System.currentTimeMillis() * 1_000_000L;

        CountDownLatch recoveryLatch = new CountDownLatch(1);
        recordingApmServer.addMessageConsumer(msg -> {
            if (msg instanceof ReceivedTelemetry.ReceivedMetricSet m && "elasticsearch".equals(m.instrumentationScopeName())) {
                long timestamp = m.collectionTime();
                if (timestamp >= outageStartEpochNanos && timestamp <= outageEndEpochNanos) {
                    recoveryLatch.countDown();
                }
            }
        });

        recordingApmServer.setResponseCode(0);

        client().performRequest(new Request("GET", "/_flush_telemetry"));
        assertTrue(
            "Timed out waiting for disk-replayed metrics with outage-window timestamps",
            recoveryLatch.await(TELEMETRY_TIMEOUT, TimeUnit.SECONDS)
        );
    }
}
