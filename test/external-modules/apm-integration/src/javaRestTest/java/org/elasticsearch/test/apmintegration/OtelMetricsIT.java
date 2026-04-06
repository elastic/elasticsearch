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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Test metrics exported by Elasticsearch directly using the OTel SDK
 */
public class OtelMetricsIT extends AbstractMetricsIT {

    public static ElasticsearchCluster cluster = AbstractMetricsIT.baseClusterBuilder()
        .systemProperty("telemetry.otel.metrics.enabled", "true")
        .setting("telemetry.otel.metrics.endpoint", () -> "http://" + recordingApmServer.getHttpAddress() + "/v1/metrics")
        .setting("telemetry.otel.metrics.interval", "10m") // one giant batch instead of multiple small ones with deltas we need to sum
        .build();

    @ClassRule
    public static TestRule ruleChain = AbstractMetricsIT.buildRuleChain(recordingApmServer, cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testOTelHealthMetrics() throws Exception {
        final Set<String> remaining = new HashSet<>(
            Set.of(
                "otel.sdk.metric_reader.collection.duration",
                "otel.sdk.exporter.metric_data_point.exported",
                "otel.sdk.exporter.metric_data_point.inflight",
                "otel.sdk.exporter.operation.duration"
            )
        );

        CountDownLatch finished = new CountDownLatch(1);

        Consumer<ReceivedTelemetry> messageConsumer = (ReceivedTelemetry msg) -> {
            if (msg instanceof ReceivedTelemetry.ReceivedMetricSet m) {
                for (Map.Entry<String, ReceivedTelemetry.ReceivedMetricValue> e : m.samples().entrySet()) {
                    remaining.remove(e.getKey());
                }
            }
            if (remaining.isEmpty()) {
                finished.countDown();
            }
        };

        recordingApmServer.addMessageConsumer(messageConsumer);

        client().performRequest(new Request("GET", "/_use_apm_metrics"));
        client().performRequest(new Request("GET", "/_flush_telemetry"));

        boolean completed = finished.await(TELEMETRY_TIMEOUT, TimeUnit.SECONDS);
        String missing = remaining.stream().sorted().collect(Collectors.joining(", "));
        assertTrue("Timeout waiting for OTel SDK health metrics. Missing: " + missing, completed && remaining.isEmpty());
    }
}
