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
 * Test metrics exported by Elasticsearch directly using the OTel SDK. Runs with
 * {@code telemetry.otel.metrics.disk_buffer_size=0b} so the inherited baseline assertions
 * also exercise the buffering-disabled bypass in {@code OtelSdkExportMeterSupplier}.
 */
public class OtelMetricsIT extends AbstractMetricsIT {

    private static final String EXPECTED_PROJECT_ID = "integ-test-project";
    private static final String EXPECTED_PROJECT_TYPE = "elasticsearch";
    private static final String EXPECTED_NODE_TIER = "index";

    public static RecordingApmServer recordingApmServer = new RecordingApmServer();

    public static ElasticsearchCluster cluster = AbstractMetricsIT.baseClusterBuilder()
        .systemProperty("telemetry.otel.metrics.enabled", "true")
        .setting("telemetry.otel.metrics.endpoint", () -> "http://" + recordingApmServer.getHttpAddress() + "/v1/metrics")
        .setting("telemetry.otel.metrics.interval", "100ms")
        .setting("telemetry.otel.otlp.send_timeout", "80ms")
        .setting("telemetry.otel.otlp.retry.initial_backoff", "20ms")
        .setting("telemetry.otel.metrics.disk_buffer_size", "0b")
        // Mirrors the three labels ServerlessServerCli writes via telemetry.agent.global_labels.* on the APM-agent path,
        // bridged here to the OTel resource via the telemetry.otel.resource.* affix.
        .setting("telemetry.otel.resource.elasticsearch.project.id", EXPECTED_PROJECT_ID)
        .setting("telemetry.otel.resource.elasticsearch.project.type", EXPECTED_PROJECT_TYPE)
        .setting("telemetry.otel.resource.elasticsearch.node.tier", EXPECTED_NODE_TIER)
        .build();

    @ClassRule
    public static TestRule ruleChain = buildRuleChain(recordingApmServer, cluster);

    @Override
    protected RecordingApmServer apmServer() {
        return recordingApmServer;
    }

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

    public void testResourceCarriesAffix() throws Exception {
        assertSdkResourceAttributes(EXPECTED_PROJECT_ID, EXPECTED_PROJECT_TYPE, EXPECTED_NODE_TIER);
    }
}
