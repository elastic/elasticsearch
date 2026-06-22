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

public class OTelMetricsBufferSurvivesRestartIT extends AbstractTelemetryIT {

    public static RecordingApmServer recordingApmServer = new RecordingApmServer();

    public static ElasticsearchCluster cluster = AbstractMetricsIT.baseClusterBuilder()
        .systemProperty("telemetry.otel.metrics.enabled", "true")
        .setting("telemetry.otel.metrics.endpoint", () -> "http://" + recordingApmServer.getHttpAddress() + "/v1/metrics")
        .setting("telemetry.otel.metrics.disk_buffer_size", "10mb")
        .setting("telemetry.otel.metrics.buffer_ttl", "5m")
        // Tight write/read windows so pre-existing buffered files become drainable within the test budget.
        .setting("telemetry.otel.metrics.disk_buffer_write_window", "100ms")
        .setting("telemetry.otel.metrics.disk_buffer_read_min_age", "200ms")
        // metrics.interval must be > otlp.send_timeout so that the OTLP exporter can fully fail, and so that PeriodicMetricReader does not
        // skip an export cycle
        .setting("telemetry.otel.metrics.interval", "500ms")
        .setting("telemetry.otel.otlp.send_timeout", "300ms")
        // initial_backoff that is way smaller than otlp.send_timeout allows the exporter to fully exhaust the retries
        .setting("telemetry.otel.otlp.retry.initial_backoff", "100ms")
        .build();

    @ClassRule
    public static TestRule ruleChain = AbstractTelemetryIT.buildRuleChain(recordingApmServer, cluster);

    @Override
    protected RecordingApmServer apmServer() {
        return recordingApmServer;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testPreExistingBufferFilesDrainAfterRestart() throws Exception {
        recordingApmServer.setResponseCode(503);
        client().performRequest(new Request("GET", "/_use_apm_metrics"));
        Thread.sleep(1000);

        cluster.restart(false);
        closeClients();
        initClient();
        recordingApmServer.reset();
        recordingApmServer.clearResponseCode();

        CountDownLatch replayed = new CountDownLatch(1);
        recordingApmServer.addMessageConsumer(msg -> {
            if (msg instanceof ReceivedTelemetry.ReceivedMetricSet m
                && "elasticsearch".equals(m.instrumentationScopeName())
                && positiveLongSample(m, "es.apm.metrics.disk_buffer.replays")) {
                replayed.countDown();
            }
        });
        client().performRequest(new Request("GET", "/_flush_telemetry"));

        assertTrue("expected pre-existing buffer files to be replayed after restart", replayed.await(TELEMETRY_TIMEOUT, TimeUnit.SECONDS));
    }
}
