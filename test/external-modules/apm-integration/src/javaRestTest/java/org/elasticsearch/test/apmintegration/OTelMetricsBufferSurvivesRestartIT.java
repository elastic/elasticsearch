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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class OTelMetricsBufferSurvivesRestartIT extends AbstractTelemetryIT {

    // Poll past the fixed production read-min-age window (33s) before the pre-existing file becomes drainable.
    private static final int BUFFER_DRAIN_TIMEOUT = 60;

    public static RecordingApmServer recordingApmServer = new RecordingApmServer();

    public static ElasticsearchCluster cluster = AbstractMetricsIT.baseClusterBuilder()
        .systemProperty("telemetry.otel.metrics.enabled", "true")
        .setting("telemetry.export.endpoint", () -> recordingApmServer.getGrpcEndpoint())
        .setting("telemetry.metrics.buffer.disk_size", "10mb")
        .setting("telemetry.metrics.buffer.ttl", "5m")
        // interval > send_timeout > initial_backoff so a failing export fully fails within an interval and the
        // PeriodicMetricReader does not skip a cycle.
        .setting("telemetry.export.interval", "1000ms")
        .setting("telemetry.export.send_timeout", "200ms")
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

        AtomicBoolean replayed = new AtomicBoolean();
        recordingApmServer.addMessageConsumer(msg -> {
            if (msg instanceof ReceivedTelemetry.ReceivedMetricSet m
                && "elasticsearch".equals(m.instrumentationScopeName())
                && positiveLongSample(m, "es.apm.metrics.disk_buffer.replays")) {
                replayed.set(true);
            }
        });
        client().performRequest(new Request("GET", "/_flush_telemetry"));

        assertBusy(
            () -> assertTrue("expected pre-existing buffer files to be replayed after restart", replayed.get()),
            BUFFER_DRAIN_TIMEOUT,
            TimeUnit.SECONDS
        );
    }
}
