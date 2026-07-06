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
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class OTelTelemetryFlushedOnShutdownIT extends AbstractTelemetryIT {

    public static RecordingApmServer recordingApmServer = new RecordingApmServer();

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.INTEG_TEST)
        .module("test-apm-integration")
        .module("apm")
        .systemProperty("telemetry.otel.metrics.enabled", "true")
        .systemProperty("telemetry.otel.traces.enabled", "true")
        .setting("telemetry.metrics.enabled", "true")
        .setting("telemetry.tracing.enabled", "true")
        .setting("telemetry.export.endpoint", () -> recordingApmServer.getGrpcEndpoint())
        .setting("telemetry.metrics.buffer.disk_size", "0b")
        // Purposefully very long so we can isolate the flush
        .setting("telemetry.export.interval", "1h")
        .setting("telemetry.tracing.sample_rate", "1.0")
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

    public void testBufferedTelemetryIsFlushedOnNodeShutdown() throws Exception {
        final String traceId = "0af7651916cd43dd8448eb211c80319c";
        AtomicBoolean counterFlushed = new AtomicBoolean();
        AtomicBoolean spanFlushed = new AtomicBoolean();
        recordingApmServer.addMessageConsumer(msg -> {
            if (msg instanceof ReceivedTelemetry.ReceivedMetricSet m
                && "elasticsearch".equals(m.instrumentationScopeName())
                && m.samples().containsKey("es.test.long_counter.total")) {
                counterFlushed.set(true);
            }
            if (msg instanceof ReceivedTelemetry.ReceivedSpan s && traceId.equals(s.traceId())) {
                spanFlushed.set(true);
            }
        });

        client().performRequest(new Request("GET", "/_use_apm_metrics"));
        Request nodeStats = new Request("GET", "/_nodes/stats");
        nodeStats.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader(Task.TRACE_PARENT_HTTP_HEADER, "00-" + traceId + "-b7ad6b7169203331-01").build()
        );
        client().performRequest(nodeStats);

        cluster.restart(false);
        closeClients();
        initClient();

        assertBusy(() -> {
            assertTrue("metric buffered before shutdown must be flushed during node stop", counterFlushed.get());
            assertTrue("span buffered before shutdown must be flushed during node stop", spanFlushed.get());
        }, 30, TimeUnit.SECONDS);
    }
}
