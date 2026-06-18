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
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * End-to-end coverage of the OTel SDK self-monitoring path that crosses both export pipelines. The
 * {@code BatchSpanProcessor} built by {@code OtelSdkExportTracerSupplier} records its own metrics
 * (e.g. {@code otel.sdk.processor.span.queue.capacity}) into the {@code MeterProvider} handed out by
 * {@code OtelSdkExportMeterSupplier#getMeterProvider()}, and the metrics pipeline exports them over OTLP.
 * <p>
 * This is the only test that wires the two real suppliers together exactly as {@code APMTelemetryProvider}
 * does in production, so it requires both {@code telemetry.otel.traces.enabled} and
 * {@code telemetry.otel.metrics.enabled}.
 */
public class OtelSdkSpanProcessorMetricsIT extends AbstractTelemetryIT {

    private static final String SPAN_PROCESSOR_QUEUE_CAPACITY = "otel.sdk.processor.span.queue.capacity";

    public static RecordingApmServer recordingApmServer = new RecordingApmServer();

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.INTEG_TEST)
        .module("test-apm-integration")
        .module("apm")
        .setting("telemetry.tracing.enabled", "true")
        .setting("telemetry.metrics.enabled", "true")
        .systemProperty("telemetry.otel.traces.enabled", "true")
        .systemProperty("telemetry.otel.metrics.enabled", "true")
        .setting("telemetry.otel.traces.endpoint", () -> "http://" + recordingApmServer.getHttpAddress() + "/v1/traces")
        .setting("telemetry.otel.traces.sample_rate", "1.0")
        .setting("telemetry.otel.metrics.endpoint", () -> "http://" + recordingApmServer.getHttpAddress() + "/v1/metrics")
        .setting("telemetry.otel.metrics.interval", "100ms")
        .setting("telemetry.otel.otlp.send_timeout", "80ms")
        .setting("telemetry.otel.otlp.retry.initial_backoff", "20ms")
        .setting("telemetry.otel.metrics.disk_buffer_size", "0b")
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

    public void testSpanProcessorSelfMonitoringMetricsExported() throws Exception {
        CountDownLatch finished = new CountDownLatch(1);
        Consumer<ReceivedTelemetry> messageConsumer = msg -> {
            if (msg instanceof ReceivedTelemetry.ReceivedMetricSet m && m.samples().containsKey(SPAN_PROCESSOR_QUEUE_CAPACITY)) {
                finished.countDown();
            }
        };
        recordingApmServer.addMessageConsumer(messageConsumer);

        client().performRequest(new Request("GET", "/_nodes/stats"));
        client().performRequest(new Request("GET", "/_flush_telemetry"));

        assertTrue(
            "Timed out waiting for " + SPAN_PROCESSOR_QUEUE_CAPACITY + " to be exported by the OTel SDK metrics pipeline",
            finished.await(TELEMETRY_TIMEOUT, TimeUnit.SECONDS)
        );
    }
}
