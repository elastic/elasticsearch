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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Exercises an operator flipping {@code telemetry.metrics.enabled} and {@code telemetry.tracing.enabled} on and off at
 * runtime on a live node, asserting the observable effect on OTLP export rather than any internal wiring: telemetry
 * flows while enabled, stops flowing once disabled (including on a manual {@code /_flush_telemetry}, which is the path
 * that is not otherwise gated on the enabled flag), and — critically — resumes flowing after being re-enabled. The
 * resume leg catches the real bug class: a provider/service swap that fails to re-attach would silently keep telemetry
 * off forever, and that can only be observed against a running cluster.
 *
 * <p>This owns a dedicated cluster because it mutates the cluster-wide enable settings; sharing a cluster with the
 * telemetry-assertion tests would let the disabled windows contaminate them.
 */
public class OTelTelemetryEnableDisableIT extends AbstractTelemetryIT {

    private static final String TEST_COUNTER = "es.test.long_counter.total";
    private static final String SPAN_ID = "b7ad6b7169203331";
    private static final String TRACE_ID_ENABLED = "0af7651916cd43dd8448eb211c800001";
    private static final String TRACE_ID_DISABLED = "0af7651916cd43dd8448eb211c800002";
    private static final String TRACE_ID_REENABLED = "0af7651916cd43dd8448eb211c800003";

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
        // Buffering (default 512mb) would let a disabled-window flush drain previously buffered batches; 0b makes each
        // forceFlush export directly so the "nothing while disabled" assertion is deterministic. Export interval and
        // send_timeout are left at their defaults on purpose: every export here is driven by a manual /_flush_telemetry,
        // and having no periodic cycle avoids a stray pre-disable point landing right after a recordingApmServer.reset().
        .setting("telemetry.metrics.buffer.disk_size", "0b")
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

    public void testMetricsStopWhenDisabledAndResumeWhenReEnabled() throws Exception {
        AtomicBoolean seenWhileEnabled = watchForTestCounter();
        assertBusy(() -> {
            recordMetricAndFlush();
            assertTrue("metrics must reach the server while enabled", seenWhileEnabled.get());
        }, TELEMETRY_TIMEOUT, TimeUnit.SECONDS);

        // Disabling swaps every application instrument (sync and async) to the no-op provider, so nothing recorded
        // through the registry is exported. We assert against *all* of the es.test.* instruments, not just the counter,
        // so a partial disable that leaked any one of them is caught. The SDK's own self-monitoring metrics (otel.sdk.*,
        // a different instrumentation scope) legitimately keep flowing because the flag only swaps the app-facing
        // registry, not the SDK provider (which closes at node shutdown); scoping to es.test.* excludes them. Drain
        // anything exported around the flip before watching, so a stray in-flight point cannot cause a false negative.
        setEnabled("telemetry.metrics.enabled", false);
        recordMetricAndFlush();
        safeSleep(1500);
        recordingApmServer.reset();

        AtomicBoolean seenWhileDisabled = watchForAnyTestMetric();
        for (int i = 0; i < 6; i++) {
            recordMetricAndFlush();
            safeSleep(500);
        }
        assertFalse("no application metric may reach the server while disabled", seenWhileDisabled.get());

        setEnabled("telemetry.metrics.enabled", true);
        recordingApmServer.reset();
        AtomicBoolean seenAfterReEnable = watchForTestCounter();
        assertBusy(() -> {
            recordMetricAndFlush();
            assertTrue("metrics must resume reaching the server after re-enabling", seenAfterReEnable.get());
        }, TELEMETRY_TIMEOUT, TimeUnit.SECONDS);
    }

    public void testTracesStopWhenDisabledAndResumeWhenReEnabled() throws Exception {
        // Phase 1: enabled -> a sampled root span reaches the server.
        AtomicBoolean seenWhileEnabled = watchForSpan(TRACE_ID_ENABLED);
        assertBusy(() -> {
            produceSpanAndFlush(TRACE_ID_ENABLED);
            assertTrue("spans must reach the server while enabled", seenWhileEnabled.get());
        }, TELEMETRY_TIMEOUT, TimeUnit.SECONDS);

        // Phase 2: disable at runtime. The tracer tears down its services, so startTrace() no-ops and no span is ever
        // created for the request; a manual flush therefore has nothing to export. A distinct trace id per phase means
        // any late pre-disable span cannot be mistaken for one produced while disabled.
        setEnabled("telemetry.tracing.enabled", false);
        recordingApmServer.reset();
        AtomicBoolean seenWhileDisabled = watchForSpan(TRACE_ID_DISABLED);
        for (int i = 0; i < 6; i++) {
            produceSpanAndFlush(TRACE_ID_DISABLED);
            safeSleep(500);
        }
        assertFalse("spans must not reach the server while disabled", seenWhileDisabled.get());

        // Phase 3: re-enable. The tracer must rebuild its services so spans are produced and exported again.
        setEnabled("telemetry.tracing.enabled", true);
        recordingApmServer.reset();
        AtomicBoolean seenAfterReEnable = watchForSpan(TRACE_ID_REENABLED);
        assertBusy(() -> {
            produceSpanAndFlush(TRACE_ID_REENABLED);
            assertTrue("spans must resume reaching the server after re-enabling", seenAfterReEnable.get());
        }, TELEMETRY_TIMEOUT, TimeUnit.SECONDS);
    }

    private AtomicBoolean watchForTestCounter() {
        AtomicBoolean seen = new AtomicBoolean();
        recordingApmServer.addMessageConsumer(msg -> {
            if (msg instanceof ReceivedTelemetry.ReceivedMetricSet m
                && "elasticsearch".equals(m.instrumentationScopeName())
                && m.samples().containsKey(TEST_COUNTER)) {
                seen.set(true);
            }
        });
        return seen;
    }

    /**
     * Flags any application metric produced by {@code /_use_apm_metrics} (the whole {@code es.test.*} family, not just
     * the counter), while excluding the SDK's own {@code otel.sdk.*} self-monitoring metrics, which legitimately keep
     * flowing while the enable flag is off.
     */
    private AtomicBoolean watchForAnyTestMetric() {
        AtomicBoolean seen = new AtomicBoolean();
        recordingApmServer.addMessageConsumer(msg -> {
            if (msg instanceof ReceivedTelemetry.ReceivedMetricSet m && "elasticsearch".equals(m.instrumentationScopeName())) {
                for (String key : m.samples().keySet()) {
                    if (key.startsWith("es.test.")) {
                        seen.set(true);
                    }
                }
            }
        });
        return seen;
    }

    private AtomicBoolean watchForSpan(String traceId) {
        AtomicBoolean seen = new AtomicBoolean();
        recordingApmServer.addMessageConsumer(msg -> {
            if (msg instanceof ReceivedTelemetry.ReceivedSpan s && traceId.equals(s.traceId())) {
                seen.set(true);
            }
        });
        return seen;
    }

    private void recordMetricAndFlush() throws IOException {
        client().performRequest(new Request("GET", "/_use_apm_metrics"));
        client().performRequest(new Request("GET", "/_flush_telemetry"));
    }

    private void produceSpanAndFlush(String traceId) throws IOException {
        Request request = new Request("GET", "/_nodes/stats");
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader(Task.TRACE_PARENT_HTTP_HEADER, "00-" + traceId + "-" + SPAN_ID + "-01").build()
        );
        client().performRequest(request);
        client().performRequest(new Request("GET", "/_flush_telemetry"));
    }

    private void setEnabled(String settingKey, boolean enabled) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"persistent\":{\"" + settingKey + "\":" + enabled + "}}");
        assertOK(client().performRequest(request));
    }
}
