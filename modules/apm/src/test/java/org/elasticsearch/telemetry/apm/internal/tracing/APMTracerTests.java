/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.tracing;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;
import org.elasticsearch.telemetry.tracing.TraceContext;
import org.elasticsearch.telemetry.tracing.Traceable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;

@TestLogging(reason = "improved visibility", value = "org.elasticsearch.telemetry.apm.internal.tracing:TRACE")
public class APMTracerTests extends ESTestCase {

    private static final Traceable TRACEABLE1 = new TestTraceable("id1");
    private static final Traceable TRACEABLE2 = new TestTraceable("id2");
    private static final Traceable TRACEABLE3 = new TestTraceable("id3");

    /**
     * The two-arg constructor accepting a {@code Supplier<MeterProvider>} should construct without
     * throwing regardless of the supplier's return value.
     */
    public void testConstructorWithMeterProviderSupplierDoesNotThrow() {
        assertNotNull(new APMTracer(Settings.EMPTY, MeterProvider::noop));
    }

    /**
     * Check that the tracer doesn't create spans when tracing is disabled.
     */
    public void test_onTraceStarted_withTracingDisabled_doesNotStartTrace() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), false).build();
        APMTracer apmTracer = buildTracer(settings);

        apmTracer.startTrace(new ThreadContext(settings), TRACEABLE1, "name1", null);

        assertThat(apmTracer.getSpans(), anEmptyMap());
    }

    /**
     * Check that the tracer doesn't create spans if a Traceable's span name is filtered out.
     */
    public void test_onTraceStarted_withSpanNameOmitted_doesNotStartTrace() {
        Settings settings = Settings.builder()
            .put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true)
            .putList(APMAgentSettings.TELEMETRY_TRACING_NAMES_INCLUDE_SETTING.getKey(), List.of("filtered*"))
            .build();
        APMTracer apmTracer = buildTracer(settings);

        apmTracer.startTrace(new ThreadContext(settings), TRACEABLE1, "name1", null);

        assertThat(apmTracer.getSpans(), anEmptyMap());
    }

    /**
     * Check that when a trace is started, the tracer starts a span and records it.
     */
    public void test_onTraceStarted_startsTrace() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings);

        ThreadContext traceContext = new ThreadContext(settings);
        apmTracer.startTrace(traceContext, TRACEABLE1, "name1", null);

        assertThat(traceContext.getTransient(Task.APM_TRACE_CONTEXT), notNullValue());
        assertThat(apmTracer.getSpans(), aMapWithSize(1));
        assertThat(apmTracer.getSpans(), hasKey(TRACEABLE1.getSpanId()));
    }

    /**
     * Check that when a root trace is started, but it is not recorded, e.g. due to sampling,
     * the tracer tracks it but doesn't start tracing.
     */
    public void test_onTraceStarted_ifNotRecorded_doesNotStartTracing() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings);

        ThreadContext traceContext = new ThreadContext(settings);
        apmTracer.startTrace(traceContext, TRACEABLE1, "name1_discard", null);

        assertThat(traceContext.getTransient(Task.APM_TRACE_CONTEXT), nullValue());
        assertThat(apmTracer.getSpans(), anEmptyMap());
    }

    /**
     * Check that when a nested trace is discarded e.g. due to transaction_max_spans exceeded, the tracer does not record it.
     */
    public void test_onNestedTraceStarted_ifNotRecorded_doesNotStartTrace() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings);

        ThreadContext traceContext = new ThreadContext(settings);
        apmTracer.startTrace(traceContext, TRACEABLE1, "name1", null);
        try (var ignore1 = traceContext.newTraceContext()) {
            apmTracer.startTrace(traceContext, TRACEABLE2, "name2_discard", null);
            assertThat(traceContext.getTransient(Task.APM_TRACE_CONTEXT), nullValue());

            try (var ignore2 = traceContext.newTraceContext()) {
                apmTracer.startTrace(traceContext, TRACEABLE3, "name3_discard", null);
                assertThat(traceContext.getTransient(Task.APM_TRACE_CONTEXT), nullValue());
            }
        }
        assertThat(apmTracer.getSpans(), aMapWithSize(1));
        assertThat(apmTracer.getSpans(), hasKey(TRACEABLE1.getSpanId()));
    }

    /**
     * Checks that when a trace is started with a specific start time, the tracer starts a span and records it.
     */
    public void test_onTraceStartedWithStartTime_startsTrace() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings);

        TraceContext traceContext = new ThreadContext(settings);
        // 1_000_000L because of "toNanos" conversions that overflow for large long millis
        Instant spanStartTime = Instant.ofEpochMilli(randomLongBetween(0, Long.MAX_VALUE / 1_000_000L));
        traceContext.putTransient(Task.TRACE_START_TIME, spanStartTime);
        apmTracer.startTrace(traceContext, TRACEABLE1, "name1", null);

        assertThat(traceContext.getTransient(Task.APM_TRACE_CONTEXT), notNullValue());
        assertThat(apmTracer.getSpans(), aMapWithSize(1));
        assertThat(apmTracer.getSpans(), hasKey(TRACEABLE1.getSpanId()));
        assertThat(((SpyAPMTracer) apmTracer).getSpanStartTime("name1"), is(spanStartTime));
    }

    /**
     * Check that when a trace is stopped, the tracer ends the span and removes the record of it.
     */
    public void test_onTraceStopped_stopsTrace() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings);

        apmTracer.startTrace(new ThreadContext(settings), TRACEABLE1, "name1", null);
        apmTracer.stopTrace(TRACEABLE1);
        apmTracer.stopTrace(TRACEABLE2); // stopping a non-existent trace is a noop

        assertThat(apmTracer.getSpans(), anEmptyMap());
    }

    /**
     * Check that {@link APMTracer#setStatusToError} sets the OTel span's status to {@link StatusCode#ERROR},
     * verifying the direct integration with the underlying OpenTelemetry span.
     */
    public void test_setStatusToError_setsSpanStatusToError() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings);

        apmTracer.startTrace(new ThreadContext(settings), TRACEABLE1, "name1", null);
        Span span = Span.fromContextOrNull(apmTracer.getSpans().get(TRACEABLE1.getSpanId()));
        assertThat(span, notNullValue());

        String description = "500 INTERNAL_SERVER_ERROR";
        apmTracer.setStatusToError(TRACEABLE1, description);

        Mockito.verify(span).setStatus(StatusCode.ERROR, description);
    }

    /**
     * Check that {@link APMTracer#setStatusToError} is a no-op when the traceable has no active span,
     * i.e. it was never started or has already been stopped.
     */
    public void test_setStatusToError_noopWhenSpanNotFound() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings);

        // TRACEABLE1 was never started — span map is empty
        assertThat(apmTracer.getSpans(), anEmptyMap());
        apmTracer.setStatusToError(TRACEABLE1, "should be ignored");
        // no exception thrown and spans map remains empty
        assertThat(apmTracer.getSpans(), anEmptyMap());
    }

    /**
     * Check that when a trace is started, then the thread context is updated with tracing information.
     * <p>
     * We expect the APM agent to inject the {@link Task#TRACE_PARENT_HTTP_HEADER} and {@link Task#TRACE_STATE}
     * headers into the context, and it does, but this doesn't happen in the unit tests. We can
     * check that the local context object is added, however.
     */
    public void test_whenTraceStarted_threadContextIsPopulated() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings);

        ThreadContext threadContext = new ThreadContext(settings);
        apmTracer.startTrace(threadContext, TRACEABLE1, "name1", null);
        assertThat(threadContext.getTransient(Task.APM_TRACE_CONTEXT), notNullValue());
    }

    /**
     * Check that when a tracer has a list of include names configured, then those
     * names are used to filter spans.
     */
    public void test_whenTraceStarted_andSpanNameIncluded_thenSpanIsStarted() {
        final List<String> includePatterns = List.of(
            // exact name
            "name-aaa",
            // regex
            "name-b*"
        );
        Settings settings = Settings.builder()
            .put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true)
            .putList(APMAgentSettings.TELEMETRY_TRACING_NAMES_INCLUDE_SETTING.getKey(), includePatterns)
            .build();
        APMTracer apmTracer = buildTracer(settings);

        apmTracer.startTrace(new ThreadContext(settings), TRACEABLE1, "name-aaa", null);
        apmTracer.startTrace(new ThreadContext(settings), TRACEABLE2, "name-bbb", null);
        apmTracer.startTrace(new ThreadContext(settings), TRACEABLE3, "name-ccc", null);

        assertThat(apmTracer.getSpans(), hasKey(TRACEABLE1.getSpanId()));
        assertThat(apmTracer.getSpans(), hasKey(TRACEABLE2.getSpanId()));
        assertThat(apmTracer.getSpans(), not(hasKey(TRACEABLE3.getSpanId())));
    }

    /**
     * Check that when a tracer has a list of include and exclude names configured, and
     * a span matches both, then the exclude filters take precedence.
     */
    public void test_whenTraceStarted_andSpanNameIncludedAndExcluded_thenSpanIsNotStarted() {
        final List<String> includePatterns = List.of("name-a*");
        final List<String> excludePatterns = List.of("name-a*");
        Settings settings = Settings.builder()
            .put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true)
            .putList(APMAgentSettings.TELEMETRY_TRACING_NAMES_INCLUDE_SETTING.getKey(), includePatterns)
            .putList(APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING.getKey(), excludePatterns)
            .build();
        APMTracer apmTracer = buildTracer(settings);

        apmTracer.startTrace(new ThreadContext(settings), TRACEABLE1, "name-aaa", null);

        assertThat(apmTracer.getSpans(), not(hasKey("id1")));
    }

    /**
     * Check that when a tracer has a list of exclude names configured, then those
     * names are used to filter spans.
     */
    public void test_whenTraceStarted_andSpanNameExcluded_thenSpanIsNotStarted() {
        final List<String> excludePatterns = List.of(
            // exact name
            "name-aaa",
            // regex
            "name-b*"
        );
        Settings settings = Settings.builder()
            .put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true)
            .putList(APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING.getKey(), excludePatterns)
            .build();
        APMTracer apmTracer = buildTracer(settings);

        apmTracer.startTrace(new ThreadContext(settings), TRACEABLE1, "name-aaa", null);
        apmTracer.startTrace(new ThreadContext(settings), TRACEABLE2, "name-bbb", null);
        apmTracer.startTrace(new ThreadContext(settings), TRACEABLE3, "name-ccc", null);

        assertThat(apmTracer.getSpans(), not(hasKey(TRACEABLE1.getSpanId())));
        assertThat(apmTracer.getSpans(), not(hasKey(TRACEABLE2.getSpanId())));
        assertThat(apmTracer.getSpans(), hasKey(TRACEABLE3.getSpanId()));
    }

    /**
     * Check that sensitive attributes are not added verbatim to a span, but instead the value is redacted.
     */
    public void test_whenAddingAttributes_thenSensitiveValuesAreRedacted() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), false).build();
        APMTracer apmTracer = buildTracer(settings);
        CharacterRunAutomaton labelFilterAutomaton = apmTracer.getLabelFilterAutomaton();

        Stream.of(
            "auth",
            "auth-header",
            "authValue",
            "card",
            "card-details",
            "card-number",
            "credit",
            "credit-card",
            "key",
            "my-credit-number",
            "my_session_id",
            "passwd",
            "password",
            "principal",
            "principal-value",
            "pwd",
            "secret",
            "secure-key",
            "sensitive-token*",
            "session",
            "session_id",
            "set-cookie",
            "some-auth",
            "some-principal",
            "token-for login"
        ).forEach(key -> assertTrue("Expected label filter automaton to redact [" + key + "]", labelFilterAutomaton.run(key)));
    }

    /**
     * Simulates a data-node scenario: {@link Task#TRACE_PARENT_HTTP_HEADER} is propagated
     * over transport (as a regular header) but {@link Task#PARENT_APM_TRACE_CONTEXT} is not
     * (transients are not serialised over transport). The tracer should start a span whose
     * parent matches the remote span encoded in the header.
     */
    public void test_whenTraceStartedWithPropagatedTraceParentHeader_andNoLocalParent_usesRemoteParentFromHeader() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracerWithW3CPropagator(settings);

        final String traceId = "0af7651916cd43dd8448eb211c80319c";
        final String remoteParentSpanId = "b7ad6b7169203331";
        ThreadContext traceContext = new ThreadContext(settings);
        // Simulate transport propagation: the transport layer copies TRACE_PARENT_HTTP_HEADER into the
        // PARENT_TRACE_PARENT_HEADER transient on the receiving node. getRemoteParentContext() reads this transient.
        traceContext.putTransient(Task.PARENT_TRACE_PARENT_HEADER, "00-" + traceId + "-" + remoteParentSpanId + "-01");
        // PARENT_APM_TRACE_CONTEXT is intentionally absent — transients are not serialised over transport.

        apmTracer.startTrace(traceContext, TRACEABLE1, "name1", null);

        assertThat(apmTracer.getSpans(), aMapWithSize(1));
        Context spanContext = apmTracer.getSpans().get(TRACEABLE1.getSpanId());
        assertThat(spanContext, notNullValue());
        // MockSpanBuilder.setParent() stores the parent's SpanContext on the mock span.
        // Verify it matches the trace ID and span ID from the traceparent header.
        Span span = Span.fromContext(spanContext);
        assertThat(span.getSpanContext().getTraceId(), is(traceId));
        assertThat(span.getSpanContext().getSpanId(), is(remoteParentSpanId));
    }

    public void testTracingResumesAfterDisableAndReEnable() {
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(exporter))
            .setSampler(Sampler.alwaysOn())
            .build();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();

        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer tracer = new APMTracer(settings, () -> sdk, false, 0, false);
        tracer.setNodeName("test-node");
        tracer.setClusterName("test-cluster");
        tracer.start();

        startAndStopSpan(tracer, settings, TRACEABLE1, "resume-test-1");
        assertThat(exporter.getFinishedSpanItems(), hasSize(1));

        tracer.setEnabled(false);
        exporter.reset();
        startAndStopSpan(tracer, settings, TRACEABLE2, "resume-test-2");
        assertThat(exporter.getFinishedSpanItems(), empty());

        tracer.setEnabled(true);
        startAndStopSpan(tracer, settings, TRACEABLE3, "resume-test-3");
        assertThat(exporter.getFinishedSpanItems(), hasSize(1));

        sdk.close();
    }

    private static void startAndStopSpan(APMTracer tracer, Settings settings, Traceable traceable, String spanName) {
        tracer.startTrace(new ThreadContext(settings), traceable, spanName, null);
        tracer.stopTrace(traceable);
    }

    private APMTracer buildTracer(Settings settings) {
        APMTracer tracer = new SpyAPMTracer(settings, OpenTelemetry.noop(), false, 0, false);
        tracer.doStart();
        return tracer;
    }

    private APMTracer buildTracerWithW3CPropagator(Settings settings) {
        APMTracer tracer = new SpyAPMTracer(settings, openTelemetryWithW3CPropagator(), false, 0, false);
        tracer.doStart();
        return tracer;
    }

    private APMTracer buildSdkPathTracer(Settings settings, int maxTraceDepth) {
        APMTracer tracer = new SpyAPMTracer(settings, OpenTelemetry.noop(), true, maxTraceDepth, false);
        tracer.doStart();
        return tracer;
    }

    private APMTracer buildSdkPathTracer(Settings settings, int maxTraceDepth, boolean recordExceptionStacks) {
        APMTracer tracer = new SpyAPMTracer(settings, OpenTelemetry.noop(), true, maxTraceDepth, recordExceptionStacks);
        tracer.doStart();
        return tracer;
    }

    private APMTracer buildSdkPathTracerWithW3CPropagator(Settings settings) {
        APMTracer tracer = new SpyAPMTracer(settings, openTelemetryWithW3CPropagator(), true, 0, false);
        tracer.doStart();
        return tracer;
    }

    private static OpenTelemetry openTelemetryWithW3CPropagator() {
        return OpenTelemetrySdk.builder().setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance())).build();
    }

    public void test_onSdkPath_withMaxTraceDepthZero_dropsChildSpan() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer tracer = buildSdkPathTracer(settings, 0);

        ThreadContext threadContext = new ThreadContext(settings);
        threadContext.putTransient(Task.PARENT_APM_TRACE_CONTEXT, Context.root());

        tracer.startTrace(threadContext, TRACEABLE1, "child-span", Map.of());

        assertThat(tracer.getSpans(), anEmptyMap());
    }

    public void test_onSdkPath_withMaxTraceDepthZero_recordsRootSpan() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer tracer = buildSdkPathTracer(settings, 0);

        // No PARENT_APM_TRACE_CONTEXT transient => no local parent => this is a root span.
        tracer.startTrace(new ThreadContext(settings), TRACEABLE1, "root-span", Map.of());

        assertThat(tracer.getSpans(), hasKey(TRACEABLE1.getSpanId()));
    }

    public void test_onSdkPath_withMaxTraceDepthOne_recordsChildSpan() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer tracer = buildSdkPathTracer(settings, 1);

        ThreadContext threadContext = new ThreadContext(settings);
        threadContext.putTransient(Task.PARENT_APM_TRACE_CONTEXT, Context.root());

        tracer.startTrace(threadContext, TRACEABLE1, "child-span", Map.of());

        assertThat(tracer.getSpans(), hasKey(TRACEABLE1.getSpanId()));
    }

    /**
     * Exercises the depth chain end-to-end: with {@code maxTraceDepth=1}, the root and its first-level
     * child are recorded but a grandchild is dropped. {@link ThreadContext#newTraceContext()} promotes
     * the previous {@code APM_TRACE_CONTEXT} to {@code PARENT_APM_TRACE_CONTEXT} the same way the task
     * framework does in production.
     */
    public void test_onSdkPath_withMaxTraceDepthOne_dropsGrandchildSpan() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer tracer = buildSdkPathTracer(settings, 1);

        ThreadContext traceContext = new ThreadContext(settings);

        tracer.startTrace(traceContext, TRACEABLE1, "root", Map.of());
        try (var ignore1 = traceContext.newTraceContext()) {
            tracer.startTrace(traceContext, TRACEABLE2, "child", Map.of());
            try (var ignore2 = traceContext.newTraceContext()) {
                tracer.startTrace(traceContext, TRACEABLE3, "grandchild", Map.of());
            }
        }

        assertThat(tracer.getSpans(), aMapWithSize(2));
        assertThat(tracer.getSpans(), hasKey(TRACEABLE1.getSpanId()));
        assertThat(tracer.getSpans(), hasKey(TRACEABLE2.getSpanId()));
        assertThat(tracer.getSpans(), not(hasKey(TRACEABLE3.getSpanId())));
    }

    public void test_onSdkPath_withMaxTraceDepthZero_recordsEntryAndDropsLocalChild() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer tracer = buildSdkPathTracerWithW3CPropagator(settings);

        final String traceId = "0af7651916cd43dd8448eb211c80319c";
        final String remoteParentSpanId = "b7ad6b7169203331";
        ThreadContext traceContext = new ThreadContext(settings);
        // PARENT_APM_TRACE_CONTEXT is intentionally absent: this simulates a fresh entry from a remote caller.
        traceContext.putTransient(Task.PARENT_TRACE_PARENT_HEADER, "00-" + traceId + "-" + remoteParentSpanId + "-01");

        tracer.startTrace(traceContext, TRACEABLE1, "entry-span", Map.of());
        try (var ignored = traceContext.newTraceContext()) {
            tracer.startTrace(traceContext, TRACEABLE2, "local-child", Map.of());
        }

        assertThat(tracer.getSpans().keySet(), equalTo(Set.of(TRACEABLE1.getSpanId())));
        Span entrySpan = Span.fromContext(tracer.getSpans().get(TRACEABLE1.getSpanId()));
        assertThat(entrySpan.getSpanContext().getTraceId(), is(traceId));
        assertThat(entrySpan.getSpanContext().getSpanId(), is(remoteParentSpanId));
    }

    public void test_addError_onAgentPath_callsRecordException() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer tracer = buildTracer(settings);
        tracer.startTrace(new ThreadContext(settings), TRACEABLE1, "span-with-error", Map.of());
        Span recordedSpan = Span.fromContext(tracer.getSpans().get(TRACEABLE1.getSpanId()));

        Exception failure = new IllegalStateException("boom");
        tracer.addError(TRACEABLE1, failure);

        Mockito.verify(recordedSpan).recordException(failure);
    }

    public void test_addError_onSdkPath_withStacksDisabled_emitsTypeAndMessageOnly() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer tracer = buildSdkPathTracer(settings, 0, false);
        tracer.startTrace(new ThreadContext(settings), TRACEABLE1, "span-with-error", Map.of());
        Span recordedSpan = Span.fromContext(tracer.getSpans().get(TRACEABLE1.getSpanId()));

        tracer.addError(TRACEABLE1, new IllegalStateException("boom"));

        ArgumentCaptor<Attributes> attrs = ArgumentCaptor.forClass(Attributes.class);
        Mockito.verify(recordedSpan).addEvent(eq("exception"), attrs.capture());
        Mockito.verify(recordedSpan, never()).recordException(Mockito.any());
        assertThat(attrs.getValue().get(AttributeKey.stringKey("exception.type")), is(IllegalStateException.class.getName()));
        assertThat(attrs.getValue().get(AttributeKey.stringKey("exception.message")), is("boom"));
        assertThat(attrs.getValue().get(AttributeKey.stringKey("exception.stacktrace")), nullValue());
    }

    public void test_addError_onSdkPath_withStacksEnabled_delegatesToRecordException() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer tracer = buildSdkPathTracer(settings, 0, true);
        tracer.startTrace(new ThreadContext(settings), TRACEABLE1, "span-with-error", Map.of());
        Span recordedSpan = Span.fromContext(tracer.getSpans().get(TRACEABLE1.getSpanId()));

        Exception failure = new IllegalStateException("boom");
        tracer.addError(TRACEABLE1, failure);

        Mockito.verify(recordedSpan).recordException(failure);
        Mockito.verify(recordedSpan, never()).addEvent(anyString(), Mockito.any(Attributes.class));
    }

    public void test_addError_onSdkPath_withNullMessage_omitsMessageAttribute() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer tracer = buildSdkPathTracer(settings, 0, false);
        tracer.startTrace(new ThreadContext(settings), TRACEABLE1, "span-with-error", Map.of());
        Span recordedSpan = Span.fromContext(tracer.getSpans().get(TRACEABLE1.getSpanId()));

        tracer.addError(TRACEABLE1, new IllegalStateException());

        ArgumentCaptor<Attributes> attrs = ArgumentCaptor.forClass(Attributes.class);
        Mockito.verify(recordedSpan).addEvent(eq("exception"), attrs.capture());
        assertThat(attrs.getValue().get(AttributeKey.stringKey("exception.type")), is(IllegalStateException.class.getName()));
        assertThat(attrs.getValue().get(AttributeKey.stringKey("exception.message")), nullValue());
    }

    public void test_setAttributes_callsSetAllAttributes() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer tracer = buildTracer(settings);
        tracer.startTrace(new ThreadContext(settings), TRACEABLE1, "name1", Map.of());
        Span recordedSpan = Span.fromContext(tracer.getSpans().get(TRACEABLE1.getSpanId()));

        Attributes attributes = Attributes.of(AttributeKey.stringKey("http.method"), "GET", AttributeKey.longKey("http.status_code"), 200L);
        tracer.setAttributes(TRACEABLE1, attributes);

        Mockito.verify(recordedSpan).setAllAttributes(attributes);
    }

    static class SpyAPMTracer extends APMTracer {

        Map<String, Instant> spanStartTimeMap;
        private final OpenTelemetry openTelemetry;

        SpyAPMTracer(
            Settings settings,
            OpenTelemetry openTelemetry,
            boolean useOtelSdkTracesExport,
            int maxTraceDepth,
            boolean recordExceptionStacks
        ) {
            super(settings, () -> openTelemetry, useOtelSdkTracesExport, maxTraceDepth, recordExceptionStacks);
            this.openTelemetry = openTelemetry;
            this.spanStartTimeMap = new HashMap<>();
        }

        @Override
        APMServices createApmServices() {
            Tracer mockTracer = mock(Tracer.class);
            doAnswer(invocation -> {
                String spanName = (String) invocation.getArguments()[0];
                return new MockSpanBuilder(spanName);
            }).when(mockTracer).spanBuilder(anyString());
            return new APMServices(mockTracer, openTelemetry);
        }

        Instant getSpanStartTime(String spanName) {
            return spanStartTimeMap.get(spanName);
        }

        /**
         * There's no APM agent in unit tests. Spans created by the default span builder would be NOOP spans that are not recorded.
         * This builder simulates recorded spans so that we can test the tracer behavior.
         */
        class MockSpanBuilder implements SpanBuilder {

            Span span;
            Instant startTime;
            String spanName;

            MockSpanBuilder(String spanName) {
                this.spanName = spanName;
                this.span = Mockito.mock(Span.class, spanName);
                // simulate discarded span due to transaction_max_spans exceeded
                Mockito.when(span.isRecording()).thenReturn(spanName.endsWith("_discard") == false);
                Mockito.when(span.storeInContext(Mockito.any(Context.class))).thenCallRealMethod();
            }

            @Override
            public SpanBuilder setParent(Context context) {
                SpanContext spanContext = Span.fromContext(context).getSpanContext();
                Mockito.when(span.getSpanContext()).thenReturn(spanContext);
                return this;
            }

            @Override
            public SpanBuilder setNoParent() {
                SpanContext invalid = SpanContext.getInvalid();
                Mockito.when(span.getSpanContext()).thenReturn(invalid);
                return this;
            }

            @Override
            public SpanBuilder addLink(SpanContext spanContext) {
                return this;
            }

            @Override
            public SpanBuilder addLink(SpanContext spanContext, Attributes attributes) {
                return this;
            }

            @Override
            public SpanBuilder setAttribute(String key, String value) {
                return this;
            }

            @Override
            public SpanBuilder setAttribute(String key, long value) {
                return this;
            }

            @Override
            public SpanBuilder setAttribute(String key, double value) {
                return this;
            }

            @Override
            public SpanBuilder setAttribute(String key, boolean value) {
                return this;
            }

            @Override
            public <T> SpanBuilder setAttribute(AttributeKey<T> key, T value) {
                return this;
            }

            @Override
            public SpanBuilder setSpanKind(SpanKind spanKind) {
                return this;
            }

            @Override
            public SpanBuilder setStartTimestamp(long startTimestamp, TimeUnit unit) {
                startTime = Instant.ofEpochMilli(TimeUnit.MILLISECONDS.convert(startTimestamp, unit));
                return this;
            }

            @Override
            public Span startSpan() {
                // finally record the spanName-startTime association when the span is actually started
                spanStartTimeMap.put(spanName, startTime);
                return span;
            }
        }
    }

    private static class TestTraceable implements Traceable {
        private final String spanId;

        TestTraceable(String spanId) {
            this.spanId = Objects.requireNonNull(spanId);
        }

        @Override
        public String getSpanId() {
            return spanId;
        }
    }
}
