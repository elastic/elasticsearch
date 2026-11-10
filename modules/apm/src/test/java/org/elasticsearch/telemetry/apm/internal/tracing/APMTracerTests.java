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
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
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
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings;
import org.elasticsearch.telemetry.tracing.Traceable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.stream.Collectors;
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
import static org.hamcrest.Matchers.sameInstance;
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

    private static final String TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
    private static final String SPAN_ID = "b7ad6b7169203331";
    private static final String TRACE_PARENT = "00-" + TRACE_ID + "-" + SPAN_ID + "-01";

    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

    /**
     * The two-arg constructor accepting a {@code Supplier<MeterProvider>} should construct without
     * throwing regardless of the supplier's return value.
     */
    public void testConstructorWithMeterProviderSupplierDoesNotThrow() {
        new APMTracer(new ThreadContext(Settings.EMPTY), Settings.EMPTY, MeterProvider::noop);
    }

    /**
     * Check that the tracer doesn't create spans when tracing is disabled, and that the trace context is dropped when it is
     * asked to make the span current.
     */
    public void test_onTraceStarted_withTracingDisabled_doesNotStartTrace() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), false).build();
        APMTracer apmTracer = buildTracer(settings);

        threadContext.putHeader(Task.TRACE_PARENT_HTTP_HEADER, TRACE_PARENT);
        threadContext.putHeader(Task.TRACE_STATE, "vendor=value");

        apmTracer.startTrace(TRACEABLE1, "name1", null);
        assertThat(apmTracer.getSpans(), anEmptyMap());
        // starting the span does not change the thread context
        assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), equalTo(TRACE_PARENT));

        try (var ignored = apmTracer.storeSpanInThreadContext(TRACEABLE1)) {
            assertLocalTraceDropped();
        }
        // restored
        assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), equalTo(TRACE_PARENT));
        assertThat(threadContext.getHeader(Task.TRACE_STATE), equalTo("vendor=value"));
    }

    /**
     * Check that the tracer doesn't create spans if a Traceable's span name is filtered out, and that the local trace is dropped when
     * it is asked to make the span current.
     */
    public void test_onTraceStarted_withSpanNameOmitted_doesNotStartTrace() {
        Settings settings = Settings.builder()
            .put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true)
            .putList(APMAgentSettings.TELEMETRY_TRACING_NAMES_INCLUDE_SETTING.getKey(), List.of("filtered*"))
            .build();
        APMTracer apmTracer = buildTracer(settings);

        apmTracer.startTrace(TRACEABLE1, "name1", null);

        assertThat(recordedSpans(apmTracer), anEmptyMap());
        try (var ignored = apmTracer.storeSpanInThreadContext(TRACEABLE1)) {
            assertLocalTraceDropped();
        }
    }

    /**
     * Check that when a trace is started, the tracer starts a span and records it, without making it the current one.
     */
    public void test_onTraceStarted_startsTrace() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings);

        apmTracer.startTrace(TRACEABLE1, "name1", null);

        assertThat(threadContext.getTransient(Task.APM_TRACE_CONTEXT), nullValue());
        assertThat(recordedSpans(apmTracer), aMapWithSize(1));
        assertThat(recordedSpans(apmTracer), hasKey(TRACEABLE1.getSpanId()));
    }

    /**
     * Check that when a root trace is started, but it is not recorded, e.g. due to sampling,
     * the tracer doesn't record it, but still makes its (not sampled) context the current one once it is asked to.
     */
    public void test_onTraceStarted_ifNotRecorded_doesNotStartTracing() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings);

        apmTracer.startTrace(TRACEABLE1, "name1_discard", null);

        assertThat(recordedSpans(apmTracer), anEmptyMap());
        try (var ignored = apmTracer.storeSpanInThreadContext(TRACEABLE1)) {
            assertThat(spanContextOf(threadContext.getTransient(Task.APM_TRACE_CONTEXT)).isSampled(), is(false));
        }
    }

    /**
     * Check that when a trace is not sampled, the tracer does not record any of the spans that are started under it.
     */
    public void test_onNestedTraceStarted_ifParentNotSampled_doesNotStartTrace() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings, 5);

        apmTracer.startTrace(TRACEABLE1, "name1", null);
        try (var ignored1 = apmTracer.storeSpanInThreadContext(TRACEABLE1)) {
            final Context rootContext = threadContext.getTransient(Task.APM_TRACE_CONTEXT);

            apmTracer.startTrace(TRACEABLE2, "name2_discard", null);
            try (var ignored2 = apmTracer.storeSpanInThreadContext(TRACEABLE2)) {
                final Context discardedContext = threadContext.getTransient(Task.APM_TRACE_CONTEXT);
                assertThat(discardedContext, not(sameInstance(rootContext)));
                assertThat(spanContextOf(discardedContext).isSampled(), is(false));

                apmTracer.startTrace(TRACEABLE3, "name3", null);
                try (var ignored3 = apmTracer.storeSpanInThreadContext(TRACEABLE3)) {
                    assertThat(threadContext.getTransient(Task.APM_TRACE_CONTEXT), sameInstance(discardedContext));
                }
            }
            assertThat(threadContext.getTransient(Task.APM_TRACE_CONTEXT), sameInstance(rootContext));
        }
        assertThat(recordedSpans(apmTracer), aMapWithSize(1));
        assertThat(recordedSpans(apmTracer), hasKey(TRACEABLE1.getSpanId()));
    }

    /**
     * Checks that when a trace is started with a specific start time, the tracer starts a span and records it.
     */
    public void test_onTraceStartedWithStartTime_startsTrace() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings);

        // 1_000_000L because of "toNanos" conversions that overflow for large long millis
        Instant spanStartTime = Instant.ofEpochMilli(randomLongBetween(0, Long.MAX_VALUE / 1_000_000L));
        threadContext.putTransient(Task.LOCAL_TRACE_START_TIME, spanStartTime);
        apmTracer.startTrace(TRACEABLE1, "name1", null);

        assertThat(recordedSpans(apmTracer), aMapWithSize(1));
        assertThat(recordedSpans(apmTracer), hasKey(TRACEABLE1.getSpanId()));
        assertThat(((SpyAPMTracer) apmTracer).getSpanStartTime("name1"), is(spanStartTime));
    }

    /**
     * Check that the start time override only applies to the local root span.
     */
    public void test_onTraceStartedWithStartTime_ignoredForChildSpan() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings, 1);

        Instant spanStartTime = Instant.ofEpochMilli(randomLongBetween(0, Long.MAX_VALUE / 1_000_000L));
        threadContext.putTransient(Task.LOCAL_TRACE_START_TIME, spanStartTime);
        apmTracer.startTrace(TRACEABLE1, "root", null);
        try (var ignored = apmTracer.storeSpanInThreadContext(TRACEABLE1)) {
            apmTracer.startTrace(TRACEABLE2, "child", null);
        }

        assertThat(recordedSpans(apmTracer), aMapWithSize(2));
        assertThat(((SpyAPMTracer) apmTracer).getSpanStartTime("root"), is(spanStartTime));
        assertThat(((SpyAPMTracer) apmTracer).getSpanStartTime("child"), nullValue());
    }

    /**
     * Check that when a trace is stopped, the tracer ends the span and removes the record of it.
     */
    public void test_onTraceStopped_stopsTrace() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings);

        apmTracer.startTrace(TRACEABLE1, "name1", null);
        apmTracer.stopTrace(TRACEABLE1);
        apmTracer.stopTrace(TRACEABLE2); // stopping a non-existent trace is a noop

        assertThat(apmTracer.getSpans(), anEmptyMap());
    }

    /**
     * Check that when the span of a trace is made the current one, then the thread context is updated with tracing information.
     * <p>
     * The tracer injects the {@link Task#TRACE_PARENT_HTTP_HEADER} and {@link Task#TRACE_STATE} headers into the context when
     * there is a propagator, which these tests do not have unless they say so. We can check that the local context object is added,
     * however.
     */
    public void test_whenSpanIsStoredInThreadContext_threadContextIsPopulated() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings);

        apmTracer.startTrace(TRACEABLE1, "name1", null);
        assertThat(threadContext.getTransient(Task.APM_TRACE_CONTEXT), nullValue());

        try (var ignored = apmTracer.storeSpanInThreadContext(TRACEABLE1)) {
            assertThat(threadContext.getTransient(Task.APM_TRACE_CONTEXT), notNullValue());
        }
        assertThat(threadContext.getTransient(Task.APM_TRACE_CONTEXT), nullValue());
    }

    /**
     * Check that when a trace is started for a request carrying the {@link Task#X_ELASTIC_PROJECT_ID_HTTP_HEADER}
     * header, the project id is stamped on the span as the {@code project.id} attribute.
     */
    public void test_whenTraceStarted_projectIdHeaderIsSetAsSpanAttribute() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings);

        String projectId = randomAlphaOfLength(16);
        threadContext.putHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId);
        apmTracer.startTrace(TRACEABLE1, "name1", null);

        Span span = Span.fromContextOrNull(apmTracer.getSpans().get(TRACEABLE1.getSpanId()));
        assertThat(span, notNullValue());
        Mockito.verify(span).setAttribute("project.id", projectId);
    }

    /**
     * Check that when a trace is started for a request without the {@link Task#X_ELASTIC_PROJECT_ID_HTTP_HEADER}
     * header (e.g. a non multi-project request), no {@code project.id} attribute is added to the span.
     */
    public void test_whenTraceStarted_withoutProjectIdHeader_noProjectIdSpanAttribute() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings);

        apmTracer.startTrace(TRACEABLE1, "name1", null);

        Span span = Span.fromContextOrNull(apmTracer.getSpans().get(TRACEABLE1.getSpanId()));
        assertThat(span, notNullValue());
        Mockito.verify(span, never()).setAttribute(eq("project.id"), anyString());
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

        startTraceInNewContext(apmTracer, TRACEABLE1, "name-aaa");
        startTraceInNewContext(apmTracer, TRACEABLE2, "name-bbb");
        startTraceInNewContext(apmTracer, TRACEABLE3, "name-ccc");

        assertThat(recordedSpans(apmTracer), hasKey(TRACEABLE1.getSpanId()));
        assertThat(recordedSpans(apmTracer), hasKey(TRACEABLE2.getSpanId()));
        assertThat(recordedSpans(apmTracer), not(hasKey(TRACEABLE3.getSpanId())));
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

        startTraceInNewContext(apmTracer, TRACEABLE1, "name-aaa");

        assertThat(recordedSpans(apmTracer), not(hasKey("id1")));
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

        startTraceInNewContext(apmTracer, TRACEABLE1, "name-aaa");
        startTraceInNewContext(apmTracer, TRACEABLE2, "name-bbb");
        startTraceInNewContext(apmTracer, TRACEABLE3, "name-ccc");

        assertThat(recordedSpans(apmTracer), not(hasKey(TRACEABLE1.getSpanId())));
        assertThat(recordedSpans(apmTracer), not(hasKey(TRACEABLE2.getSpanId())));
        assertThat(recordedSpans(apmTracer), hasKey(TRACEABLE3.getSpanId()));
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
     * over transport (as a regular header) but {@link Task#APM_TRACE_CONTEXT} is not
     * (transients are not serialised over transport). The tracer should start a span whose
     * parent matches the remote span encoded in the header.
     */
    public void test_whenTraceStartedWithPropagatedTraceParentHeader_andNoLocalParent_usesRemoteParentFromHeader() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracerWithW3CPropagator(settings);

        // Simulate transport propagation: the TRACE_PARENT_HTTP_HEADER request header is read from the stream on the receiving node.
        threadContext.putHeader(Task.TRACE_PARENT_HTTP_HEADER, TRACE_PARENT);
        // APM_TRACE_CONTEXT is intentionally absent — transients are not serialised over transport.

        apmTracer.startTrace(TRACEABLE1, "name1", null);

        assertThat(apmTracer.getSpans(), aMapWithSize(1));
        Context spanContext = apmTracer.getSpans().get(TRACEABLE1.getSpanId());
        assertThat(spanContext, notNullValue());
        // MockSpanBuilder.setParent() stores the parent's SpanContext on the mock span.
        // Verify it matches the trace ID and span ID from the traceparent header.
        Span span = Span.fromContext(spanContext);
        assertThat(span.getSpanContext().getTraceId(), is(TRACE_ID));
        assertThat(span.getSpanContext().getSpanId(), is(SPAN_ID));
    }

    public void testTracingResumesAfterDisableAndReEnable() {
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(exporter))
            .setSampler(Sampler.alwaysOn())
            .build();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();

        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer tracer = new APMTracer(threadContext, settings, () -> sdk, 0, false);
        tracer.setNodeName("test-node");
        tracer.setClusterName("test-cluster");
        tracer.start();

        startAndStopSpan(tracer, TRACEABLE1, "resume-test-1");
        assertThat(exporter.getFinishedSpanItems(), hasSize(1));

        tracer.setEnabled(false);
        exporter.reset();
        startAndStopSpan(tracer, TRACEABLE2, "resume-test-2");
        assertThat(exporter.getFinishedSpanItems(), empty());

        tracer.setEnabled(true);
        startAndStopSpan(tracer, TRACEABLE3, "resume-test-3");
        assertThat(exporter.getFinishedSpanItems(), hasSize(1));

        sdk.close();
    }

    private void startAndStopSpan(APMTracer tracer, Traceable traceable, String spanName) {
        startTraceInNewContext(tracer, traceable, spanName);
        tracer.stopTrace(traceable);
    }

    /**
     * The tracer stores the span context in a transient header, which can only be set once per context, so spans that
     * are not nested must each be started in their own context.
     */
    private void startTraceInNewContext(APMTracer tracer, Traceable traceable, String spanName) {
        try (var ignored = threadContext.newEmptyContext()) {
            tracer.startTrace(traceable, spanName, null);
        }
    }

    /** The spans that are recorded, which is not the case for the ones that are started, but dropped. */
    private static Map<String, Context> recordedSpans(APMTracer tracer) {
        return tracer.getSpans()
            .entrySet()
            .stream()
            .filter(entry -> Span.fromContext(entry.getValue()).isRecording())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /** Asserts that the thread context marks the local trace as dropped and that no trace context is propagated. */
    private void assertLocalTraceDropped() {
        assertThat(spanContextOf(threadContext.getTransient(Task.APM_TRACE_CONTEXT)).isValid(), is(false));
        assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), nullValue());
        assertThat(threadContext.getHeader(Task.TRACE_STATE), nullValue());
    }

    private static SpanContext spanContextOf(Context context) {
        assertThat(context, notNullValue());
        return Span.fromContext(context).getSpanContext();
    }

    /** The context of a recorded span that was started by this node, as found in the thread context. */
    private static Context sampledLocalParentContext() {
        return Context.root().with(Span.wrap(sampledSpanContext()));
    }

    private static SpanContext sampledSpanContext() {
        return SpanContext.create(TRACE_ID, SPAN_ID, TraceFlags.getSampled(), TraceState.getDefault());
    }

    private APMTracer buildTracer(Settings settings) {
        return buildTracer(settings, 0, false);
    }

    private APMTracer buildTracer(Settings settings, int maxTraceDepth) {
        return buildTracer(settings, maxTraceDepth, false);
    }

    private APMTracer buildTracer(Settings settings, int maxTraceDepth, boolean recordExceptionStacks) {
        APMTracer tracer = new SpyAPMTracer(threadContext, settings, OpenTelemetry.noop(), maxTraceDepth, recordExceptionStacks);
        tracer.doStart();
        return tracer;
    }

    private APMTracer buildTracerWithW3CPropagator(Settings settings) {
        return buildTracerWithW3CPropagator(settings, 0);
    }

    private APMTracer buildTracerWithW3CPropagator(Settings settings, int maxTraceDepth) {
        APMTracer tracer = new SpyAPMTracer(threadContext, settings, openTelemetryWithW3CPropagator(), maxTraceDepth, false);
        tracer.doStart();
        return tracer;
    }

    private static String traceParent(SpanContext spanContext) {
        return "00-" + spanContext.getTraceId() + "-" + spanContext.getSpanId() + "-" + spanContext.getTraceFlags().asHex();
    }

    private static OpenTelemetry openTelemetryWithW3CPropagator() {
        return OpenTelemetrySdk.builder().setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance())).build();
    }

    public void test_onSdkPath_withMaxTraceDepthZero_dropsChildSpan() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer tracer = buildTracer(settings, 0);

        threadContext.putTransientAllowOverwrite(Task.APM_TRACE_CONTEXT, sampledLocalParentContext());

        tracer.startTrace(TRACEABLE1, "child-span", Map.of());

        assertThat(recordedSpans(tracer), anEmptyMap());
    }

    public void test_onSdkPath_withMaxTraceDepthZero_recordsRootSpan() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer tracer = buildTracer(settings, 0);

        // No APM_TRACE_CONTEXT transient => no local parent => this is a root span.
        tracer.startTrace(TRACEABLE1, "root-span", Map.of());

        assertThat(tracer.getSpans(), hasKey(TRACEABLE1.getSpanId()));
    }

    public void test_onSdkPath_withMaxTraceDepthOne_recordsChildSpan() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer tracer = buildTracer(settings, 1);

        threadContext.putTransientAllowOverwrite(Task.APM_TRACE_CONTEXT, sampledLocalParentContext());

        tracer.startTrace(TRACEABLE1, "child-span", Map.of());

        assertThat(tracer.getSpans(), hasKey(TRACEABLE1.getSpanId()));
    }

    public void test_onSdkPath_withMaxTraceDepthOne_dropsGrandchildSpan() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer tracer = buildTracer(settings, 1);

        tracer.startTrace(TRACEABLE1, "root", Map.of());
        try (var ignore1 = tracer.storeSpanInThreadContext(TRACEABLE1)) {
            tracer.startTrace(TRACEABLE2, "child", Map.of());
            try (var ignore2 = tracer.storeSpanInThreadContext(TRACEABLE2)) {
                tracer.startTrace(TRACEABLE3, "grandchild", Map.of());
            }
        }

        assertThat(recordedSpans(tracer), aMapWithSize(2));
        assertThat(recordedSpans(tracer), hasKey(TRACEABLE1.getSpanId()));
        assertThat(recordedSpans(tracer), hasKey(TRACEABLE2.getSpanId()));
        assertThat(recordedSpans(tracer), not(hasKey(TRACEABLE3.getSpanId())));
    }

    public void test_onSdkPath_withMaxSpansTwo_dropsThirdChildSpan() {
        Settings settings = Settings.builder()
            .put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true)
            .put(OtelSdkSettings.TELEMETRY_TRACING_MAX_SPANS.getKey(), 2)
            .build();
        APMTracer tracer = buildTracer(settings, 5);
        Traceable child3 = new TestTraceable("id4");

        tracer.startTrace(TRACEABLE1, "root", Map.of());
        for (Traceable child : List.of(TRACEABLE2, TRACEABLE3, child3)) {
            try (var ignored = tracer.storeSpanInThreadContext(TRACEABLE1)) {
                tracer.startTrace(child, "child", Map.of());
            }
        }

        assertThat(recordedSpans(tracer).keySet(), equalTo(Set.of(TRACEABLE1.getSpanId(), TRACEABLE2.getSpanId(), TRACEABLE3.getSpanId())));
    }

    public void test_onSdkPath_withMaxTraceDepthZero_recordsEntryAndDropsLocalChild() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer tracer = buildTracerWithW3CPropagator(settings);

        // APM_TRACE_CONTEXT is intentionally absent: this simulates a fresh entry from a remote caller.
        threadContext.putHeader(Task.TRACE_PARENT_HTTP_HEADER, TRACE_PARENT);

        tracer.startTrace(TRACEABLE1, "entry-span", Map.of());
        try (var ignored = tracer.storeSpanInThreadContext(TRACEABLE1)) {
            tracer.startTrace(TRACEABLE2, "local-child", Map.of());
        }

        assertThat(recordedSpans(tracer).keySet(), equalTo(Set.of(TRACEABLE1.getSpanId())));
        Span entrySpan = Span.fromContext(tracer.getSpans().get(TRACEABLE1.getSpanId()));
        assertThat(entrySpan.getSpanContext().getTraceId(), is(TRACE_ID));
        assertThat(entrySpan.getSpanContext().getSpanId(), is(SPAN_ID));
    }

    /**
     * The span that is stored in the thread context is the one that its descendants are parented to, and its context is propagated in
     * the trace headers; closing the stored context restores both, but keeps the response headers.
     */
    public void test_storeSpanInThreadContext_propagatesTheSpanAndRestoresTheThreadContext() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracerWithW3CPropagator(settings);
        threadContext.putHeader(Task.TRACE_PARENT_HTTP_HEADER, "00-" + TRACE_ID + "-0000000000000001-01");
        apmTracer.startTrace(TRACEABLE1, "name1", null);

        try (var ignored = apmTracer.storeSpanInThreadContext(TRACEABLE1)) {
            final Context stored = threadContext.getTransient(Task.APM_TRACE_CONTEXT);
            assertThat(stored, sameInstance(apmTracer.getSpans().get(TRACEABLE1.getSpanId())));
            assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), equalTo(traceParent(spanContextOf(stored))));
            threadContext.addResponseHeader("response-header", "value");
        }

        assertThat(threadContext.getTransient(Task.APM_TRACE_CONTEXT), nullValue());
        assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), equalTo("00-" + TRACE_ID + "-0000000000000001-01"));
        assertThat(threadContext.getResponseHeaders().get("response-header"), equalTo(List.of("value")));
    }

    public void test_storeSpanInThreadContext_ofSpanThatWasNotStarted_leavesThreadContextAsItIs() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracerWithW3CPropagator(settings);
        threadContext.putHeader(Task.TRACE_PARENT_HTTP_HEADER, TRACE_PARENT);

        try (var ignored = apmTracer.storeSpanInThreadContext(TRACEABLE1)) {
            assertThat(threadContext.getTransient(Task.APM_TRACE_CONTEXT), nullValue());
            assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), equalTo(TRACE_PARENT));
        }
        assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), equalTo(TRACE_PARENT));
    }

    /**
     * Starting a span does not make it the current one, so the siblings that are started one after the other are not nested.
     */
    public void test_spansThatAreStartedOneAfterTheOtherAreSiblings() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracerWithW3CPropagator(settings, 5);
        threadContext.putHeader(Task.TRACE_PARENT_HTTP_HEADER, TRACE_PARENT);

        apmTracer.startTrace(TRACEABLE1, "sibling-1", null);
        apmTracer.startTrace(TRACEABLE2, "sibling-2", null);

        assertThat(threadContext.getTransient(Task.APM_TRACE_CONTEXT), nullValue());
        assertThat(
            Span.fromContext(apmTracer.getSpans().get(TRACEABLE2.getSpanId())).getSpanContext().getSpanId(),
            equalTo(Span.fromContext(apmTracer.getSpans().get(TRACEABLE1.getSpanId())).getSpanContext().getSpanId())
        );
    }

    public void test_addError_withStacksDisabled_emitsTypeAndMessageOnly() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer tracer = buildTracer(settings, 0, false);
        tracer.startTrace(TRACEABLE1, "span-with-error", Map.of());
        Span recordedSpan = Span.fromContext(tracer.getSpans().get(TRACEABLE1.getSpanId()));

        tracer.addError(TRACEABLE1, new IllegalStateException("boom"));

        ArgumentCaptor<Attributes> attrs = ArgumentCaptor.forClass(Attributes.class);
        Mockito.verify(recordedSpan).addEvent(eq("exception"), attrs.capture());
        Mockito.verify(recordedSpan, never()).recordException(Mockito.any());
        assertThat(attrs.getValue().get(AttributeKey.stringKey("exception.type")), is(IllegalStateException.class.getName()));
        assertThat(attrs.getValue().get(AttributeKey.stringKey("exception.message")), is("boom"));
        assertThat(attrs.getValue().get(AttributeKey.stringKey("exception.stacktrace")), nullValue());
    }

    public void test_addError_withStacksEnabled_delegatesToRecordException() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer tracer = buildTracer(settings, 0, true);
        tracer.startTrace(TRACEABLE1, "span-with-error", Map.of());
        Span recordedSpan = Span.fromContext(tracer.getSpans().get(TRACEABLE1.getSpanId()));

        Exception failure = new IllegalStateException("boom");
        tracer.addError(TRACEABLE1, failure);

        Mockito.verify(recordedSpan).recordException(failure);
        Mockito.verify(recordedSpan, never()).addEvent(anyString(), Mockito.any(Attributes.class));
    }

    public void test_addError_withNullMessage_omitsMessageAttribute() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer tracer = buildTracer(settings, 0, false);
        tracer.startTrace(TRACEABLE1, "span-with-error", Map.of());
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
        tracer.startTrace(TRACEABLE1, "name1", Map.of());
        Span recordedSpan = Span.fromContext(tracer.getSpans().get(TRACEABLE1.getSpanId()));

        Attributes attributes = Attributes.of(AttributeKey.stringKey("http.method"), "GET", AttributeKey.longKey("http.status_code"), 200L);
        tracer.setAttributes(TRACEABLE1, attributes);

        Mockito.verify(recordedSpan).setAllAttributes(attributes);
    }

    @SuppressForbidden(reason = "OpenTelemetry logs API usage issues via java.util.logging")
    public void testUntrackedSpans() {
        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings);

        List<String> usageIssues = new ArrayList<>();
        Handler handler = new Handler() {
            @Override
            public void publish(LogRecord logRecord) {
                usageIssues.add(logRecord.getMessage());
            }

            @Override
            public void flush() {}

            @Override
            public void close() {}
        };
        // OpenTelemetry logs API usage issues at WARNING only once per JVM, but at FINEST every time
        java.util.logging.Logger usageLogger = java.util.logging.Logger.getLogger("io.opentelemetry.usage");
        Level previousLevel = usageLogger.getLevel();
        usageLogger.setLevel(Level.FINEST);
        usageLogger.addHandler(handler);
        try {
            apmTracer.stopTrace(TRACEABLE1); // Stopping a trace that was never started
        } finally {
            usageLogger.removeHandler(handler);
            usageLogger.setLevel(previousLevel);
        }

        assertThat(usageIssues, empty());
    }

    static class SpyAPMTracer extends APMTracer {

        Map<String, Instant> spanStartTimeMap;
        private final OpenTelemetry openTelemetry;

        SpyAPMTracer(
            ThreadContext threadContext,
            Settings settings,
            OpenTelemetry openTelemetry,
            int maxTraceDepth,
            boolean recordExceptionStacks
        ) {
            super(threadContext, settings, () -> openTelemetry, maxTraceDepth, recordExceptionStacks);
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
         * Spans created by the default span builder would be NOOP spans that are not recorded, because these unit tests
         * have no configured exporter. This builder simulates recorded spans so that we can test the tracer behavior.
         */
        class MockSpanBuilder implements SpanBuilder {

            Span span;
            Instant startTime;
            String spanName;

            MockSpanBuilder(String spanName) {
                this.spanName = spanName;
                this.span = Mockito.mock(Span.class, spanName);
                // simulate a span discarded because its trace was not sampled
                Mockito.when(span.isRecording()).thenReturn(isSampled());
                Mockito.when(span.storeInContext(Mockito.any(Context.class))).thenCallRealMethod();
                Mockito.when(span.getSpanContext()).thenReturn(withOwnSampling(sampledSpanContext()));
            }

            private boolean isSampled() {
                return spanName.endsWith("_discard") == false;
            }

            /** Keeps the ids of the given context, but has this span's own sampling decision. */
            private SpanContext withOwnSampling(SpanContext parent) {
                final TraceFlags flags = isSampled() ? TraceFlags.getSampled() : TraceFlags.getDefault();
                return SpanContext.create(parent.getTraceId(), parent.getSpanId(), flags, parent.getTraceState());
            }

            @Override
            public SpanBuilder setParent(Context context) {
                SpanContext spanContext = withOwnSampling(Span.fromContext(context).getSpanContext());
                Mockito.when(span.getSpanContext()).thenReturn(spanContext);
                return this;
            }

            @Override
            public SpanBuilder setNoParent() {
                Mockito.when(span.getSpanContext()).thenReturn(withOwnSampling(SpanContext.getInvalid()));
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
                // Record string attributes on the mock span so tests can Mockito.verify(span).setAttribute(...)
                span.setAttribute(key, value);
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
