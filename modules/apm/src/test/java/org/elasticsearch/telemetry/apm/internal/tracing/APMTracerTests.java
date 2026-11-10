/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.tracing;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingDecision;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;
import org.elasticsearch.telemetry.apm.internal.APMTracingService;
import org.elasticsearch.telemetry.apm.internal.TracingSettingsUpdater;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings;
import org.elasticsearch.telemetry.tracing.Span;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.After;
import org.junit.Before;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;

@TestLogging(reason = "improved visibility", value = "org.elasticsearch.telemetry.apm.internal.tracing:TRACE")
public class APMTracerTests extends ESTestCase {

    private static final String TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
    private static final String SPAN_ID = "b7ad6b7169203331";
    private static final String TRACE_PARENT = "00-" + TRACE_ID + "-" + SPAN_ID + "-01";
    private static final String UNSAMPLED_TRACE_PARENT = "00-" + TRACE_ID + "-" + SPAN_ID + "-00";

    /** Does not sample spans whose name ends in {@code _discard}, the way a sampler that drops a trace would. */
    private static final Sampler NAME_SAMPLER = new Sampler() {
        @Override
        public SamplingResult shouldSample(
            Context parentContext,
            String traceId,
            String name,
            SpanKind spanKind,
            Attributes attributes,
            List<LinkData> parentLinks
        ) {
            return name.endsWith("_discard") ? SamplingResult.drop() : SamplingResult.recordAndSample();
        }

        @Override
        public String getDescription() {
            return "NameSampler";
        }
    };

    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    private final InMemorySpanExporter exporter = InMemorySpanExporter.create();
    private TracingSettingsUpdater settingsUpdater;
    private final List<OpenTelemetrySdk> sdks = new ArrayList<>();

    @Before
    public void resetExporter() {
        exporter.reset();
    }

    @After
    public void shutdownSdks() {
        sdks.forEach(OpenTelemetrySdk::close);
    }

    public void testConstructorWithMeterProviderSupplierDoesNotThrow() {
        new APMTracingService(threadContext, Settings.EMPTY, MeterProvider::noop);
    }

    public void testTracingCanBeEnabledAfterStartingDisabled() {
        final APMTracer tracer = tracer(disabledSettings());
        assertThat(isRecorded(tracer.spanBuilder("name1").startSpan()), is(false));
        assertThat(finishedSpanNames(), empty());

        updateSettings(Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true));

        startAndEnd(tracer, "name2");
        assertThat(finishedSpanNames(), containsInAnyOrder("name2"));
    }

    public void testSpanBuilderRejectsNullName() {
        expectThrows(AssertionError.class, () -> tracer(enabledSettings()).spanBuilder(null));
    }

    public void testTracingResumesAfterDisableAndReEnable() {
        final APMTracer tracer = tracer(enabledSettings());

        startAndEnd(tracer, "resume-test-1");
        assertThat(finishedSpanNames(), containsInAnyOrder("resume-test-1"));

        updateSettings(Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), false));
        exporter.reset();
        final Span disabled = tracer.spanBuilder("resume-test-2").startSpan();
        assertThat(isRecorded(disabled), is(false));
        disabled.end();
        assertThat(finishedSpanNames(), empty());

        updateSettings(Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true));
        startAndEnd(tracer, "resume-test-3");
        assertThat(finishedSpanNames(), containsInAnyOrder("resume-test-3"));
    }

    public void testStartSpanRecordsSpanOnEnd() {
        final APMTracer tracer = tracer(enabledSettings());

        final Span span = tracer.spanBuilder("name1").startSpan();
        assertThat(span, instanceOf(APMSpan.class));
        assertThat(isRecorded(span), is(true));
        assertThat(finishedSpanNames(), empty()); // not ended yet

        span.end();

        final SpanData data = finishedSpan("name1");
        assertThat(data.getKind(), is(SpanKind.INTERNAL));
        assertThat(data.getParentSpanContext().isValid(), is(false));
    }

    public void testStartSpanDoesNotChangeTheThreadContext() {
        final APMTracer tracer = tracer(enabledSettings());
        threadContext.putHeader(Task.TRACE_PARENT_HTTP_HEADER, TRACE_PARENT);
        final var headersBefore = threadContext.getHeaders();

        final Span span = tracer.spanBuilder("name1").startSpan();

        assertThat(threadContext.getTransient(Task.APM_TRACE_CONTEXT), nullValue());
        assertThat(threadContext.getHeaders(), equalTo(headersBefore));
        assertThat(tracer.currentSpan(), sameInstance(Span.noop()));
        span.end();
    }

    public void testSpanWithoutParentStartsANewTrace() {
        final APMTracer tracer = tracer(enabledSettings());

        startAndEnd(tracer, "name1");

        assertThat(finishedSpan("name1").getParentSpanContext().isValid(), is(false));
    }

    public void testStoredSpanIsTheCurrentSpan() {
        final APMTracer tracer = tracer(enabledSettings());
        final Span span = tracer.spanBuilder("name1").startSpan();

        final SpanContext current;
        try (var ignored = span.storeInThreadContext()) {
            current = currentSpanContext();
            assertThat(current.isValid(), is(true));
            assertThat(current.isSampled(), is(true));
            assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), equalTo(traceParent(current)));
            assertThat(tracer.currentSpan(), instanceOf(APMSpan.class));
            assertThat(spanContextOf(((APMSpan) tracer.currentSpan()).context()), equalTo(current));
        }
        span.end();

        assertThat(finishedSpan("name1").getSpanContext(), equalTo(current));
    }

    public void testClosingTheStoredContextRestoresTheThreadContext() {
        final APMTracer tracer = tracer(enabledSettings());
        threadContext.putHeader(Task.TRACE_PARENT_HTTP_HEADER, TRACE_PARENT);
        threadContext.putHeader(Task.TRACE_STATE, "vendor=value");
        final Span span = tracer.spanBuilder("name1").startSpan();

        try (var ignored = span.storeInThreadContext()) {
            assertThat(threadContext.getTransient(Task.APM_TRACE_CONTEXT), notNullValue());
        }

        assertThat(threadContext.getTransient(Task.APM_TRACE_CONTEXT), nullValue());
        assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), equalTo(TRACE_PARENT));
        assertThat(threadContext.getHeader(Task.TRACE_STATE), equalTo("vendor=value"));
        span.end();
    }

    public void testClosingTheStoredContextKeepsTheResponseHeaders() {
        final APMTracer tracer = tracer(enabledSettings());
        final Span span = tracer.spanBuilder("name1").startSpan();

        try (var ignored = span.storeInThreadContext()) {
            threadContext.addResponseHeader("Warning", "something");
        }

        assertThat(threadContext.getResponseHeaders().get("Warning"), equalTo(List.of("something")));
        span.end();
    }

    public void testTraceParentHeaderFollowsTheCurrentSpan() {
        final APMTracer tracer = tracer(enabledSettings(), 1);
        final Span root = tracer.spanBuilder("root").startSpan();

        try (var ignored = root.storeInThreadContext()) {
            final SpanContext rootContext = currentSpanContext();
            assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), equalTo(traceParent(rootContext)));

            final Span child = tracer.spanBuilder("child").startSpan();
            try (var ignored2 = child.storeInThreadContext()) {
                final SpanContext childContext = currentSpanContext();
                assertThat(childContext.getSpanId(), not(equalTo(rootContext.getSpanId())));
                assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), equalTo(traceParent(childContext)));
            }
            child.end();

            assertThat(currentSpanContext(), equalTo(rootContext));
            assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), equalTo(traceParent(rootContext)));
        }
        root.end();
    }

    public void testNoopSpanCannotBeStoredInTheThreadContext() {
        final Tracer noop = Tracer.noop();
        final var headersBefore = threadContext.getHeaders();

        try (var ignored = noop.spanBuilder("name1").startSpan().storeInThreadContext()) {
            assertThat(threadContext.getTransient(Task.APM_TRACE_CONTEXT), nullValue());
        }

        assertThat(threadContext.getHeaders(), equalTo(headersBefore));
    }

    public void testChildSpanIsStartedUnderTheCurrentSpan() {
        final APMTracer tracer = tracer(enabledSettings(), 1);
        final Span root = tracer.spanBuilder("root").startSpan();

        try (var ignored = root.storeInThreadContext()) {
            startAndEnd(tracer, "child");
        }
        root.end();

        final SpanData rootData = finishedSpan("root");
        final SpanData childData = finishedSpan("child");
        assertThat(childData.getParentSpanId(), equalTo(rootData.getSpanId()));
        assertThat(childData.getTraceId(), equalTo(rootData.getTraceId()));
    }

    public void testChildSpanIsStartedUnderTheGivenParent() {
        final APMTracer tracer = tracer(enabledSettings(), 1);
        final Span root = tracer.spanBuilder("root").startSpan();

        tracer.spanBuilder("child").setParent(root).startSpan().end();
        root.end();

        assertThat(finishedSpan("child").getParentSpanId(), equalTo(finishedSpan("root").getSpanId()));
        assertThat(finishedSpan("child").getTraceId(), equalTo(finishedSpan("root").getTraceId()));
    }

    public void testGivenParentTakesPrecedenceOverTheCurrentSpan() {
        final APMTracer tracer = tracer(enabledSettings(), 1);
        final Span current = tracer.spanBuilder("current").startSpan();
        final Span given = tracer.spanBuilder("given").startSpan();

        try (var ignored = current.storeInThreadContext()) {
            tracer.spanBuilder("child").setParent(given).startSpan().end();
        }
        given.end();
        current.end();

        assertThat(finishedSpan("child").getParentSpanId(), equalTo(finishedSpan("given").getSpanId()));
    }

    public void testCurrentSpanCanBeGivenAsTheParent() {
        final APMTracer tracer = tracer(enabledSettings(), 1);
        final Span root = tracer.spanBuilder("root").startSpan();

        final Span child;
        try (var ignored = root.storeInThreadContext()) {
            child = tracer.spanBuilder("child").setParent(tracer.currentSpan()).startSpan();
        }
        child.end();
        root.end();

        assertThat(finishedSpan("child").getParentSpanId(), equalTo(finishedSpan("root").getSpanId()));
    }

    public void testNoParentStartsANewTraceInsteadOfInheritingTheCurrentSpan() {
        final APMTracer tracer = tracer(enabledSettings(), 0);
        final Span root = tracer.spanBuilder("root").startSpan();

        try (var ignored = root.storeInThreadContext()) {
            // a root, so it is at depth 0 and is not dropped by the limit, which would drop a child of the current span
            final Span span = tracer.spanBuilder("new-root").setNoParent().startSpan();
            assertThat(isRecorded(span), is(true));
            span.end();
        }
        root.end();

        assertThat(finishedSpan("new-root").getParentSpanContext().isValid(), is(false));
        assertThat(finishedSpan("new-root").getTraceId(), not(equalTo(finishedSpan("root").getTraceId())));
    }

    public void testNoParentIgnoresTheRemoteParentInTheRequestHeaders() {
        final APMTracer tracer = tracer(enabledSettings());
        threadContext.putHeader(Task.TRACE_PARENT_HTTP_HEADER, TRACE_PARENT);

        tracer.spanBuilder("new-root").setNoParent().startSpan().end();

        final SpanData data = finishedSpan("new-root");
        assertThat(data.getParentSpanContext().isValid(), is(false));
        assertThat(data.getTraceId(), not(equalTo(TRACE_ID)));
    }

    public void testNoParentIsNotDroppedWithTheTraceOfTheCurrentSpan() {
        final APMTracer tracer = tracer(enabledSettings(), 5, false, Sampler.parentBased(Sampler.alwaysOn()));
        threadContext.putHeader(Task.TRACE_PARENT_HTTP_HEADER, UNSAMPLED_TRACE_PARENT);
        final Span entry = tracer.spanBuilder("entry").startSpan();
        assertThat(isRecorded(entry), is(false));

        try (var ignored = entry.storeInThreadContext()) {
            // a child of the current span is dropped with it, but a new trace is sampled on its own
            assertThat(isRecorded(tracer.spanBuilder("child").startSpan()), is(false));

            final Span newRoot = tracer.spanBuilder("new-root").setNoParent().startSpan();
            assertThat(isRecorded(newRoot), is(true));
            newRoot.end();
        }

        assertThat(finishedSpan("new-root").getTraceId(), not(equalTo(TRACE_ID)));
    }

    public void testChildOfASpanStartedWithNoParentBelongsToTheNewTrace() {
        final APMTracer tracer = tracer(enabledSettings(), 1);
        final Span root = tracer.spanBuilder("root").startSpan();

        try (var ignored = root.storeInThreadContext()) {
            final Span newRoot = tracer.spanBuilder("new-root").setNoParent().startSpan();
            try (var ignored2 = newRoot.storeInThreadContext()) {
                startAndEnd(tracer, "child");
            }
            newRoot.end();
        }
        root.end();

        assertThat(finishedSpan("child").getTraceId(), equalTo(finishedSpan("new-root").getTraceId()));
        assertThat(finishedSpan("child").getParentSpanId(), equalTo(finishedSpan("new-root").getSpanId()));
        assertThat(finishedSpan("child").getTraceId(), not(equalTo(finishedSpan("root").getTraceId())));
    }

    public void testNoParentSetAfterTheParentWins() {
        final APMTracer tracer = tracer(enabledSettings());
        final Span given = tracer.spanBuilder("given").startSpan();
        tracer.spanBuilder("name1").setParent(given).setNoParent().startSpan().end();
        given.end();

        assertThat(finishedSpan("name1").getParentSpanContext().isValid(), is(false));
        assertThat(finishedSpan("name1").getTraceId(), not(equalTo(finishedSpan("given").getTraceId())));
    }

    public void testParentSetAfterNoParentWins() {
        final APMTracer tracer = tracer(enabledSettings(), 1);
        final Span given = tracer.spanBuilder("given").startSpan();
        tracer.spanBuilder("name1").setNoParent().setParent(given).startSpan().end();
        given.end();

        assertThat(finishedSpan("name1").getParentSpanId(), equalTo(finishedSpan("given").getSpanId()));
        assertThat(finishedSpan("name1").getTraceId(), equalTo(finishedSpan("given").getTraceId()));
    }

    /** A noop span is not a context, so as a parent it changes nothing: the parent is inherited, as if none had been given. */
    public void testNoopSpanAsTheParentIsIgnored() {
        final APMTracer tracer = tracer(enabledSettings(), 1);
        final Span root = tracer.spanBuilder("root").startSpan();

        try (var ignored = root.storeInThreadContext()) {
            tracer.spanBuilder("child").setParent(Span.noop()).startSpan().end();
        }
        root.end();

        assertThat(finishedSpan("child").getParentSpanId(), equalTo(finishedSpan("root").getSpanId()));
    }

    public void testNoopSpanAsTheParentStillFollowsTheRemoteParent() {
        final APMTracer tracer = tracer(enabledSettings());
        threadContext.putHeader(Task.TRACE_PARENT_HTTP_HEADER, TRACE_PARENT);

        tracer.spanBuilder("name1").setParent(Span.noop()).startSpan().end();

        assertThat(finishedSpan("name1").getTraceId(), equalTo(TRACE_ID));
        assertThat(finishedSpan("name1").getParentSpanId(), equalTo(SPAN_ID));
    }

    public void testNoParentOnADisabledTracerDoesNotRecord() {
        final APMTracer tracer = tracer(enabledSettings());
        settingsUpdater.update(disabledSettings());

        assertThat(isRecorded(tracer.spanBuilder("name1").setNoParent().startSpan()), is(false));
    }

    /**
     * Simulates a data node: the {@code traceparent} request header is propagated over transport (as a regular header) but the context
     * of the span is not (transients are not serialised over transport). The span is started as a child of the remote span.
     */
    public void testRemoteParentIsTakenFromTheTraceParentHeader() {
        final APMTracer tracer = tracer(enabledSettings());
        threadContext.putHeader(Task.TRACE_PARENT_HTTP_HEADER, TRACE_PARENT);

        startAndEnd(tracer, "name1");

        final SpanData data = finishedSpan("name1");
        assertThat(data.getTraceId(), equalTo(TRACE_ID));
        assertThat(data.getParentSpanId(), equalTo(SPAN_ID));
        assertThat(data.getParentSpanContext().isRemote(), is(true));
    }

    public void testTraceStateIsPropagatedFromTheRemoteParent() {
        final APMTracer tracer = tracer(enabledSettings());
        threadContext.putHeader(Task.TRACE_PARENT_HTTP_HEADER, TRACE_PARENT);
        threadContext.putHeader(Task.TRACE_STATE, "vendor=value");

        final Span span = tracer.spanBuilder("name1").startSpan();
        try (var ignored = span.storeInThreadContext()) {
            assertThat(threadContext.getHeader(Task.TRACE_STATE), equalTo("vendor=value"));
        }
        span.end();

        assertThat(finishedSpan("name1").getSpanContext().getTraceState().get("vendor"), equalTo("value"));
    }

    public void testLocalParentTakesPrecedenceOverRemoteParent() {
        final APMTracer tracer = tracer(enabledSettings(), 1);
        final Span root = tracer.spanBuilder("root").startSpan();

        try (var ignored = root.storeInThreadContext()) {
            // a different remote parent, as if the header was stale
            threadContext.putHeaderAllowOverwrite(Task.TRACE_PARENT_HTTP_HEADER, TRACE_PARENT);
            startAndEnd(tracer, "child");
        }
        root.end();

        assertThat(finishedSpan("child").getParentSpanId(), equalTo(finishedSpan("root").getSpanId()));
    }

    public void testCurrentSpanIsNoopWithoutAStoredSpan() {
        assertThat(tracer(enabledSettings()).currentSpan(), sameInstance(Span.noop()));
    }

    public void testCurrentSpanIsTheLiveSpan() {
        final APMTracer tracer = tracer(enabledSettings());
        final Span span = tracer.spanBuilder("name1").startSpan();

        try (var ignored = span.storeInThreadContext()) {
            final Span current = tracer.currentSpan();
            assertThat(current, instanceOf(APMSpan.class));
            current.setAttribute("added.later", "value");
        }
        span.end();

        assertThat(finishedSpan("name1").getAttributes().get(stringKey("added.later")), equalTo("value"));
    }

    public void testCurrentSpanOfASpanThatHasEndedIsNotRecorded() {
        final APMTracer tracer = tracer(enabledSettings(), 1);
        final Span span = tracer.spanBuilder("name1").startSpan();

        try (var ignored = span.storeInThreadContext()) {
            span.end();

            final Span current = tracer.currentSpan();
            assertThat(current, instanceOf(APMSpan.class));
            assertThat(isRecorded(current), is(false));

            tracer.spanBuilder("child").setParent(current).startSpan().end();
        }

        assertThat(finishedSpan("child").getParentSpanId(), equalTo(finishedSpan("name1").getSpanId()));
    }

    public void testCurrentSpanOfASpanThatIsNotRecordedIsNotRecorded() {
        final APMTracer tracer = tracer(enabledSettings(), 1);
        final Span span = tracer.spanBuilder("name1_discard").startSpan();

        try (var ignored = span.storeInThreadContext()) {
            final Span current = tracer.currentSpan();
            assertThat(current, instanceOf(APMSpan.class));
            assertThat(isRecorded(current), is(false));

            assertThat(isRecorded(tracer.spanBuilder("child").setParent(current).startSpan()), is(false));
        }

        assertThat(finishedSpanNames(), empty());
    }

    public void testDisabledTracerDropsTheLocalTrace() {
        final APMTracer tracer = tracer(disabledSettings());
        threadContext.putHeader(Task.TRACE_PARENT_HTTP_HEADER, TRACE_PARENT);
        threadContext.putHeader(Task.TRACE_STATE, "vendor=value");

        final Span span = tracer.spanBuilder("name1").startSpan();
        assertThat(isRecorded(span), is(false));
        try (var ignored = span.storeInThreadContext()) {
            assertLocalTraceDropped();
        }
        span.end();

        assertThat(finishedSpanNames(), empty());
        // restored
        assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), equalTo(TRACE_PARENT));
    }

    public void testDisabledTracerClearsTheContextOfAnExistingTrace() {
        final APMTracer tracer = tracer(enabledSettings());
        final Span root = tracer.spanBuilder("root").startSpan();

        try (var ignored = root.storeInThreadContext()) {
            assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), notNullValue());
            updateSettings(Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), false));

            // not even the spans of the current trace are recorded
            final Span span = tracer.spanBuilder("name1").setParent(root).startSpan();
            assertThat(isRecorded(span), is(false));
            try (var ignored2 = span.storeInThreadContext()) {
                assertLocalTraceDropped();
            }
        }
        root.end();

        assertThat(finishedSpanNames(), not(hasItem("name1")));
    }

    public void testSpansStartedWhileTheTracerIsDisabledAreNeverRecorded() {
        final APMTracer tracer = tracer(disabledSettings());

        final Span span = tracer.spanBuilder("name1").startSpan();

        settingsUpdater.update(enabledSettings());

        // even though tracing is now enabled, the parent is sampled out, and will not allow to start a new child span
        final Span child = tracer.spanBuilder("child").setParent(span).startSpan();

        assertThat(span, instanceOf(APMSpan.class));
        assertThat(isRecorded(span), is(false));
        assertThat(isRecorded(child), is(false));
        child.end();
        span.end();
        assertThat(finishedSpanNames(), empty());
    }

    public void testCurrentSpanOfADroppedTraceIsNotRecorded() {
        final APMTracer tracer = tracer(disabledSettings());
        final Span span = tracer.spanBuilder("name1").startSpan();

        try (var ignored = span.storeInThreadContext()) {
            final Span current = tracer.currentSpan();
            assertThat(current, instanceOf(APMSpan.class));
            assertThat(isRecorded(current), is(false));
        }
    }

    /**
     * A span that is not recorded, e.g. due to sampling, is not exported, but its (not sampled) context is what is stored and
     * propagated, so that the decision is shared by everything that is started under it.
     */
    public void testRootSpanThatIsNotRecordedPropagatesTheDecision() {
        final APMTracer tracer = tracer(enabledSettings());
        final Span span = tracer.spanBuilder("name1_discard").startSpan();
        assertThat(isRecorded(span), is(false));

        try (var ignored = span.storeInThreadContext()) {
            final SpanContext current = currentSpanContext();
            assertThat(current.isValid(), is(true));
            assertThat(current.isSampled(), is(false));
            assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), equalTo(traceParent(current)));
        }
        span.end();

        assertThat(finishedSpanNames(), empty());
    }

    public void testSpansUnderAnUnsampledTraceAreDropped() {
        final APMTracer tracer = tracer(enabledSettings(), 5);
        final Span root = tracer.spanBuilder("root").startSpan();

        try (var ignored = root.storeInThreadContext()) {
            final Span discarded = tracer.spanBuilder("child_discard").startSpan();
            try (var ignored2 = discarded.storeInThreadContext()) {
                final SpanContext discardedContext = currentSpanContext();
                assertThat(discardedContext.isSampled(), is(false));

                // the sampler would record this one, but it is part of a dropped trace
                final Span grandchild = tracer.spanBuilder("grandchild").startSpan();
                assertThat(isRecorded(grandchild), is(false));
                try (var ignored3 = grandchild.storeInThreadContext()) {
                    // the decision is the one of the trace
                    assertThat(currentSpanContext().getTraceId(), equalTo(discardedContext.getTraceId()));
                    assertThat(currentSpanContext().isSampled(), is(false));
                }
            }
        }
        root.end();

        assertThat(finishedSpanNames(), containsInAnyOrder("root"));
    }

    public void testRemoteParentThatIsNotSampledDropsTheLocalTrace() {
        final APMTracer tracer = tracer(enabledSettings(), 5, false, Sampler.parentBased(Sampler.alwaysOn()));
        threadContext.putHeader(Task.TRACE_PARENT_HTTP_HEADER, UNSAMPLED_TRACE_PARENT);

        final Span entry = tracer.spanBuilder("entry").startSpan();
        assertThat(isRecorded(entry), is(false));

        try (var ignored = entry.storeInThreadContext()) {
            assertThat(isRecorded(tracer.spanBuilder("child").startSpan()), is(false));

            final SpanContext current = currentSpanContext();
            assertThat(current.getTraceId(), equalTo(TRACE_ID));
            assertThat(current.isSampled(), is(false));
            assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), startsWith("00-" + TRACE_ID + "-"));
            assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), equalTo(traceParent(current)));
        }

        assertThat(finishedSpanNames(), empty());
    }

    public void testRemoteParentThatIsSampledIsFollowed() {
        final APMTracer tracer = tracer(enabledSettings(), 5, false, Sampler.parentBased(Sampler.alwaysOff()));
        threadContext.putHeader(Task.TRACE_PARENT_HTTP_HEADER, TRACE_PARENT);

        startAndEnd(tracer, "entry");

        assertThat(finishedSpan("entry").getTraceId(), equalTo(TRACE_ID));
    }

    public void testLocalRootSpanUsesTheTraceStartTime() {
        final APMTracer tracer = tracer(enabledSettings());
        final Instant startTime = Instant.ofEpochMilli(randomLongBetween(0, 4_000_000_000_000L));
        threadContext.putTransient(Task.LOCAL_TRACE_START_TIME, startTime);

        startAndEnd(tracer, "name1");

        assertThat(finishedSpan("name1").getStartEpochNanos(), equalTo(epochNanos(startTime)));
    }

    public void testTraceStartTimeIsRemovedFromTheThreadContextOnceItIsUsed() {
        final APMTracer tracer = tracer(enabledSettings());
        final Instant startTime = Instant.ofEpochMilli(randomLongBetween(0, 1_000_000_000_000L));
        threadContext.putTransient(Task.LOCAL_TRACE_START_TIME, startTime);

        final Span first = tracer.spanBuilder("first").startSpan();

        assertThat(threadContext.getTransient(Task.LOCAL_TRACE_START_TIME), nullValue());
        first.end();
        // the next one, that does not belong to the same trace, starts when it is started
        startAndEnd(tracer, "second");

        assertThat(finishedSpan("first").getStartEpochNanos(), equalTo(epochNanos(startTime)));
        assertThat(finishedSpan("second").getStartEpochNanos(), not(equalTo(epochNanos(startTime))));
    }

    public void testTraceStartTimeIsKeptWhenTheSpanDoesNotUseIt() {
        final APMTracer tracer = tracer(enabledSettings(), 1);
        final Span root = tracer.spanBuilder("root").startSpan();
        final Instant startTime = Instant.ofEpochMilli(randomLongBetween(0, 1_000_000_000_000L));
        threadContext.putTransient(Task.LOCAL_TRACE_START_TIME, startTime);

        // a child span starts when it is started, so the start time is not for it
        tracer.spanBuilder("child").setParent(root).startSpan().end();

        assertThat(threadContext.getTransient(Task.LOCAL_TRACE_START_TIME), equalTo(startTime));
        root.end();
    }

    public void testTraceStartTimeIsIgnoredForChildSpans() {
        final APMTracer tracer = tracer(enabledSettings(), 1);
        final Instant startTime = Instant.ofEpochMilli(randomLongBetween(0, 1_000_000_000_000L));
        threadContext.putTransient(Task.LOCAL_TRACE_START_TIME, startTime);

        final Span root = tracer.spanBuilder("root").startSpan();
        tracer.spanBuilder("child").setParent(root).startSpan().end();
        root.end();

        assertThat(finishedSpan("root").getStartEpochNanos(), equalTo(epochNanos(startTime)));
        assertThat(finishedSpan("child").getStartEpochNanos(), not(equalTo(epochNanos(startTime))));
    }

    public void testAttributesSetOnTheBuilderAreRecorded() {
        final APMTracer tracer = tracer(enabledSettings());

        tracer.spanBuilder("name1")
            .setAttribute("a.boolean", true)
            .setAttribute("a.double", 1.5d)
            .setAttribute("a.long", 42L)
            .setAttribute("an.int", 7)
            .setAttribute("a.string", "value")
            .startSpan()
            .end();

        final Attributes attributes = finishedSpan("name1").getAttributes();
        assertThat(attributes.get(AttributeKey.booleanKey("a.boolean")), equalTo(true));
        assertThat(attributes.get(AttributeKey.doubleKey("a.double")), equalTo(1.5d));
        assertThat(attributes.get(AttributeKey.longKey("a.long")), equalTo(42L));
        assertThat(attributes.get(AttributeKey.longKey("an.int")), equalTo(7L));
        assertThat(attributes.get(stringKey("a.string")), equalTo("value"));
    }

    public void testAttributesSetOnTheSpanAreRecorded() {
        final APMTracer tracer = tracer(enabledSettings());

        final Span span = tracer.spanBuilder("name1").startSpan();
        span.setAttribute("a.boolean", true).setAttribute("a.double", 1.5d).setAttribute("a.long", 42L).setAttribute("a.string", "value");
        span.end();

        final Attributes attributes = finishedSpan("name1").getAttributes();
        assertThat(attributes.get(AttributeKey.booleanKey("a.boolean")), equalTo(true));
        assertThat(attributes.get(AttributeKey.doubleKey("a.double")), equalTo(1.5d));
        assertThat(attributes.get(AttributeKey.longKey("a.long")), equalTo(42L));
        assertThat(attributes.get(stringKey("a.string")), equalTo("value"));
    }

    public void testSettingAnAttributeToNullIsRejected() {
        final APMTracer tracer = tracer(enabledSettings());

        expectThrows(AssertionError.class, () -> tracer.spanBuilder("name1").setAttribute("a.string", (String) null));
        final Span span = tracer.spanBuilder("name1").startSpan();
        expectThrows(AssertionError.class, () -> span.setAttribute("a.string", (String) null));
        span.end();
    }

    public void testSpanKindIsInternalUnlessSet() {
        final APMTracer tracer = tracer(enabledSettings());

        tracer.spanBuilder("server-span").setSpanKind(SpanKind.SERVER).startSpan().end();
        // the attributes do not make the kind
        tracer.spanBuilder("internal-span").setAttribute("http.method", "GET").startSpan().end();

        assertThat(finishedSpan("server-span").getKind(), is(SpanKind.SERVER));
        assertThat(finishedSpan("internal-span").getKind(), is(SpanKind.INTERNAL));
    }

    public void testRecordExceptionWithStacksDisabledEmitsTypeAndMessageOnly() {
        final APMTracer tracer = tracer(enabledSettings(), 0, false);

        final Span span = tracer.spanBuilder("span-with-error").startSpan();
        assertThat(span.recordException(new IllegalStateException("boom")), sameInstance(span));
        span.end();

        final EventData event = exceptionEvent(finishedSpan("span-with-error"));
        assertThat(event.getAttributes().get(stringKey("exception.type")), is(IllegalStateException.class.getName()));
        assertThat(event.getAttributes().get(stringKey("exception.message")), is("boom"));
        assertThat(event.getAttributes().get(stringKey("exception.stacktrace")), nullValue());
    }

    public void testRecordExceptionWithStacksEnabledRecordsTheStackTrace() {
        final APMTracer tracer = tracer(enabledSettings(), 0, true);

        final Span span = tracer.spanBuilder("span-with-error").startSpan();
        span.recordException(new IllegalStateException("boom"));
        span.end();

        final EventData event = exceptionEvent(finishedSpan("span-with-error"));
        assertThat(event.getAttributes().get(stringKey("exception.type")), is(IllegalStateException.class.getName()));
        assertThat(event.getAttributes().get(stringKey("exception.message")), is("boom"));
        assertThat(event.getAttributes().get(stringKey("exception.stacktrace")), notNullValue());
    }

    public void testRecordExceptionWithNullMessageOmitsTheMessageAttribute() {
        final APMTracer tracer = tracer(enabledSettings(), 0, false);

        final Span span = tracer.spanBuilder("span-with-error").startSpan();
        span.recordException(new IllegalStateException());
        span.end();

        final EventData event = exceptionEvent(finishedSpan("span-with-error"));
        assertThat(event.getAttributes().get(stringKey("exception.type")), is(IllegalStateException.class.getName()));
        assertThat(event.getAttributes().get(stringKey("exception.message")), nullValue());
    }

    public void testRecordExceptionStacksCanBeUpdated() {
        final APMTracer tracer = tracer(enabledSettings(), 0, false);
        updateSettings(Settings.builder().put(OtelSdkSettings.TELEMETRY_TRACING_RECORD_EXCEPTION_STACKS.getKey(), true));

        final Span span = tracer.spanBuilder("span-with-error").startSpan();
        span.recordException(new IllegalStateException("boom"));
        span.end();

        assertThat(exceptionEvent(finishedSpan("span-with-error")).getAttributes().get(stringKey("exception.stacktrace")), notNullValue());
    }

    public void testEndingASpanTwiceHasNoEffect() {
        final APMTracer tracer = tracer(enabledSettings());

        final Span span = tracer.spanBuilder("name1").startSpan();
        span.end();
        span.end();

        assertThat(finishedSpanNames(), containsInAnyOrder("name1"));
    }

    public void testSpanCanBeEndedOnAnotherThread() {
        final APMTracer tracer = tracer(enabledSettings());
        final Span span = tracer.spanBuilder("name1").startSpan();

        runInParallel(1, i -> span.end());

        assertThat(finishedSpanNames(), containsInAnyOrder("name1"));
    }

    public void testSpanThatIsNotRecordedCanBeUsedLikeAnyOtherSpan() {
        final APMTracer tracer = tracer(disabledSettings());

        final Span span = tracer.spanBuilder("name1").setAttribute("a", "b").startSpan();
        span.setAttribute("a", true).setAttribute("a", 1.0d).setAttribute("a", 1L).setAttribute("a", "b").recordException(new Exception());
        span.end();
        span.end();

        assertThat(finishedSpanNames(), empty());
    }

    // helpers

    /** Changes the settings of the tracer that was created last, the way an update of the cluster settings does. */
    private void updateSettings(Settings.Builder changes) {
        settingsUpdater.update(changes.build());
    }

    private static Settings.Builder enabledSettingsBuilder() {
        return Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true);
    }

    private static Settings enabledSettings() {
        return enabledSettingsBuilder().build();
    }

    private static Settings disabledSettings() {
        return Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), false).build();
    }

    private APMTracer tracer(Settings settings) {
        return tracer(settings, 0);
    }

    private APMTracer tracer(Settings settings, int maxTraceDepth) {
        return tracer(settings, maxTraceDepth, false);
    }

    private APMTracer tracer(Settings settings, int maxTraceDepth, boolean recordExceptionStacks) {
        return tracer(settings, maxTraceDepth, recordExceptionStacks, NAME_SAMPLER);
    }

    private APMTracer tracer(Settings settings, int maxTraceDepth, boolean recordExceptionStacks, Sampler sampler) {
        // everything that the limits of the service let through is sampled, unless the given sampler drops it
        final Settings allSampled = Settings.builder()
            .put(settings)
            .put(OtelSdkSettings.TELEMETRY_TRACING_SAMPLE_RATE.getKey(), 1.0)
            .put(OtelSdkSettings.TELEMETRY_TRACING_MAX_DEPTH.getKey(), maxTraceDepth)
            .put(OtelSdkSettings.TELEMETRY_TRACING_RECORD_EXCEPTION_STACKS.getKey(), recordExceptionStacks)
            .build();
        final APMTracingService service = new APMTracingService(threadContext, allSampled, serviceSampler -> () -> {
            final SdkTracerProvider tracerProvider = SdkTracerProvider.builder().setSampler(new Sampler() {
                @Override
                public SamplingResult shouldSample(
                    Context parentContext,
                    String traceId,
                    String name,
                    SpanKind spanKind,
                    Attributes attributes,
                    List<LinkData> parentLinks
                ) {
                    final SamplingResult result = sampler.shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);
                    return result.getDecision() == SamplingDecision.DROP
                        ? result
                        : serviceSampler.shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);
                }

                @Override
                public String getDescription() {
                    return "test sampler in front of " + serviceSampler.getDescription();
                }
            }).addSpanProcessor(SimpleSpanProcessor.create(exporter)).build();
            final OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
            sdks.add(sdk);
            return sdk;
        });
        settingsUpdater = new TracingSettingsUpdater(service, allSampled);
        service.start();
        return service.tracer();
    }

    private static void startAndEnd(Tracer tracer, String name) {
        tracer.spanBuilder(name).startSpan().end();
    }

    /** Whether the span is recorded, which is not the case for the spans that are dropped, nor once they have ended. */
    private static boolean isRecorded(Span span) {
        return span instanceof APMSpan apmSpan && spanOf(apmSpan.context()).isRecording();
    }

    private List<String> finishedSpanNames() {
        return exporter.getFinishedSpanItems().stream().map(SpanData::getName).toList();
    }

    private SpanData finishedSpan(String name) {
        final List<SpanData> spans = exporter.getFinishedSpanItems().stream().filter(s -> s.getName().equals(name)).toList();
        assertThat("finished spans named [" + name + "] in " + finishedSpanNames(), spans.size(), equalTo(1));
        return spans.getFirst();
    }

    private static EventData exceptionEvent(SpanData span) {
        final List<EventData> events = span.getEvents().stream().filter(e -> e.getName().equals("exception")).toList();
        assertThat(events.size(), equalTo(1));
        return events.getFirst();
    }

    private static long epochNanos(Instant instant) {
        return TimeUnit.SECONDS.toNanos(instant.getEpochSecond()) + instant.getNano();
    }

    /** Asserts that the thread context marks the local trace as dropped and that no trace context is propagated. */
    private void assertLocalTraceDropped() {
        assertThat(currentSpanContext().isValid(), is(false));
        assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), nullValue());
        assertThat(threadContext.getHeader(Task.TRACE_STATE), nullValue());
    }

    private SpanContext currentSpanContext() {
        return spanContextOf(threadContext.getTransient(Task.APM_TRACE_CONTEXT));
    }

    private static SpanContext spanContextOf(Context context) {
        return spanOf(context).getSpanContext();
    }

    private static io.opentelemetry.api.trace.Span spanOf(Context context) {
        assertThat(context, notNullValue());
        return io.opentelemetry.api.trace.Span.fromContext(context);
    }

    private static String traceParent(SpanContext spanContext) {
        return "00-" + spanContext.getTraceId() + "-" + spanContext.getSpanId() + "-" + spanContext.getTraceFlags().asHex();
    }
}
