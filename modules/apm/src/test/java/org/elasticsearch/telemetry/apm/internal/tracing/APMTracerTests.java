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
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;

import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
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
    private final List<SdkTracerProvider> sdks = new ArrayList<>();

    @Before
    public void resetExporter() {
        exporter.reset();
    }

    @After
    public void shutdownSdks() {
        sdks.forEach(SdkTracerProvider::close);
        sdks.clear();
    }

    // ------------------------------------------------------------------------------------------------------------------------------
    // The life cycle of a span
    // ------------------------------------------------------------------------------------------------------------------------------

    public void testConstructorWithMeterProviderSupplierDoesNotThrow() {
        new APMTracingService(threadContext, Settings.EMPTY, MeterProvider::noop);
    }

    public void testSpanBuilderRejectsNullName() {
        expectThrows(AssertionError.class, () -> tracer(enabledSettings()).spanBuilder(null));
    }

    public void testStartSpanRecordsSpanOnEnd() {
        final APMTracer tracer = tracer(enabledSettings());

        final Span span = tracer.spanBuilder("name1").startSpan();
        assertThat(span, instanceOf(APMSpan.class));
        assertThat(span.isRecording(), is(true));
        assertThat(finishedSpanNames(), empty()); // not ended yet

        span.end();
        assertThat(span.isRecording(), is(false));

        final SpanData data = finishedSpan("name1");
        assertThat(data.getKind(), is(SpanKind.INTERNAL));
        // a span without a parent starts a new trace
        assertThat(data.getParentSpanContext().isValid(), is(false));
    }

    /** A span that ends does not record any more, but it is still a parent that the spans under it can be started with. */
    public void testEndedSpanCanStillBeTheParentOfAnotherSpan() {
        final APMTracer tracer = tracer(enabledSettings(), 1);
        final Span span = tracer.spanBuilder("name1").startSpan();
        span.end();

        tracer.spanBuilder("child").setParent(span).startSpan().end();

        assertThat(finishedSpan("child").getParentSpanId(), equalTo(finishedSpan("name1").getSpanId()));
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

    public void testSpanKindIsInternalUnlessSet() {
        final APMTracer tracer = tracer(enabledSettings());

        tracer.spanBuilder("server-span").setSpanKind(SpanKind.SERVER).startSpan().end();
        // the attributes do not make the kind
        tracer.spanBuilder("internal-span").setAttribute("http.method", "GET").startSpan().end();

        assertThat(finishedSpan("server-span").getKind(), is(SpanKind.SERVER));
        assertThat(finishedSpan("internal-span").getKind(), is(SpanKind.INTERNAL));
    }

    // ------------------------------------------------------------------------------------------------------------------------------
    // The attributes and the exceptions of a span
    // ------------------------------------------------------------------------------------------------------------------------------

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

    public void testRecordExceptionStacksCanBeUpdated() {
        final APMTracer tracer = tracer(enabledSettings(), 0, false);
        updateSettings(Settings.builder().put(OtelSdkSettings.TELEMETRY_TRACING_RECORD_EXCEPTION_STACKS.getKey(), true));

        final Span span = tracer.spanBuilder("span-with-error").startSpan();
        span.recordException(new IllegalStateException("boom"));
        span.end();

        assertThat(exceptionEvent(finishedSpan("span-with-error")).getAttributes().get(stringKey("exception.stacktrace")), notNullValue());
    }

    // ------------------------------------------------------------------------------------------------------------------------------
    // Storing a span in the thread context, and the current span
    // ------------------------------------------------------------------------------------------------------------------------------

    public void testStartSpanDoesNotChangeTheThreadContext() {
        final APMTracer tracer = tracer(enabledSettings());
        threadContext.putHeader(Task.TRACE_PARENT_HTTP_HEADER, TRACE_PARENT);
        final var headersBefore = threadContext.getHeaders();

        final Span span = tracer.spanBuilder("name1").startSpan();

        assertThat(threadContext.getSpan(), sameInstance(Span.noop()));
        assertThat(threadContext.getHeaders(), equalTo(headersBefore));
        assertThat(tracer.currentSpan(), sameInstance(Span.noop()));
        span.end();
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
            assertThat(tracer.currentSpan(), sameInstance(span));
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
            assertThat(threadContext.getSpan(), sameInstance(span));
        }

        assertThat(threadContext.getSpan(), sameInstance(Span.noop()));
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
            assertThat(threadContext.getSpan(), sameInstance(Span.noop()));
        }

        assertThat(threadContext.getHeaders(), equalTo(headersBefore));
    }

    public void testCurrentSpanIsNoopWithoutAStoredSpan() {
        assertThat(tracer(enabledSettings()).currentSpan(), sameInstance(Span.noop()));
    }

    public void testCurrentSpanIsTheLiveSpan() {
        final APMTracer tracer = tracer(enabledSettings());
        final Span span = tracer.spanBuilder("name1").startSpan();

        try (var ignored = span.storeInThreadContext()) {
            final Span current = tracer.currentSpan();
            assertThat(current, sameInstance(span));
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
            assertThat(current, sameInstance(span));
            assertThat(current.isRecording(), is(false));

            // it is still the current span, so what is started under it is its child
            startAndEnd(tracer, "child");
        }

        assertThat(finishedSpan("child").getParentSpanId(), equalTo(finishedSpan("name1").getSpanId()));
    }

    public void testCurrentSpanOfASpanThatIsNotSampledIsNotRecorded() {
        final APMTracer tracer = tracer(enabledSettings(), 1);
        final Span span = tracer.spanBuilder("name1_discard").startSpan();

        try (var ignored = span.storeInThreadContext()) {
            final Span current = tracer.currentSpan();
            assertThat(current, sameInstance(span));
            assertThat(current, instanceOf(PropagatedSpan.class));
            assertThat(current.isRecording(), is(false));

            assertThat(tracer.spanBuilder("child").setParent(current).startSpan().isRecording(), is(false));
        }

        assertThat(finishedSpanNames(), empty());
    }

    // ------------------------------------------------------------------------------------------------------------------------------
    // Finding the parent of a span
    // ------------------------------------------------------------------------------------------------------------------------------

    // the current span

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

    // the given parent

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

    // no parent

    public void testNoParentStartsANewTraceInsteadOfInheritingTheCurrentSpan() {
        final APMTracer tracer = tracer(enabledSettings(), 0);
        final Span root = tracer.spanBuilder("root").startSpan();

        try (var ignored = root.storeInThreadContext()) {
            // a root, so it is at depth 0 and is not dropped by the limit, which would drop a child of the current span
            final Span span = tracer.spanBuilder("new-root").setNoParent().startSpan();
            assertThat(span.isRecording(), is(true));
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
        assertThat(entry.isRecording(), is(false));

        try (var ignored = entry.storeInThreadContext()) {
            // a child of the current span is dropped with it, but a new trace is sampled on its own
            assertThat(tracer.spanBuilder("child").startSpan().isRecording(), is(false));

            final Span newRoot = tracer.spanBuilder("new-root").setNoParent().startSpan();
            assertThat(newRoot.isRecording(), is(true));
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
        assertThat(finishedSpan("child").getTraceId(), not(equalTo(finishedSpan("root").getTraceId())));
        assertThat(finishedSpan("child").getParentSpanId(), equalTo(finishedSpan("new-root").getSpanId()));
    }

    /** As in the OTel API, it is the last of the calls that sets the parent that counts. */
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

    // the remote parent

    /**
     * Simulates a data node: the {@code traceparent} request header is propagated over transport (as a regular header) but the span of
     * the other node is not (transients are not serialized over transport). The span is started as a child of the remote span.
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

    // ------------------------------------------------------------------------------------------------------------------------------
    // What is sampled: the decision of the SDK, and how it is passed on to the spans that are started under it
    // ------------------------------------------------------------------------------------------------------------------------------

    public void testSpanThatIsNotSampledIsAPropagatedSpan() {
        final APMTracer tracer = tracer(enabledSettings());

        final Span recorded = tracer.spanBuilder("name1").startSpan();
        final Span notRecorded = tracer.spanBuilder("name2_discard").startSpan();

        assertThat(recorded, instanceOf(APMSpan.class));
        assertThat(recorded.isRecording(), is(true));
        assertThat(notRecorded, instanceOf(PropagatedSpan.class));
        assertThat(notRecorded.isRecording(), is(false));
    }

    public void testPropagatedSpanDoesNotRecordAnything() {
        final APMTracer tracer = tracer(enabledSettings());
        final Span span = tracer.spanBuilder("name1_discard").startSpan();

        assertThat(span.isRecording(), is(false));
        assertThat(span.setAttribute("a.boolean", true), sameInstance(span));
        assertThat(span.setAttribute("a.double", 1.5d), sameInstance(span));
        assertThat(span.setAttribute("a.long", 1L), sameInstance(span));
        assertThat(span.setAttribute("a.string", "value"), sameInstance(span));
        assertThat(span.recordException(new IllegalStateException("boom")), sameInstance(span));
        ((PropagatedSpan) span).setAllAttributes(Attributes.of(stringKey("another.string"), "value"));
        assertThat(((PropagatedSpan) span).setStatus(StatusCode.ERROR, "failed"), sameInstance(span));
        span.end();

        assertThat(span.isRecording(), is(false));
        assertThat(finishedSpanNames(), empty());
    }

    /**
     * A span that is not recorded, e.g. due to sampling, is not exported, but its (not sampled) context is what is stored and
     * propagated, so that the decision is shared by everything that is started under it.
     */
    public void testSpanThatIsNotSampledIsStoredInThreadContextAndPropagated() {
        final APMTracer tracer = tracer(enabledSettings(), 1);
        final Span span = tracer.spanBuilder("name1_discard").startSpan();
        threadContext.putHeader(Task.TRACE_STATE, "vendor=value");

        try (var ignored = span.storeInThreadContext()) {
            final SpanContext current = currentSpanContext();
            assertThat(current.isValid(), is(true));
            assertThat(current.isSampled(), is(false));
            assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), equalTo(traceParent(current)));
            assertThat(tracer.currentSpan(), sameInstance(span));
        }
        span.end();

        // restored
        assertThat(threadContext.getSpan(), sameInstance(Span.noop()));
        assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), nullValue());
        assertThat(threadContext.getHeader(Task.TRACE_STATE), equalTo("vendor=value"));
        assertThat(finishedSpanNames(), empty());
    }

    /** What is started under a span that is not sampled is part of the decision, and is the same span for what propagates the trace. */
    public void testChildOfASpanThatIsNotSampledIsAPropagatedSpanOfTheSameContext() {
        final APMTracer tracer = tracer(enabledSettings(), 5);
        final Span parent = tracer.spanBuilder("name1_discard").startSpan();

        final Span child = tracer.spanBuilder("child").setParent(parent).startSpan();

        assertThat(child, instanceOf(PropagatedSpan.class));
        assertThat(child.isRecording(), is(false));
        assertThat(contextOf(child), sameInstance(contextOf(parent)));
        try (var ignored = child.storeInThreadContext()) {
            assertThat(currentSpanContext().getTraceId(), equalTo(spanContextOf(contextOf(parent)).getTraceId()));
            assertThat(currentSpanContext().isSampled(), is(false));
        }
        child.end();
        assertThat(finishedSpanNames(), empty());
    }

    public void testSpansUnderATraceThatIsNotSampledAreDropped() {
        final APMTracer tracer = tracer(enabledSettings(), 5);
        final Span root = tracer.spanBuilder("root").startSpan();
        assertThat(root.isRecording(), is(true));

        try (var ignored = root.storeInThreadContext()) {
            final Span discarded = tracer.spanBuilder("child_discard").startSpan();
            assertThat(discarded.isRecording(), is(false));

            try (var ignored2 = discarded.storeInThreadContext()) {
                final SpanContext discardedContext = currentSpanContext();
                assertThat(discardedContext.isSampled(), is(false));

                // the sampler would record this one, but it is part of a dropped trace
                final Span grandchild = tracer.spanBuilder("grandchild").startSpan();
                assertThat(grandchild.isRecording(), is(false));
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
        assertThat(entry.isRecording(), is(false));

        try (var ignored = entry.storeInThreadContext()) {
            assertThat(tracer.spanBuilder("child").startSpan().isRecording(), is(false));

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

    // ------------------------------------------------------------------------------------------------------------------------------
    // The local limits and the filters of the spans: the names, the depth and the number of the spans
    //
    // They apply to the spans that are started on the node, and to nothing else: what a span that they drop leaves behind is the context
    // of its parent, see the next section.
    // ------------------------------------------------------------------------------------------------------------------------------

    // the names of the spans

    public void testExcludedSpanNamesAreNotRecorded() {
        final APMTracer tracer = tracer(
            enabledSettings().putList(APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING.getKey(), List.of("name-a*"))
        );

        startAndEnd(tracer, "name-aaa");
        startAndEnd(tracer, "name-bbb");

        assertThat(finishedSpanNames(), containsInAnyOrder("name-bbb"));
    }

    public void testOnlyIncludedSpanNamesAreRecorded() {
        final APMTracer tracer = tracer(
            enabledSettings().putList(APMAgentSettings.TELEMETRY_TRACING_NAMES_INCLUDE_SETTING.getKey(), List.of("name-a*"))
        );

        startAndEnd(tracer, "name-aaa");
        startAndEnd(tracer, "name-bbb");

        assertThat(finishedSpanNames(), containsInAnyOrder("name-aaa"));
    }

    public void testSpanNameFiltersCanBeUpdated() {
        final APMTracer tracer = tracer(enabledSettings());

        updateSettings(Settings.builder().putList(APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING.getKey(), "name-a*"));
        startAndEnd(tracer, "name-aaa");
        startAndEnd(tracer, "name-bbb");
        assertThat(finishedSpanNames(), containsInAnyOrder("name-bbb"));

        updateSettings(
            Settings.builder()
                .putList(APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING.getKey(), List.of())
                .putList(APMAgentSettings.TELEMETRY_TRACING_NAMES_INCLUDE_SETTING.getKey(), "name-a*")
        );
        startAndEnd(tracer, "name-aaa");
        startAndEnd(tracer, "name-ccc");
        assertThat(finishedSpanNames(), containsInAnyOrder("name-bbb", "name-aaa"));
    }

    /** A span that is filtered out is skipped: its parent stays the current span, and so what is under it is under its parent. */
    public void testChildrenOfASpanThatIsFilteredOutAreStartedUnderItsParent() {
        final APMTracer tracer = tracer(
            enabledSettings().putList(APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING.getKey(), List.of("skipped*")),
            2
        );
        final Span root = tracer.spanBuilder("root").startSpan();

        try (var ignored = root.storeInThreadContext()) {
            final Span skipped = tracer.spanBuilder("skipped").startSpan();
            try (var ignored2 = skipped.storeInThreadContext()) {
                startAndEnd(tracer, "grandchild");
            }
        }
        root.end();

        assertThat(finishedSpanNames(), containsInAnyOrder("root", "grandchild"));
        assertThat(finishedSpan("grandchild").getParentSpanId(), equalTo(finishedSpan("root").getSpanId()));
    }

    // the depth of the spans

    public void testMaxDepthZeroRecordsOnlyTheRootSpan() {
        final APMTracer tracer = tracer(enabledSettings(), 0);

        final Span root = tracer.spanBuilder("root").startSpan();
        final Span child = tracer.spanBuilder("child").setParent(root).startSpan();
        assertThat(child.isRecording(), is(false));
        child.end();
        root.end();

        assertThat(finishedSpanNames(), containsInAnyOrder("root"));
    }

    public void testMaxDepthOneRecordsTheChildButNotTheGrandchild() {
        final APMTracer tracer = tracer(enabledSettings(), 1);

        final Span root = tracer.spanBuilder("root").startSpan();
        final Span child = tracer.spanBuilder("child").setParent(root).startSpan();
        final Span grandchild = tracer.spanBuilder("grandchild").setParent(child).startSpan();
        assertThat(grandchild.isRecording(), is(false));
        grandchild.end();
        child.end();
        root.end();

        assertThat(finishedSpanNames(), containsInAnyOrder("root", "child"));
    }

    /** A span that is dropped by the depth does not make what is under it any deeper, so all of that is dropped too. */
    public void testSpansUnderASpanThatIsDroppedByTheDepthAreDropped() {
        final APMTracer tracer = tracer(enabledSettings(), 1);
        final Span root = tracer.spanBuilder("root").startSpan();
        final Span child = tracer.spanBuilder("child").setParent(root).startSpan();
        final Span grandchild = tracer.spanBuilder("grandchild").setParent(child).startSpan();

        final Span greatGrandchild = tracer.spanBuilder("great-grandchild").setParent(grandchild).startSpan();

        assertThat(greatGrandchild.isRecording(), is(false));
        assertThat(contextOf(greatGrandchild), sameInstance(contextOf(child)));
    }

    /** The spans of the trace of another node are not the spans of this one: the depth is the one of the spans on this node. */
    public void testMaxDepthDoesNotCountTheSpansOfAnUpstreamTrace() {
        final APMTracer tracer = tracer(enabledSettings(), 0);
        threadContext.putHeader(Task.TRACE_PARENT_HTTP_HEADER, TRACE_PARENT);

        final Span entry = tracer.spanBuilder("entry").startSpan();
        assertThat(entry.isRecording(), is(true));
        assertThat(tracer.spanBuilder("child").setParent(entry).startSpan().isRecording(), is(false));
        entry.end();

        assertThat(finishedSpanNames(), containsInAnyOrder("entry"));
        assertThat(finishedSpan("entry").getParentSpanId(), equalTo(SPAN_ID));
    }

    // the number of the spans

    /** The local root is one of the spans that the limit is for. */
    public void testMaxSpansIncludesTheLocalRoot() {
        final APMTracer tracer = tracer(enabledSettings().put(OtelSdkSettings.TELEMETRY_TRACING_MAX_SPANS.getKey(), 2), 5);

        final Span root = tracer.spanBuilder("root").startSpan();
        for (String name : List.of("child-1", "child-2", "child-3")) {
            tracer.spanBuilder(name).setParent(root).startSpan().end();
        }
        root.end();

        assertThat(finishedSpanNames(), containsInAnyOrder("root", "child-1"));
    }

    /** The spans that are dropped for their names are not recorded, so they have no place in the count. */
    public void testSpansDroppedByTheNameFilterAreNotCounted() {
        final APMTracer tracer = tracer(
            enabledSettings().put(OtelSdkSettings.TELEMETRY_TRACING_MAX_SPANS.getKey(), 3)
                .putList(APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING.getKey(), "skipped*"),
            5
        );

        final Span root = tracer.spanBuilder("root").startSpan();
        for (String name : List.of("skipped-1", "skipped-2", "child-1", "skipped-3", "child-2", "child-3")) {
            tracer.spanBuilder(name).setParent(root).startSpan().end();
        }
        root.end();

        assertThat(finishedSpanNames(), containsInAnyOrder("root", "child-1", "child-2"));
    }

    /** The depth is checked before the number of the spans, so the spans that are too deep are not counted either. */
    public void testSpansDroppedByTheDepthAreNotCounted() {
        final APMTracer tracer = tracer(enabledSettings().put(OtelSdkSettings.TELEMETRY_TRACING_MAX_SPANS.getKey(), 3), 1);

        final Span root = tracer.spanBuilder("root").startSpan();
        final Span child = tracer.spanBuilder("child-1").setParent(root).startSpan();
        tracer.spanBuilder("grandchild-1").setParent(child).startSpan().end();
        tracer.spanBuilder("grandchild-2").setParent(child).startSpan().end();
        tracer.spanBuilder("child-2").setParent(root).startSpan().end();
        tracer.spanBuilder("child-3").setParent(root).startSpan().end();
        child.end();
        root.end();

        assertThat(finishedSpanNames(), containsInAnyOrder("root", "child-1", "child-2"));
    }

    public void testMaxSpansZeroDropsEvenTheLocalRoot() {
        final APMTracer tracer = tracer(enabledSettings().put(OtelSdkSettings.TELEMETRY_TRACING_MAX_SPANS.getKey(), 0), 5);

        final Span root = tracer.spanBuilder("root").startSpan();

        assertThat(root, instanceOf(PropagatedSpan.class));
        assertThat(root.isRecording(), is(false));
        root.end();
        assertThat(finishedSpanNames(), empty());
    }

    public void testLimitsCanBeUpdated() {
        final APMTracer tracer = tracer(enabledSettings(), 0);
        final Span root = tracer.spanBuilder("root").startSpan();
        assertThat(tracer.spanBuilder("child-1").setParent(root).startSpan().isRecording(), is(false));

        updateSettings(
            Settings.builder()
                .put(OtelSdkSettings.TELEMETRY_TRACING_MAX_DEPTH.getKey(), 1)
                .put(OtelSdkSettings.TELEMETRY_TRACING_MAX_SPANS.getKey(), 2)
        );

        tracer.spanBuilder("child-2").setParent(root).startSpan().end();
        assertThat(tracer.spanBuilder("child-3").setParent(root).startSpan().isRecording(), is(false));
        root.end();

        assertThat(finishedSpanNames(), containsInAnyOrder("root", "child-2"));
    }

    public void testMaxSpansIsEnforcedWhenSpansAreStartedConcurrently() {
        final int recordedChildren = between(1, 20);
        // the local root is one of the spans
        final var settings = enabledSettings().put(OtelSdkSettings.TELEMETRY_TRACING_MAX_SPANS.getKey(), recordedChildren + 1);
        final APMTracer tracer = tracer(settings, 1);
        final Span root = tracer.spanBuilder("root").startSpan();
        final int threads = recordedChildren + between(1, 20);

        startInParallel(threads, i -> tracer.spanBuilder("child-" + i).setParent(root).startSpan().end());
        root.end();

        assertThat(finishedSpanNames(), hasSize(recordedChildren + 1));
    }

    // ------------------------------------------------------------------------------------------------------------------------------
    // The spans that are dropped by the local limits, and the other nodes
    //
    // A span that is dropped by the local limits or by the filters is skipped, it is not the trace that is dropped: what it leaves behind
    // is the context of its parent, that is propagated to the other nodes as it is, sampled. What the span records is not the concern of
    // the other nodes, they have their own limits.
    // ------------------------------------------------------------------------------------------------------------------------------

    public void testSpanDroppedByTheNameFilterIsAPropagatedSpanOfItsParent() {
        final APMTracer tracer = tracer(
            enabledSettings().putList(APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING.getKey(), List.of("skipped*")),
            5
        );
        final Span root = tracer.spanBuilder("root").startSpan();

        final Span skipped = tracer.spanBuilder("skipped").setParent(root).startSpan();

        assertThatDroppedSpanIsTheParentWhenPropagated(tracer, root, skipped);
        root.end();
    }

    public void testSpanDroppedByTheDepthIsAPropagatedSpanOfItsParent() {
        final APMTracer tracer = tracer(enabledSettings(), 0);
        final Span root = tracer.spanBuilder("root").startSpan();

        final Span child = tracer.spanBuilder("child").setParent(root).startSpan();

        assertThatDroppedSpanIsTheParentWhenPropagated(tracer, root, child);
        root.end();
    }

    public void testSpanDroppedByTheNumberOfSpansIsAPropagatedSpanOfItsParent() {
        final APMTracer tracer = tracer(enabledSettings().put(OtelSdkSettings.TELEMETRY_TRACING_MAX_SPANS.getKey(), 1), 5);
        final Span root = tracer.spanBuilder("root").startSpan();

        final Span child = tracer.spanBuilder("child").setParent(root).startSpan();

        assertThatDroppedSpanIsTheParentWhenPropagated(tracer, root, child);
        root.end();
    }

    /**
     * The parent of a span that is dropped by the local limits is not changed by what the code that started the span does with it, as
     * that code does not know that the span was dropped: it records nothing, and does not end the parent.
     */
    public void testSpanDroppedByALocalLimitDoesNotChangeItsParent() {
        final APMTracer tracer = tracer(enabledSettings(), 0);
        final Span root = tracer.spanBuilder("root").setAttribute("set.by.the.owner", "yes").startSpan();
        final Span dropped = tracer.spanBuilder("child").setParent(root).startSpan();
        assertThat(dropped, instanceOf(PropagatedSpan.class));

        try (var ignored = dropped.storeInThreadContext()) {
            // what is current is the dropped span, so it is the one that the instrumentation sets what it knows on
            tracer.currentSpan()
                .setAttribute("set.by.the.child", "no")
                .setAttribute("set.by.the.child.too", 1L)
                .recordException(new IllegalStateException("boom"));
            ((AbstractSpan) tracer.currentSpan()).setAllAttributes(Attributes.of(stringKey("set.by.the.child.as.well"), "no"));
            ((AbstractSpan) tracer.currentSpan()).setStatus(StatusCode.ERROR, "failed");
        }
        dropped.setAttribute("set.by.the.child.at.the.end", "no");
        dropped.end();

        // the parent is not ended by the end of the span that was dropped
        assertThat(root.isRecording(), is(true));
        assertThat(finishedSpanNames(), empty());
        root.end();

        final SpanData rootData = finishedSpan("root");
        assertThat(rootData.getAttributes().get(stringKey("set.by.the.owner")), equalTo("yes"));
        assertThat(rootData.getAttributes().get(stringKey("set.by.the.child")), nullValue());
        assertThat(rootData.getAttributes().get(AttributeKey.longKey("set.by.the.child.too")), nullValue());
        assertThat(rootData.getAttributes().get(stringKey("set.by.the.child.as.well")), nullValue());
        assertThat(rootData.getAttributes().get(stringKey("set.by.the.child.at.the.end")), nullValue());
        assertThat(rootData.getEvents(), empty());
        assertThat(rootData.getStatus().getStatusCode(), is(StatusCode.UNSET));
    }

    // a node does not drop the trace for the others

    public void testSpansDroppedByTheLimitsOfOneNodeDoNotDropTheTraceOnAnotherNode() {
        final ThreadContext remoteThreadContext = new ThreadContext(Settings.EMPTY);
        final APMTracer node = tracer(threadContext, enabledSettings(), 0, false, NAME_SAMPLER);
        final APMTracer remoteNode = tracer(remoteThreadContext, enabledSettings(), 0, false, NAME_SAMPLER);

        final Span root = node.spanBuilder("root").startSpan();
        // the only span that this node records is the root, the child is dropped by the depth
        final Span child = node.spanBuilder("child").setParent(root).startSpan();
        assertThat(child.isRecording(), is(false));
        try (var ignored = child.storeInThreadContext()) {
            sendTraceHeadersTo(remoteThreadContext);
        }

        // the one span that the other node is allowed to record is not counted with the one of this node
        final Span entry = remoteNode.spanBuilder("entry").startSpan();
        assertThat(entry.isRecording(), is(true));
        assertThat(remoteNode.spanBuilder("entry-child").setParent(entry).startSpan().isRecording(), is(false));
        entry.end();
        root.end();

        assertThat(finishedSpanNames(), containsInAnyOrder("root", "entry"));
        assertThat(finishedSpan("entry").getTraceId(), equalTo(finishedSpan("root").getTraceId()));
        assertThat(finishedSpan("entry").getParentSpanId(), equalTo(finishedSpan("root").getSpanId()));
        assertThat(finishedSpan("entry").getParentSpanContext().isRemote(), is(true));
    }

    public void testSpanThatIsNotSampledDropsTheTraceOnAnotherNode() {
        final ThreadContext remoteThreadContext = new ThreadContext(Settings.EMPTY);
        final APMTracer node = tracer(threadContext, enabledSettings(), 5, false, NAME_SAMPLER);
        final APMTracer remoteNode = tracer(remoteThreadContext, enabledSettings(), 5, false, NAME_SAMPLER);

        final Span root = node.spanBuilder("root_discard").startSpan();
        try (var ignored = root.storeInThreadContext()) {
            sendTraceHeadersTo(remoteThreadContext);
        }
        root.end();

        // what makes the difference with the spans that are dropped by the limits is the decision, that is the one of the trace
        assertThat(remoteNode.spanBuilder("entry").startSpan().isRecording(), is(false));
        assertThat(finishedSpanNames(), empty());
    }

    // the span that is dropped is the local root

    /** There is no context of a parent to leave behind: nothing is propagated, and the spans that are started next are not under it. */
    public void testLocalRootDroppedByTheNameFilterLeavesNothingToPropagate() {
        final APMTracer tracer = tracer(
            enabledSettings().putList(APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING.getKey(), List.of("skipped*")),
            5
        );

        final Span skipped = tracer.spanBuilder("skipped").startSpan();
        assertThat(skipped, instanceOf(PropagatedSpan.class));
        assertThat(skipped.isRecording(), is(false));
        try (var ignored = skipped.storeInThreadContext()) {
            assertThat(tracer.currentSpan(), sameInstance(skipped));
            assertThat(currentSpanContext().isValid(), is(false));
            assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), nullValue());
            assertThat(threadContext.getHeader(Task.TRACE_STATE), nullValue());

            // what is started under it is the beginning of a trace
            startAndEnd(tracer, "child");
        }

        assertThat(finishedSpanNames(), containsInAnyOrder("child"));
        assertThat(finishedSpan("child").getParentSpanContext().isValid(), is(false));
    }

    public void testLocalRootDroppedByTheNameFilterKeepsTheRemoteParent() {
        final APMTracer tracer = tracer(
            enabledSettings().putList(APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING.getKey(), List.of("skipped*")),
            5
        );
        threadContext.putHeader(Task.TRACE_PARENT_HTTP_HEADER, TRACE_PARENT);
        threadContext.putHeader(Task.TRACE_STATE, "vendor=value");

        final Span skipped = tracer.spanBuilder("skipped").startSpan();
        assertThat(skipped.isRecording(), is(false));
        try (var ignored = skipped.storeInThreadContext()) {
            // the trace of the other node goes on as it came
            assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), equalTo(TRACE_PARENT));
            assertThat(threadContext.getHeader(Task.TRACE_STATE), equalTo("vendor=value"));

            startAndEnd(tracer, "child");
        }

        assertThat(finishedSpanNames(), containsInAnyOrder("child"));
        assertThat(finishedSpan("child").getTraceId(), equalTo(TRACE_ID));
        assertThat(finishedSpan("child").getParentSpanId(), equalTo(SPAN_ID));
    }

    // ------------------------------------------------------------------------------------------------------------------------------
    // Enabling and disabling tracing
    //
    // A span that is started while tracing is disabled is not recorded, and nothing of it is propagated to the other nodes: what changes
    // is what is started from then on, a span that was started before goes on as it is.
    // ------------------------------------------------------------------------------------------------------------------------------

    public void testTracingCanBeEnabledAfterStartingDisabled() {
        final APMTracer tracer = tracer(disabledSettings());
        assertThat(tracer.spanBuilder("name1").startSpan().isRecording(), is(false));
        assertThat(finishedSpanNames(), empty());

        updateSettings(Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true));

        startAndEnd(tracer, "name2");
        assertThat(finishedSpanNames(), containsInAnyOrder("name2"));
    }

    public void testTracingResumesAfterDisableAndReEnable() {
        final APMTracer tracer = tracer(enabledSettings());

        startAndEnd(tracer, "resume-test-1");
        assertThat(finishedSpanNames(), containsInAnyOrder("resume-test-1"));

        updateSettings(Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), false));
        exporter.reset();
        final Span disabled = tracer.spanBuilder("resume-test-2").startSpan();
        assertThat(disabled.isRecording(), is(false));
        disabled.end();
        assertThat(finishedSpanNames(), empty());

        updateSettings(Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true));
        startAndEnd(tracer, "resume-test-3");
        assertThat(finishedSpanNames(), containsInAnyOrder("resume-test-3"));
    }

    public void testSpanStartedWhileTracingIsDisabledIsNotRecordedAndNotPropagated() {
        final APMTracer tracer = tracer(disabledSettings());
        threadContext.putHeader(Task.TRACE_PARENT_HTTP_HEADER, TRACE_PARENT);
        threadContext.putHeader(Task.TRACE_STATE, "vendor=value");

        final Span span = tracer.spanBuilder("name1").startSpan();
        assertThat(span, instanceOf(PropagatedSpan.class));
        assertThat(span.isRecording(), is(false));
        try (var ignored = span.storeInThreadContext()) {
            assertThat(tracer.currentSpan(), sameInstance(span));
            assertThat(tracer.currentSpan().isRecording(), is(false));
            assertNoTraceHeaders();
        }
        span.end();

        assertThat(finishedSpanNames(), empty());
        // restored
        assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), equalTo(TRACE_PARENT));
    }

    public void testSpanStartedUnderASpanWhileTracingIsDisabledIsNotRecordedAndNotPropagated() {
        final APMTracer tracer = tracer(enabledSettings());
        final Span root = tracer.spanBuilder("root").startSpan();

        try (var ignored = root.storeInThreadContext()) {
            assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), notNullValue());
            updateSettings(Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), false));

            // not even the spans of the current trace are recorded
            final Span span = tracer.spanBuilder("name1").setParent(root).startSpan();
            assertThat(span.isRecording(), is(false));
            try (var ignored2 = span.storeInThreadContext()) {
                assertNoTraceHeaders();
                // it is still the trace of the span that was current when tracing was disabled
                assertThat(currentSpanContext().getTraceId(), equalTo(spanContextOf(contextOf(root)).getTraceId()));
            }
            // and what the span of the trace had in the thread context is back
            assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), notNullValue());
        }
        root.end();

        assertThat(finishedSpanNames(), not(hasItem("name1")));
    }

    public void testSpanThatWasStartedBeforeTracingWasDisabledGoesOnRecording() {
        final APMTracer tracer = tracer(enabledSettings(), 1);
        final Span root = tracer.spanBuilder("root").startSpan();

        try (var ignored = root.storeInThreadContext()) {
            updateSettings(Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), false));

            assertThat(tracer.currentSpan(), sameInstance(root));
            assertThat(tracer.currentSpan().isRecording(), is(true));
            tracer.currentSpan().setAttribute("set.after.the.change", "yes");
        }
        root.end();

        assertThat(finishedSpan("root").getAttributes().get(stringKey("set.after.the.change")), equalTo("yes"));
    }

    public void testSpanThatIsStoredWhileTracingIsDisabledIsNotPropagatedEvenIfItRecords() {
        final APMTracer tracer = tracer(enabledSettings());
        final Span root = tracer.spanBuilder("root").startSpan();
        updateSettings(Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), false));

        try (var ignored = root.storeInThreadContext()) {
            assertThat(tracer.currentSpan(), sameInstance(root));
            assertNoTraceHeaders();
        }
        root.end();

        // the span that was started while tracing is enabled is still exported
        assertThat(finishedSpanNames(), hasItem("root"));
    }

    /** Only what is started after the change is affected: the span that was started while disabled has no trace to continue. */
    public void testSpanUnderASpanThatWasStartedWhileTracingWasDisabledStartsANewTraceOnceItIsEnabled() {
        final APMTracer tracer = tracer(disabledSettings());
        final Span span = tracer.spanBuilder("name1").startSpan();
        assertThat(span.isRecording(), is(false));

        updateSettings(Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true));
        final Span child = tracer.spanBuilder("child").setParent(span).startSpan();
        assertThat(child.isRecording(), is(true));
        child.end();
        span.end();

        assertThat(finishedSpanNames(), containsInAnyOrder("child"));
        assertThat(finishedSpan("child").getParentSpanContext().isValid(), is(false));
    }

    public void testSpanUnderASpanThatWasStartedWhileTracingWasDisabledContinuesTheTraceOfItsParentOnceItIsEnabled() {
        final APMTracer tracer = tracer(enabledSettings(), 2);
        final Span root = tracer.spanBuilder("root").startSpan();
        updateSettings(Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), false));
        final Span disabled = tracer.spanBuilder("child").setParent(root).startSpan();
        assertThat(disabled.isRecording(), is(false));

        updateSettings(Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true));
        tracer.spanBuilder("grandchild").setParent(disabled).startSpan().end();
        root.end();

        assertThat(finishedSpanNames(), containsInAnyOrder("root", "grandchild"));
        assertThat(finishedSpan("grandchild").getParentSpanId(), equalTo(finishedSpan("root").getSpanId()));
    }

    public void testSpanThatIsNotRecordedCanBeUsedLikeAnyOtherSpan() {
        final APMTracer tracer = tracer(disabledSettings());

        final Span span = tracer.spanBuilder("name1").setAttribute("a", "b").startSpan();
        span.setAttribute("a", true).setAttribute("a", 1.0d).setAttribute("a", 1L).setAttribute("a", "b").recordException(new Exception());
        span.end();
        span.end();

        assertThat(finishedSpanNames(), empty());
    }

    public void testNoParentOnADisabledTracerDoesNotRecord() {
        final APMTracer tracer = tracer(enabledSettings());
        updateSettings(disabledSettings());

        assertThat(tracer.spanBuilder("name1").setNoParent().startSpan().isRecording(), is(false));
    }

    /** The SDK has threads and connections, and none of that is needed for what is never recorded. */
    public void testSdkIsNotCreatedWhileTracingIsDisabled() {
        final APMTracer tracer = tracer(threadContext, disabledSettings(), 5, false, NAME_SAMPLER);
        final Span parent = tracer.spanBuilder("parent").startSpan();

        // without a parent, with a parent that is given, with a current span and with a remote parent
        tracer.spanBuilder("name1").startSpan().end();
        tracer.spanBuilder("name2").setParent(parent).startSpan().end();
        tracer.spanBuilder("name3").setNoParent().startSpan().end();
        try (var ignored = parent.storeInThreadContext()) {
            tracer.spanBuilder("name4").startSpan().end();
        }
        threadContext.putHeader(Task.TRACE_PARENT_HTTP_HEADER, TRACE_PARENT);
        tracer.spanBuilder("name5").startSpan().end();

        assertThat(sdks.size(), equalTo(0));

        updateSettings(Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true));
        startAndEnd(tracer, "name6");
        assertThat(sdks.size(), equalTo(1));
    }

    // ------------------------------------------------------------------------------------------------------------------------------
    // The start time of a trace
    //
    // The time at which the request arrived is the start time of the local root span of the request, which is the first span that is
    // started: the one that either has a remote parent, or has none. It is used once, and it is removed when it is.
    // ------------------------------------------------------------------------------------------------------------------------------

    public void testLocalRootSpanUsesTheTraceStartTime() {
        final APMTracer tracer = tracer(enabledSettings());
        final Instant startTime = Instant.ofEpochMilli(randomLongBetween(0, 4_000_000_000_000L));
        threadContext.putTransient(Task.LOCAL_TRACE_START_TIME, startTime);

        startAndEnd(tracer, "name1");

        assertThat(finishedSpan("name1").getStartEpochNanos(), equalTo(epochNanos(startTime)));
    }

    public void testLocalRootSpanWithARemoteParentUsesTheTraceStartTime() {
        final APMTracer tracer = tracer(enabledSettings());
        final Instant startTime = Instant.ofEpochMilli(randomLongBetween(0, 4_000_000_000_000L));
        threadContext.putHeader(Task.TRACE_PARENT_HTTP_HEADER, TRACE_PARENT);
        threadContext.putTransient(Task.LOCAL_TRACE_START_TIME, startTime);

        startAndEnd(tracer, "name1");

        assertThat(finishedSpan("name1").getStartEpochNanos(), equalTo(epochNanos(startTime)));
        assertThat(finishedSpan("name1").getParentSpanId(), equalTo(SPAN_ID));
        assertThat(threadContext.getTransient(Task.LOCAL_TRACE_START_TIME), nullValue());
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
        assertThat(finishedSpan("child").getStartEpochNanos(), not(equalTo(epochNanos(startTime))));
    }

    /** It is the start of the local root, that is used up by it whether the SDK records it or not, and it is not something to propagate. */
    public void testTraceStartTimeIsUsedUpByALocalRootThatIsNotSampled() {
        final APMTracer tracer = tracer(enabledSettings());
        threadContext.putTransient(Task.LOCAL_TRACE_START_TIME, Instant.now());

        final Span root = tracer.spanBuilder("root_discard").startSpan();

        assertThat(root.isRecording(), is(false));
        assertThat(threadContext.getTransient(Task.LOCAL_TRACE_START_TIME), nullValue());
        try (var ignored = root.storeInThreadContext()) {
            assertThat(threadContext.getTransient(Task.LOCAL_TRACE_START_TIME), nullValue());
            assertThat(threadContext.getHeaders().keySet(), not(hasItem(Task.LOCAL_TRACE_START_TIME)));
        }
    }

    /** A root that is dropped by the filters never gets as far as the start: the one that is started next is the local root. */
    public void testTraceStartTimeIsKeptWhenTheLocalRootIsDroppedByTheNameFilter() {
        final APMTracer tracer = tracer(
            enabledSettings().putList(APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING.getKey(), List.of("skipped*"))
        );
        final Instant startTime = Instant.ofEpochMilli(randomLongBetween(0, 4_000_000_000_000L));
        threadContext.putTransient(Task.LOCAL_TRACE_START_TIME, startTime);

        tracer.spanBuilder("skipped").startSpan().end();
        assertThat(threadContext.getTransient(Task.LOCAL_TRACE_START_TIME), equalTo(startTime));

        startAndEnd(tracer, "name1");
        assertThat(finishedSpan("name1").getStartEpochNanos(), equalTo(epochNanos(startTime)));
        assertThat(threadContext.getTransient(Task.LOCAL_TRACE_START_TIME), nullValue());
    }

    public void testTraceStartTimeIsKeptWhileTracingIsDisabled() {
        final APMTracer tracer = tracer(disabledSettings());
        final Instant startTime = Instant.ofEpochMilli(randomLongBetween(0, 4_000_000_000_000L));
        threadContext.putTransient(Task.LOCAL_TRACE_START_TIME, startTime);

        tracer.spanBuilder("name1").startSpan().end();

        assertThat(threadContext.getTransient(Task.LOCAL_TRACE_START_TIME), equalTo(startTime));
    }

    // ------------------------------------------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------------------------------------------

    /** Changes the settings of the tracer that was created last, the way an update of the cluster settings does. */
    private void updateSettings(Settings.Builder changes) {
        settingsUpdater.update(changes.build());
    }

    private static Settings.Builder enabledSettings() {
        return Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true);
    }

    private static Settings.Builder disabledSettings() {
        return Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), false);
    }

    private APMTracer tracer(Settings.Builder settings) {
        return tracer(settings, 0);
    }

    private APMTracer tracer(Settings.Builder settings, int maxTraceDepth) {
        return tracer(settings, maxTraceDepth, false);
    }

    private APMTracer tracer(Settings.Builder settings, int maxTraceDepth, boolean recordExceptionStacks) {
        return tracer(settings, maxTraceDepth, recordExceptionStacks, NAME_SAMPLER);
    }

    private APMTracer tracer(Settings.Builder settings, int maxTraceDepth, boolean recordExceptionStacks, Sampler sampler) {
        return tracer(threadContext, settings, maxTraceDepth, recordExceptionStacks, sampler);
    }

    /** A tracer, of a node that has the given thread context, that exports to {@link #exporter}, and samples with the given sampler. */
    private APMTracer tracer(
        ThreadContext threadContext,
        Settings.Builder settings,
        int maxTraceDepth,
        boolean recordExceptionStacks,
        Sampler sampler
    ) {
        final Settings allSampled = settings.put(OtelSdkSettings.TELEMETRY_TRACING_SAMPLE_RATE.getKey(), 1.0)
            .put(OtelSdkSettings.TELEMETRY_TRACING_MAX_DEPTH.getKey(), maxTraceDepth)
            .put(OtelSdkSettings.TELEMETRY_TRACING_RECORD_EXCEPTION_STACKS.getKey(), recordExceptionStacks)
            .build();
        final APMTracingService service = new APMTracingService(threadContext, allSampled, () -> {
            final var sdk = SdkTracerProvider.builder().setSampler(sampler).addSpanProcessor(SimpleSpanProcessor.create(exporter)).build();
            sdks.add(sdk);
            return (TracerProvider) sdk;
        });
        settingsUpdater = new TracingSettingsUpdater(service, allSampled);
        service.start();
        return service.tracer();
    }

    private static void startAndEnd(Tracer tracer, String name) {
        tracer.spanBuilder(name).startSpan().end();
    }

    /**
     * What a span that was dropped by a local limit gives to the code that started it, and to the other nodes: it is not a span that
     * records, but when it is made current, the context of the parent is what is current, and propagated, sampled.
     */
    private void assertThatDroppedSpanIsTheParentWhenPropagated(APMTracer tracer, Span parent, Span dropped) {
        assertThat(dropped, instanceOf(PropagatedSpan.class));
        assertThat(dropped.isRecording(), is(false));
        assertThat(contextOf(dropped), sameInstance(contextOf(parent)));

        final SpanContext parentContext = spanContextOf(contextOf(parent));
        assertThat(parentContext.isSampled(), is(true));
        try (var ignored = dropped.storeInThreadContext()) {
            assertThat(tracer.currentSpan(), sameInstance(dropped));
            assertThat(currentSpanContext(), equalTo(parentContext));
            assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), equalTo(traceParent(parentContext)));
        }
        assertThat(threadContext.getSpan(), sameInstance(Span.noop()));
        assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), nullValue());
    }

    private void sendTraceHeadersTo(ThreadContext remoteThreadContext) {
        try (var out = new BytesStreamOutput()) {
            threadContext.writeTo(out);
            remoteThreadContext.readHeaders(new ByteArrayStreamInput(out.bytes().array()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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

    private void assertNoTraceHeaders() {
        assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), nullValue());
        assertThat(threadContext.getHeader(Task.TRACE_STATE), nullValue());
    }

    private SpanContext currentSpanContext() {
        return spanContextOf(contextOf(threadContext.getSpan()));
    }

    private static Context contextOf(Span span) {
        assertThat(span, instanceOf(AbstractSpan.class));
        return ((AbstractSpan) span).context();
    }

    private static SpanContext spanContextOf(Context context) {
        return io.opentelemetry.api.trace.Span.fromContext(context).getSpanContext();
    }

    private static String traceParent(SpanContext spanContext) {
        return "00-" + spanContext.getTraceId() + "-" + spanContext.getSpanId() + "-" + spanContext.getTraceFlags().asHex();
    }
}
