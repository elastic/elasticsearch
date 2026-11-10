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
import io.opentelemetry.sdk.trace.samplers.SamplingResult;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings;
import org.elasticsearch.telemetry.tracing.AttributeKeys;
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
import java.util.stream.Stream;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
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
        new APMTracer(threadContext, Settings.EMPTY, MeterProvider::noop);
    }

    public void testTracingCanBeEnabledAfterStartingDisabled() {
        final APMTracer tracer = tracer(disabledSettings());
        assertThat(isRecorded(tracer.spanBuilder("name1").startSpan()), is(false));
        assertThat(finishedSpanNames(), empty());

        tracer.setEnabled(true);

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

        tracer.setEnabled(false);
        exporter.reset();
        final Span disabled = tracer.spanBuilder("resume-test-2").startSpan();
        assertThat(isRecorded(disabled), is(false));
        disabled.end();
        assertThat(finishedSpanNames(), empty());

        tracer.setEnabled(true);
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
        assertThat(data.getAttributes().get(stringKey(AttributeKeys.NODE_NAME)), equalTo("test-node"));
        assertThat(data.getAttributes().get(stringKey(AttributeKeys.CLUSTER_NAME)), equalTo("test-cluster"));
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

    public void testNoopSpanAsTheParentStartsALocalRoot() {
        final APMTracer tracer = tracer(enabledSettings(), 0);
        final Span root = tracer.spanBuilder("root").startSpan();

        try (var ignored = root.storeInThreadContext()) {
            // no remote parent either, as the header describes the current span
            threadContext.removeHeaders(Task.TRACE_PARENT_HTTP_HEADER, Task.TRACE_STATE);

            // a root, so it is at depth 0 and is not dropped by the limit
            final Span span = tracer.spanBuilder("local-root").setParent(Span.noop()).startSpan();
            assertThat(isRecorded(span), is(true));
            span.end();
        }
        root.end();

        assertThat(finishedSpan("local-root").getParentSpanContext().isValid(), is(false));
        assertThat(finishedSpan("local-root").getTraceId(), not(equalTo(finishedSpan("root").getTraceId())));
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
            tracer.setEnabled(false);

            final Span span = tracer.spanBuilder("name1").startSpan();
            assertThat(isRecorded(span), is(false));
            try (var ignored2 = span.storeInThreadContext()) {
                assertLocalTraceDropped();
            }
        }
    }

    public void testSpanWithAFilteredOutNameDropsTheLocalTrace() {
        final Settings settings = enabledSettingsBuilder().putList(
            APMAgentSettings.TELEMETRY_TRACING_NAMES_INCLUDE_SETTING.getKey(),
            List.of("filtered*")
        ).build();
        final APMTracer tracer = tracer(settings);
        threadContext.putHeader(Task.TRACE_PARENT_HTTP_HEADER, TRACE_PARENT);

        final Span span = tracer.spanBuilder("name1").startSpan();
        assertThat(isRecorded(span), is(false));
        try (var ignored = span.storeInThreadContext()) {
            assertLocalTraceDropped();
        }

        assertThat(finishedSpanNames(), empty());
    }

    /**
     * A span that is filtered out is not recorded, and neither is anything that is started under it, but its siblings are not
     * affected. It also stops the trace from being propagated from where it is stored, since there is no span to continue it.
     */
    public void testFilteredOutChildSpanDropsItsSubtree() {
        final Settings settings = enabledSettingsBuilder().putList(
            APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING.getKey(),
            List.of("excluded*")
        ).build();
        final APMTracer tracer = tracer(settings, 2);
        final Span root = tracer.spanBuilder("root").startSpan();

        try (var ignored = root.storeInThreadContext()) {
            final Span excluded = tracer.spanBuilder("excluded-child").startSpan();
            assertThat(isRecorded(excluded), is(false));
            try (var ignored2 = excluded.storeInThreadContext()) {
                assertThat(spanContextOf(threadContext.getTransient(Task.APM_TRACE_CONTEXT)).isValid(), is(false));
                assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), nullValue());

                // part of the dropped subtree
                assertThat(isRecorded(tracer.spanBuilder("included-grandchild").startSpan()), is(false));
            }
            // the context of the parent is back, and so is the sibling's chance
            startAndEnd(tracer, "included-child");
        }
        root.end();

        assertThat(finishedSpanNames(), containsInAnyOrder("root", "included-child"));
        assertThat(finishedSpan("included-child").getParentSpanId(), equalTo(finishedSpan("root").getSpanId()));
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
                    assertThat(currentSpanContext(), equalTo(discardedContext));
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

    public void testIncludedSpanNamesAreRecorded() {
        final Settings settings = enabledSettingsBuilder().putList(
            APMAgentSettings.TELEMETRY_TRACING_NAMES_INCLUDE_SETTING.getKey(),
            List.of(
                // exact name
                "name-aaa",
                // regex
                "name-b*"
            )
        ).build();
        final APMTracer tracer = tracer(settings);

        for (String name : List.of("name-aaa", "name-bbb", "name-ccc")) {
            startAndEnd(tracer, name);
        }

        assertThat(finishedSpanNames(), containsInAnyOrder("name-aaa", "name-bbb"));
    }

    public void testExcludedSpanNamesAreNotRecorded() {
        final Settings settings = enabledSettingsBuilder().putList(
            APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING.getKey(),
            List.of(
                // exact name
                "name-aaa",
                // regex
                "name-b*"
            )
        ).build();
        final APMTracer tracer = tracer(settings);

        for (String name : List.of("name-aaa", "name-bbb", "name-ccc")) {
            startAndEnd(tracer, name);
        }

        assertThat(finishedSpanNames(), containsInAnyOrder("name-ccc"));
    }

    public void testExcludeTakesPrecedenceOverInclude() {
        final Settings settings = enabledSettingsBuilder().putList(
            APMAgentSettings.TELEMETRY_TRACING_NAMES_INCLUDE_SETTING.getKey(),
            List.of("name-a*")
        ).putList(APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING.getKey(), List.of("name-a*")).build();
        final APMTracer tracer = tracer(settings);

        startAndEnd(tracer, "name-aaa");

        assertThat(finishedSpanNames(), empty());
    }

    public void testSpanNameFiltersCanBeUpdated() {
        final APMTracer tracer = tracer(enabledSettings());

        tracer.setExcludeNames(List.of("name-a*"));
        startAndEnd(tracer, "name-aaa");
        startAndEnd(tracer, "name-bbb");
        assertThat(finishedSpanNames(), containsInAnyOrder("name-bbb"));

        tracer.setExcludeNames(List.of());
        tracer.setIncludeNames(List.of("name-a*"));
        startAndEnd(tracer, "name-aaa");
        startAndEnd(tracer, "name-ccc");
        assertThat(finishedSpanNames(), containsInAnyOrder("name-bbb", "name-aaa"));
    }

    public void testMaxDepthZeroRecordsOnlyTheRootSpan() {
        final APMTracer tracer = tracer(enabledSettings(), 0);

        final Span root = tracer.spanBuilder("root").startSpan();
        final Span child = tracer.spanBuilder("child").setParent(root).startSpan();
        assertThat(isRecorded(child), is(false));
        child.end();
        root.end();

        assertThat(finishedSpanNames(), containsInAnyOrder("root"));
    }

    public void testSpanDroppedByALimitDropsItsSubtree() {
        final APMTracer tracer = tracer(enabledSettings(), 0);
        final Span root = tracer.spanBuilder("root").startSpan();

        try (var ignored = root.storeInThreadContext()) {
            final SpanContext rootContext = currentSpanContext();

            final Span child = tracer.spanBuilder("child").startSpan();
            assertThat(isRecorded(child), is(false));
            try (var ignored2 = child.storeInThreadContext()) {
                assertThat(spanContextOf(threadContext.getTransient(Task.APM_TRACE_CONTEXT)).isValid(), is(false));
                assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), nullValue());
            }

            // back to the root span
            assertThat(currentSpanContext(), equalTo(rootContext));
            assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), equalTo(traceParent(rootContext)));
        }
        root.end();

        assertThat(finishedSpanNames(), containsInAnyOrder("root"));
    }

    public void testMaxDepthOneRecordsTheChildButNotTheGrandchild() {
        final APMTracer tracer = tracer(enabledSettings(), 1);

        final Span root = tracer.spanBuilder("root").startSpan();
        final Span child = tracer.spanBuilder("child").setParent(root).startSpan();
        final Span grandchild = tracer.spanBuilder("grandchild").setParent(child).startSpan();
        assertThat(isRecorded(grandchild), is(false));
        grandchild.end();
        child.end();
        root.end();

        assertThat(finishedSpanNames(), containsInAnyOrder("root", "child"));
        assertThat(finishedSpan("child").getParentSpanId(), equalTo(finishedSpan("root").getSpanId()));
    }

    public void testMaxDepthAppliesToStoredParentsToo() {
        final APMTracer tracer = tracer(enabledSettings(), 1);
        final Span root = tracer.spanBuilder("root").startSpan();

        try (var ignored = root.storeInThreadContext()) {
            final Span child = tracer.spanBuilder("child").startSpan();
            try (var ignored2 = child.storeInThreadContext()) {
                assertThat(isRecorded(tracer.spanBuilder("grandchild").startSpan()), is(false));
            }
            child.end();
        }
        root.end();

        assertThat(finishedSpanNames(), containsInAnyOrder("root", "child"));
    }

    public void testMaxSpansTwoDropsTheThirdChild() {
        final Settings settings = enabledSettingsBuilder().put(OtelSdkSettings.TELEMETRY_TRACING_MAX_SPANS.getKey(), 2).build();
        final APMTracer tracer = tracer(settings, 5);

        final Span root = tracer.spanBuilder("root").startSpan();
        for (String name : List.of("child-1", "child-2", "child-3")) {
            tracer.spanBuilder(name).setParent(root).startSpan().end();
        }
        root.end();

        assertThat(finishedSpanNames(), containsInAnyOrder("root", "child-1", "child-2"));
    }

    public void testMaxSpansIsSharedByTheWholeLocalTrace() {
        final Settings settings = enabledSettingsBuilder().put(OtelSdkSettings.TELEMETRY_TRACING_MAX_SPANS.getKey(), 2).build();
        final APMTracer tracer = tracer(settings, 5);

        final Span root = tracer.spanBuilder("root").startSpan();
        final Span child = tracer.spanBuilder("child").setParent(root).startSpan();
        tracer.spanBuilder("grandchild").setParent(child).startSpan().end();
        child.end();
        tracer.spanBuilder("second-child").setParent(root).startSpan().end();
        root.end();

        assertThat(finishedSpanNames(), containsInAnyOrder("root", "child", "grandchild"));
    }

    public void testMaxDepthZeroRecordsTheEntrySpanOfARemoteTraceButNotItsLocalChildren() {
        final APMTracer tracer = tracer(enabledSettings(), 0);
        threadContext.putHeader(Task.TRACE_PARENT_HTTP_HEADER, TRACE_PARENT);

        final Span entry = tracer.spanBuilder("entry-span").startSpan();
        assertThat(isRecorded(tracer.spanBuilder("local-child").setParent(entry).startSpan()), is(false));
        entry.end();

        assertThat(finishedSpanNames(), containsInAnyOrder("entry-span"));
        assertThat(finishedSpan("entry-span").getParentSpanId(), equalTo(SPAN_ID));
    }

    public void testLimitsCanBeUpdated() {
        final APMTracer tracer = tracer(enabledSettings(), 0);
        final Span root = tracer.spanBuilder("root").startSpan();
        assertThat(isRecorded(tracer.spanBuilder("child-1").setParent(root).startSpan()), is(false));

        tracer.setMaxTraceDepth(1);
        tracer.setMaxSpans(1);

        tracer.spanBuilder("child-2").setParent(root).startSpan().end();
        assertThat(isRecorded(tracer.spanBuilder("child-3").setParent(root).startSpan()), is(false));
        root.end();

        assertThat(finishedSpanNames(), containsInAnyOrder("root", "child-2"));
    }

    public void testLocalRootSpanUsesTheTraceStartTime() {
        final APMTracer tracer = tracer(enabledSettings());
        final Instant startTime = Instant.ofEpochMilli(randomLongBetween(0, 4_000_000_000_000L));
        threadContext.putTransient(Task.LOCAL_TRACE_START_TIME, startTime);

        startAndEnd(tracer, "name1");

        assertThat(finishedSpan("name1").getStartEpochNanos(), equalTo(epochNanos(startTime)));
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

    public void testSensitiveAttributeValuesAreRedacted() {
        final APMTracer tracer = tracer(enabledSettings());

        tracer.spanBuilder("name1")
            .setAttribute("password", "hunter2")
            .setAttribute("http.request.headers.authorization", "Basic abc")
            .setAttribute("session_id", 1234L)
            .setAttribute("credit-card", 1.5d)
            .setAttribute("my-token-flag", true)
            .setAttribute("not.sensitive", "visible")
            .startSpan()
            .end();

        final Attributes attributes = finishedSpan("name1").getAttributes();
        for (String key : List.of("password", "http.request.headers.authorization", "session_id", "credit-card", "my-token-flag")) {
            assertThat(key, attributes.get(stringKey(key)), equalTo("[REDACTED]"));
        }
        assertThat(attributes.get(stringKey("not.sensitive")), equalTo("visible"));
    }

    public void testRedactedKeysCanBeUpdated() {
        final APMTracer tracer = tracer(enabledSettings());
        tracer.setLabelFilters(List.of("custom*"));

        tracer.spanBuilder("name1").setAttribute("custom-field", "a").setAttribute("password", "hunter2").startSpan().end();

        final Attributes attributes = finishedSpan("name1").getAttributes();
        assertThat(attributes.get(stringKey("custom-field")), equalTo("[REDACTED]"));
        assertThat(attributes.get(stringKey("password")), equalTo("hunter2"));
    }

    public void testDefaultSensitiveKeysAreMatchedByTheLabelFilter() {
        final var labelFilterAutomaton = tracer(disabledSettings()).getLabelFilterAutomaton();

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

    public void testSpanKindIsInternalUnlessSet() {
        final APMTracer tracer = tracer(enabledSettings());

        tracer.spanBuilder("server-span").setSpanKind(SpanKind.SERVER).startSpan().end();
        // the attributes do not make the kind
        tracer.spanBuilder("internal-span").setAttribute("http.method", "GET").startSpan().end();

        assertThat(finishedSpan("server-span").getKind(), is(SpanKind.SERVER));
        assertThat(finishedSpan("internal-span").getKind(), is(SpanKind.INTERNAL));
    }

    public void testHeadersAreRecordedAsAttributes() {
        final APMTracer tracer = tracer(enabledSettings());
        final String projectId = randomAlphaOfLength(16);
        final String opaqueId = randomAlphaOfLength(16);
        threadContext.putHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId);
        threadContext.putHeader(Task.X_OPAQUE_ID_HTTP_HEADER, opaqueId);

        startAndEnd(tracer, "name1");

        final Attributes attributes = finishedSpan("name1").getAttributes();
        assertThat(attributes.get(stringKey("project.id")), equalTo(projectId));
        assertThat(attributes.get(stringKey("es.x-opaque-id")), equalTo(opaqueId));
    }

    public void testNoHeaderAttributesWithoutTheHeaders() {
        final APMTracer tracer = tracer(enabledSettings());

        startAndEnd(tracer, "name1");

        final Attributes attributes = finishedSpan("name1").getAttributes();
        assertThat(attributes.get(stringKey("project.id")), nullValue());
        assertThat(attributes.get(stringKey("es.x-opaque-id")), nullValue());
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
        tracer.setRecordExceptionStacks(true);

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
        final SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
            .setSampler(sampler)
            .addSpanProcessor(SimpleSpanProcessor.create(exporter))
            .build();
        final OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .build();
        sdks.add(sdk);

        final APMTracer tracer = new APMTracer(threadContext, settings, () -> sdk, maxTraceDepth, recordExceptionStacks);
        tracer.setNodeName("test-node");
        tracer.setClusterName("test-cluster");
        tracer.start();
        return tracer;
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

    /** Asserts that the thread context marks the local trace as dropped and that no trace context is propagated. */
    private void assertLocalTraceDropped() {
        assertThat(currentSpanContext().isValid(), is(false));
        assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), nullValue());
        assertThat(threadContext.getHeader(Task.TRACE_STATE), nullValue());
    }
}
