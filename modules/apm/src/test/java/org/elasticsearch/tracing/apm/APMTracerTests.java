/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tracing.apm;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.tracing.SpanId;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.elasticsearch.tracing.apm.APMAgentSettings.APM_ENABLED_SETTING;
import static org.elasticsearch.tracing.apm.APMAgentSettings.APM_TRACING_NAMES_EXCLUDE_SETTING;
import static org.elasticsearch.tracing.apm.APMAgentSettings.APM_TRACING_NAMES_INCLUDE_SETTING;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class APMTracerTests extends ESTestCase {

    private static final SpanId SPAN_ID1 = SpanId.forBareString("id1");
    private static final SpanId SPAN_ID2 = SpanId.forBareString("id2");
    private static final SpanId SPAN_ID3 = SpanId.forBareString("id3");

    /**
     * Check that the tracer doesn't create spans when tracing is disabled.
     */
    public void test_onTraceStarted_withTracingDisabled_doesNotStartTrace() {
        Settings settings = Settings.builder().put(APM_ENABLED_SETTING.getKey(), false).build();
        APMTracer apmTracer = buildTracer(settings);

        apmTracer.startTrace(new ThreadContext(settings), SPAN_ID1, "name1", null);

        assertThat(apmTracer.getSpans(), anEmptyMap());
    }

    /**
     * Check that the tracer doesn't create spans if a Traceable's span name is filtered out.
     */
    public void test_onTraceStarted_withSpanNameOmitted_doesNotStartTrace() {
        Settings settings = Settings.builder()
            .put(APM_ENABLED_SETTING.getKey(), true)
            .putList(APM_TRACING_NAMES_INCLUDE_SETTING.getKey(), List.of("filtered*"))
            .build();
        APMTracer apmTracer = buildTracer(settings);

        apmTracer.startTrace(new ThreadContext(settings), SPAN_ID1, "name1", null);

        assertThat(apmTracer.getSpans(), anEmptyMap());
    }

    /**
     * Check that when a trace is started, the tracer starts a span and records it.
     */
    public void test_onTraceStarted_startsTrace() {
        Settings settings = Settings.builder().put(APM_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings);

        apmTracer.startTrace(new ThreadContext(settings), SPAN_ID1, "name1", null);

        assertThat(apmTracer.getSpans(), aMapWithSize(1));
        assertThat(apmTracer.getSpans(), hasKey(SPAN_ID1));
    }

    /**
     * Checks that when a trace is started with a specific start time, the tracer starts a span and records it.
     */
    public void test_onTraceStartedWithStartTime_startsTrace() {
        Settings settings = Settings.builder().put(APM_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings);

        ThreadContext threadContext = new ThreadContext(settings);
        // 1_000_000L because of "toNanos" conversions that overflow for large long millis
        Instant spanStartTime = Instant.ofEpochMilli(randomLongBetween(0, Long.MAX_VALUE / 1_000_000L));
        threadContext.putTransient(Task.TRACE_START_TIME, spanStartTime);
        apmTracer.startTrace(threadContext, SPAN_ID1, "name1", null);

        assertThat(apmTracer.getSpans(), aMapWithSize(1));
        assertThat(apmTracer.getSpans(), hasKey(SPAN_ID1));
        assertThat(((SpyAPMTracer) apmTracer).getSpanStartTime("name1"), is(spanStartTime));
    }

    /**
     * Check that when a trace is stopped, the tracer ends the span and removes the record of it.
     */
    public void test_onTraceStopped_stopsTrace() {
        Settings settings = Settings.builder().put(APM_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings);

        apmTracer.startTrace(new ThreadContext(settings), SPAN_ID1, "name1", null);
        apmTracer.stopTrace(SPAN_ID1);

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
        Settings settings = Settings.builder().put(APM_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings);

        ThreadContext threadContext = new ThreadContext(settings);
        apmTracer.startTrace(threadContext, SPAN_ID1, "name1", null);
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
            .put(APM_ENABLED_SETTING.getKey(), true)
            .putList(APM_TRACING_NAMES_INCLUDE_SETTING.getKey(), includePatterns)
            .build();
        APMTracer apmTracer = buildTracer(settings);

        apmTracer.startTrace(new ThreadContext(settings), SPAN_ID1, "name-aaa", null);
        apmTracer.startTrace(new ThreadContext(settings), SPAN_ID2, "name-bbb", null);
        apmTracer.startTrace(new ThreadContext(settings), SPAN_ID3, "name-ccc", null);

        assertThat(apmTracer.getSpans(), hasKey(SPAN_ID1));
        assertThat(apmTracer.getSpans(), hasKey(SPAN_ID2));
        assertThat(apmTracer.getSpans(), not(hasKey(SPAN_ID3)));
    }

    /**
     * Check that when a tracer has a list of include and exclude names configured, and
     * a span matches both, then the exclude filters take precedence.
     */
    public void test_whenTraceStarted_andSpanNameIncludedAndExcluded_thenSpanIsNotStarted() {
        final List<String> includePatterns = List.of("name-a*");
        final List<String> excludePatterns = List.of("name-a*");
        Settings settings = Settings.builder()
            .put(APM_ENABLED_SETTING.getKey(), true)
            .putList(APM_TRACING_NAMES_INCLUDE_SETTING.getKey(), includePatterns)
            .putList(APM_TRACING_NAMES_EXCLUDE_SETTING.getKey(), excludePatterns)
            .build();
        APMTracer apmTracer = buildTracer(settings);

        apmTracer.startTrace(new ThreadContext(settings), SPAN_ID1, "name-aaa", null);

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
            .put(APM_ENABLED_SETTING.getKey(), true)
            .putList(APM_TRACING_NAMES_EXCLUDE_SETTING.getKey(), excludePatterns)
            .build();
        APMTracer apmTracer = buildTracer(settings);

        apmTracer.startTrace(new ThreadContext(settings), SPAN_ID1, "name-aaa", null);
        apmTracer.startTrace(new ThreadContext(settings), SPAN_ID2, "name-bbb", null);
        apmTracer.startTrace(new ThreadContext(settings), SPAN_ID3, "name-ccc", null);

        assertThat(apmTracer.getSpans(), not(hasKey(SPAN_ID1)));
        assertThat(apmTracer.getSpans(), not(hasKey(SPAN_ID2)));
        assertThat(apmTracer.getSpans(), hasKey(SPAN_ID3));
    }

    /**
     * Check that sensitive attributes are not added verbatim to a span, but instead the value is redacted.
     */
    public void test_whenAddingAttributes_thenSensitiveValuesAreRedacted() {
        Settings settings = Settings.builder().put(APM_ENABLED_SETTING.getKey(), false).build();
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

    private APMTracer buildTracer(Settings settings) {
        APMTracer tracer = new SpyAPMTracer(settings);
        tracer.doStart();
        return tracer;
    }

    static class SpyAPMTracer extends APMTracer {

        Map<String, Instant> spanStartTimeMap;

        SpyAPMTracer(Settings settings) {
            super(settings);
            this.spanStartTimeMap = new HashMap<>();
        }

        @Override
        APMServices createApmServices() {
            APMServices apmServices = super.createApmServices();
            Tracer mockTracer = mock(Tracer.class);
            doAnswer(invocation -> {
                String spanName = (String) invocation.getArguments()[0];
                // spy the spanBuilder
                return new SpySpanBuilder(apmServices.tracer(), spanName);
            }).when(mockTracer).spanBuilder(anyString());
            return new APMServices(mockTracer, apmServices.openTelemetry());
        }

        Instant getSpanStartTime(String spanName) {
            return spanStartTimeMap.get(spanName);
        }

        class SpySpanBuilder implements SpanBuilder {

            SpanBuilder delegatedSpanBuilder;
            Instant startTime;
            String spanName;

            SpySpanBuilder(Tracer tracer, String spanName) {
                this.delegatedSpanBuilder = tracer.spanBuilder(spanName);
                this.spanName = spanName;
            }

            @Override
            public SpanBuilder setParent(Context context) {
                delegatedSpanBuilder.setParent(context);
                return this;
            }

            @Override
            public SpanBuilder setNoParent() {
                delegatedSpanBuilder.setNoParent();
                return this;
            }

            @Override
            public SpanBuilder addLink(SpanContext spanContext) {
                delegatedSpanBuilder.addLink(spanContext);
                return this;
            }

            @Override
            public SpanBuilder addLink(SpanContext spanContext, Attributes attributes) {
                delegatedSpanBuilder.addLink(spanContext, attributes);
                return this;
            }

            @Override
            public SpanBuilder setAttribute(String key, String value) {
                delegatedSpanBuilder.setAttribute(key, value);
                return this;
            }

            @Override
            public SpanBuilder setAttribute(String key, long value) {
                delegatedSpanBuilder.setAttribute(key, value);
                return this;
            }

            @Override
            public SpanBuilder setAttribute(String key, double value) {
                delegatedSpanBuilder.setAttribute(key, value);
                return this;
            }

            @Override
            public SpanBuilder setAttribute(String key, boolean value) {
                delegatedSpanBuilder.setAttribute(key, value);
                return this;
            }

            @Override
            public <T> SpanBuilder setAttribute(AttributeKey<T> key, T value) {
                delegatedSpanBuilder.setAttribute(key, value);
                return this;
            }

            @Override
            public SpanBuilder setSpanKind(SpanKind spanKind) {
                delegatedSpanBuilder.setSpanKind(spanKind);
                return this;
            }

            @Override
            public SpanBuilder setStartTimestamp(long startTimestamp, TimeUnit unit) {
                startTime = Instant.ofEpochMilli(TimeUnit.MILLISECONDS.convert(startTimestamp, unit));
                delegatedSpanBuilder.setStartTimestamp(startTimestamp, unit);
                return this;
            }

            @Override
            public Span startSpan() {
                // finally record the spanName-startTime association when the span is actually started
                spanStartTimeMap.put(spanName, startTime);
                return delegatedSpanBuilder.startSpan();
            }
        }
    }
}
