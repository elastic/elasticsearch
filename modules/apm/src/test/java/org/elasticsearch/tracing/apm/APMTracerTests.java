/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tracing.apm;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.tracing.apm.APMAgentSettings.APM_ENABLED_SETTING;
import static org.elasticsearch.tracing.apm.APMAgentSettings.APM_TRACING_NAMES_EXCLUDE_SETTING;
import static org.elasticsearch.tracing.apm.APMAgentSettings.APM_TRACING_NAMES_INCLUDE_SETTING;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class APMTracerTests extends ESTestCase {

    /**
     * Check that the tracer doesn't create spans when tracing is disabled.
     */
    public void test_onTraceStarted_withTracingDisabled_doesNotStartTrace() {
        Settings settings = Settings.builder().put(APM_ENABLED_SETTING.getKey(), false).build();
        APMTracer apmTracer = buildTracer(settings);

        apmTracer.startTrace(new ThreadContext(settings), "id1", "name1", null);

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

        apmTracer.startTrace(new ThreadContext(settings), "id1", "name1", null);

        assertThat(apmTracer.getSpans(), anEmptyMap());
    }

    /**
     * Check that when a trace is started, the tracer starts a span and records it.
     */
    public void test_onTraceStarted_startsTrace() {
        Settings settings = Settings.builder().put(APM_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings);

        apmTracer.startTrace(new ThreadContext(settings), "id1", "name1", null);

        assertThat(apmTracer.getSpans(), aMapWithSize(1));
        assertThat(apmTracer.getSpans(), hasKey("id1"));
    }

    /**
     * Check that when a trace is started, the tracer ends the span and removes the record of it.
     */
    public void test_onTraceStopped_stopsTrace() {
        Settings settings = Settings.builder().put(APM_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings);

        apmTracer.startTrace(new ThreadContext(settings), "id1", "name1", null);
        apmTracer.stopTrace("id1");

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
        apmTracer.startTrace(threadContext, "id1", "name1", null);
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

        apmTracer.startTrace(new ThreadContext(settings), "id1", "name-aaa", null);
        apmTracer.startTrace(new ThreadContext(settings), "id2", "name-bbb", null);
        apmTracer.startTrace(new ThreadContext(settings), "id3", "name-ccc", null);

        assertThat(apmTracer.getSpans(), hasKey("id1"));
        assertThat(apmTracer.getSpans(), hasKey("id2"));
        assertThat(apmTracer.getSpans(), not(hasKey("id3")));
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

        apmTracer.startTrace(new ThreadContext(settings), "id1", "name-aaa", null);

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

        apmTracer.startTrace(new ThreadContext(settings), "id1", "name-aaa", null);
        apmTracer.startTrace(new ThreadContext(settings), "id2", "name-bbb", null);
        apmTracer.startTrace(new ThreadContext(settings), "id3", "name-ccc", null);

        assertThat(apmTracer.getSpans(), not(hasKey("id1")));
        assertThat(apmTracer.getSpans(), not(hasKey("id2")));
        assertThat(apmTracer.getSpans(), hasKey("id3"));
    }

    private APMTracer buildTracer(Settings settings) {
        APMTracer tracer = new APMTracer(settings);
        tracer.doStart();
        return tracer;
    }
}
