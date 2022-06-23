/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apm;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.tracing.Traceable;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.apm.APMAgentSettings.APM_ENABLED_SETTING;
import static org.elasticsearch.xpack.apm.APMAgentSettings.APM_TRACING_NAMES_EXCLUDE_SETTING;
import static org.elasticsearch.xpack.apm.APMAgentSettings.APM_TRACING_NAMES_INCLUDE_SETTING;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class APMTracerTests extends ESTestCase {

    /**
     * Check that the tracer doesn't create spans when tracing is disabled.
     */
    public void test_onTraceStarted_withTracingDisabled_doesNotStartTrace() {
        Settings settings = Settings.builder().put(APM_ENABLED_SETTING.getKey(), false).build();
        APMTracer apmTracer = buildTracer(settings);

        Traceable traceable = new TestTraceable("1");
        apmTracer.onTraceStarted(new ThreadContext(settings), traceable);

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

        Traceable traceable = new TestTraceable("1");
        apmTracer.onTraceStarted(new ThreadContext(settings), traceable);

        assertThat(apmTracer.getSpans(), anEmptyMap());
    }

    /**
     * Check that when a trace is started, the tracer starts a span and records it.
     */
    public void test_onTraceStarted_startsTrace() {
        Settings settings = Settings.builder().put(APM_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings);

        Traceable traceable = new TestTraceable("1");
        apmTracer.onTraceStarted(new ThreadContext(settings), traceable);

        assertThat(apmTracer.getSpans(), aMapWithSize(1));
        assertThat(apmTracer.getSpans(), hasKey(traceable.getSpanId()));
    }

    /**
     * Check that when a trace is started, the tracer ends the span and removes the record of it.
     */
    public void test_onTraceStopped_stopsTrace() {
        Settings settings = Settings.builder().put(APM_ENABLED_SETTING.getKey(), true).build();
        APMTracer apmTracer = buildTracer(settings);

        Traceable traceable = new TestTraceable("1");
        apmTracer.onTraceStarted(new ThreadContext(settings), traceable);
        apmTracer.onTraceStopped(traceable);

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

        Traceable traceable = new TestTraceable("1");
        ThreadContext threadContext = new ThreadContext(settings);
        apmTracer.onTraceStarted(threadContext, traceable);
        assertThat(threadContext.getTransient(Task.APM_TRACE_CONTEXT), notNullValue());
    }

    /**
     * Check that when a tracer has a list of include names configured, then those
     * names are used to filter spans.
     */
    public void test_whenTraceStarted_andSpanNameIncluded_thenSpanIsStarted() {
        final List<String> includePatterns = List.of(
            // exact name
            "test-span-name-aaa",
            // regex
            "test-span-name-b*"
        );
        Settings settings = Settings.builder()
            .put(APM_ENABLED_SETTING.getKey(), true)
            .putList(APM_TRACING_NAMES_INCLUDE_SETTING.getKey(), includePatterns)
            .build();
        APMTracer apmTracer = buildTracer(settings);

        Traceable traceableA = new TestTraceable("aaa");
        Traceable traceableB = new TestTraceable("bbb");
        Traceable traceableC = new TestTraceable("ccc");
        apmTracer.onTraceStarted(new ThreadContext(settings), traceableA);
        apmTracer.onTraceStarted(new ThreadContext(settings), traceableB);
        apmTracer.onTraceStarted(new ThreadContext(settings), traceableC);

        assertThat(apmTracer.getSpans(), hasKey(traceableA.getSpanId()));
        assertThat(apmTracer.getSpans(), hasKey(traceableB.getSpanId()));
        assertThat(apmTracer.getSpans(), not(hasKey(traceableC.getSpanId())));
    }

    /**
     * Check that when a tracer has a list of include and exclude names configured, and
     * a span matches both, then the exclude filters take precedence.
     */
    public void test_whenTraceStarted_andSpanNameIncludedAndExcluded_thenSpanIsNotStarted() {
        final List<String> includePatterns = List.of("test-span-name-a*");
        final List<String> excludePatterns = List.of("test-span-name-a*");
        Settings settings = Settings.builder()
            .put(APM_ENABLED_SETTING.getKey(), true)
            .putList(APM_TRACING_NAMES_INCLUDE_SETTING.getKey(), includePatterns)
            .putList(APM_TRACING_NAMES_EXCLUDE_SETTING.getKey(), excludePatterns)
            .build();
        APMTracer apmTracer = buildTracer(settings);

        Traceable traceableA = new TestTraceable("aaa");
        apmTracer.onTraceStarted(new ThreadContext(settings), traceableA);

        assertThat(apmTracer.getSpans(), not(hasKey(traceableA.getSpanId())));
    }

    /**
     * Check that when a tracer has a list of exclude names configured, then those
     * names are used to filter spans.
     */
    public void test_whenTraceStarted_andSpanNameExcluded_thenSpanIsNotStarted() {
        final List<String> excludePatterns = List.of(
            // exact name
            "test-span-name-aaa",
            // regex
            "test-span-name-b*"
        );
        Settings settings = Settings.builder()
            .put(APM_ENABLED_SETTING.getKey(), true)
            .putList(APM_TRACING_NAMES_EXCLUDE_SETTING.getKey(), excludePatterns)
            .build();
        APMTracer apmTracer = buildTracer(settings);

        Traceable traceableA = new TestTraceable("aaa");
        Traceable traceableB = new TestTraceable("bbb");
        Traceable traceableC = new TestTraceable("ccc");
        apmTracer.onTraceStarted(new ThreadContext(settings), traceableA);
        apmTracer.onTraceStarted(new ThreadContext(settings), traceableB);
        apmTracer.onTraceStarted(new ThreadContext(settings), traceableC);

        assertThat(apmTracer.getSpans(), not(hasKey(traceableA.getSpanId())));
        assertThat(apmTracer.getSpans(), not(hasKey(traceableB.getSpanId())));
        assertThat(apmTracer.getSpans(), hasKey(traceableC.getSpanId()));
    }

    private APMTracer buildTracer(Settings settings) {
        APM apm = new APM(settings);

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(settings, new HashSet<>(apm.getSettings())));
        when(clusterService.getClusterName()).thenReturn(new ClusterName("testCluster"));

        APMTracer tracer = new APMTracer(settings, clusterService);
        tracer.doStart();
        return tracer;
    }

    private record TestTraceable(String id) implements Traceable {
        @Override
        public String getSpanId() {
            return "test-span-id-" + id;
        }

        @Override
        public String getSpanName() {
            return "test-span-name-" + id;
        }

        @Override
        public Map<String, Object> getAttributes() {
            return Map.of();
        }
    }
}
