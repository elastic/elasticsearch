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
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.common.CompletableResultCode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.Build;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.lucene.util.automaton.MinimizationOperations;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;
import org.elasticsearch.telemetry.apm.internal.export.TraceSupplier;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkExportTracerSupplier;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings;
import org.elasticsearch.telemetry.tracing.Span;
import org.elasticsearch.telemetry.tracing.Tracer;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * {@link Tracer} implementation provided by the Elasticsearch {@code apm} module ({@code modules/apm}). It records spans using the
 * OpenTelemetry API, which exports them over OTLP through an {@link io.opentelemetry.sdk.OpenTelemetrySdk} owned by this module.
 */
public class APMTracer extends AbstractLifecycleComponent implements Tracer {

    private static final Logger logger = LogManager.getLogger(APMTracer.class);

    private final ThreadContext threadContext;
    private final TraceSupplier traceSupplier;

    private volatile boolean enabled;
    private volatile APMServices services;

    private List<String> includeNames;
    private List<String> excludeNames;
    private List<String> labelFilters;
    /** Built using {@link #includeNames} and {@link #excludeNames}, and filters out spans based on their name. */
    private volatile CharacterRunAutomaton filterAutomaton;
    private volatile CharacterRunAutomaton labelFilterAutomaton;
    private String clusterName;
    private String nodeName;

    /** Maximum local span depth on the OTel SDK path; see {@link OtelSdkSettings#TELEMETRY_TRACING_MAX_DEPTH}. */
    private volatile int maxTraceDepth;

    /** Maximum number of spans under a local root on the OTel SDK path; see {@link OtelSdkSettings#TELEMETRY_TRACING_MAX_SPANS}. */
    private volatile int maxSpans;

    /** Whether to attach exception stack traces on the OTel SDK path;
     *  see {@link OtelSdkSettings#TELEMETRY_TRACING_RECORD_EXCEPTION_STACKS}. */
    private volatile boolean recordExceptionStacks;

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    /**
     * This class is used to make all OpenTelemetry services visible at once
     */
    record APMServices(io.opentelemetry.api.trace.Tracer tracer, OpenTelemetry openTelemetry) {}

    public APMTracer(ThreadContext threadContext, Settings settings, Supplier<MeterProvider> meterProvider) {
        this(
            threadContext,
            settings,
            new OtelSdkExportTracerSupplier(settings, meterProvider),
            OtelSdkSettings.TELEMETRY_TRACING_MAX_DEPTH.get(settings),
            OtelSdkSettings.TELEMETRY_TRACING_RECORD_EXCEPTION_STACKS.get(settings)
        );
    }

    // package-private for testing
    APMTracer(
        ThreadContext threadContext,
        Settings settings,
        TraceSupplier traceSupplier,
        int maxTraceDepth,
        boolean recordExceptionStacks
    ) {
        this.threadContext = threadContext;
        this.traceSupplier = traceSupplier;
        this.includeNames = APMAgentSettings.TELEMETRY_TRACING_NAMES_INCLUDE_SETTING.get(settings);
        this.excludeNames = APMAgentSettings.TELEMETRY_TRACING_NAMES_EXCLUDE_SETTING.get(settings);
        this.labelFilters = APMAgentSettings.TELEMETRY_TRACING_SANITIZE_FIELD_NAMES.get(settings);

        this.filterAutomaton = buildAutomaton(includeNames, excludeNames);
        this.labelFilterAutomaton = buildAutomaton(labelFilters, List.of());
        this.enabled = APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.get(settings);
        this.maxTraceDepth = maxTraceDepth;
        this.maxSpans = OtelSdkSettings.TELEMETRY_TRACING_MAX_SPANS.get(settings);
        this.recordExceptionStacks = recordExceptionStacks;
    }

    public CompletableResultCode attemptFlushTraces() {
        if (enabled == false) {
            return CompletableResultCode.ofSuccess();
        }
        return traceSupplier.attemptFlushTraces();
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        if (enabled) {
            this.services = createApmServices();
        } else {
            destroyApmServices();
        }
    }

    public void setIncludeNames(List<String> includeNames) {
        this.includeNames = includeNames;
        this.filterAutomaton = buildAutomaton(includeNames, excludeNames);
    }

    public void setExcludeNames(List<String> excludeNames) {
        this.excludeNames = excludeNames;
        this.filterAutomaton = buildAutomaton(includeNames, excludeNames);
    }

    public void setLabelFilters(List<String> labelFilters) {
        this.labelFilters = labelFilters;
        this.labelFilterAutomaton = buildAutomaton(labelFilters, List.of());
    }

    public void setMaxTraceDepth(int maxTraceDepth) {
        this.maxTraceDepth = maxTraceDepth;
    }

    public void setMaxSpans(int maxSpans) {
        this.maxSpans = maxSpans;
    }

    public void setRecordExceptionStacks(boolean recordExceptionStacks) {
        this.recordExceptionStacks = recordExceptionStacks;
    }

    // package-private for testing
    CharacterRunAutomaton getLabelFilterAutomaton() {
        return labelFilterAutomaton;
    }

    @Override
    protected void doStart() {
        if (enabled) {
            this.services = createApmServices();
        }
    }

    @Override
    protected void doStop() {
        // Best-effort flush of buffered spans, but keep the SDK alive: node shutdown stops all lifecycle components
        // before closing any of them, so leaving the tracer running until doClose() lets spans emitted during the rest
        // of the shutdown sequence (e.g. shard relocations draining in IndicesService) still be recorded and exported.
        if (enabled) {
            try {
                traceSupplier.attemptFlushTraces().join(OtelSdkSettings.OTEL_EXPORT_FLUSH_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                logger.warn("Exception flushing trace supplier", e);
            }
        }
    }

    @Override
    protected void doClose() {
        try {
            traceSupplier.close();
        } catch (Exception e) {
            logger.warn("Exception closing trace supplier", e);
        }
        destroyApmServices();
    }

    // package-private for tests
    APMServices createApmServices() {
        assert this.enabled;
        assert this.services == null;

        var openTelemetry = traceSupplier.get();
        var tracer = openTelemetry.getTracer("elasticsearch", Build.current().version());
        return new APMServices(tracer, openTelemetry);
    }

    private void destroyApmServices() {
        this.services = null;
    }

    @Override
    public APMSpanBuilder spanBuilder(String name) {
        assert name != null : "span name cannot be null";
        return new APMSpanBuilder(this, requireNonNull(name, "span name"));
    }

    @Override
    public Span currentSpan() {
        final Context context = threadContext.getTransient(Task.APM_TRACE_CONTEXT);
        if (context == null) {
            return Span.noop();
        }
        // always return the APMSpan wrapper, even if it's ended or not sampled: other spans might want to set it as the parent span
        return new APMSpan(this, context);
    }

    ThreadContext threadContext() {
        return threadContext;
    }

    /** The OpenTelemetry services, or {@code null} if tracing is disabled. */
    @Nullable
    APMServices services() {
        return services;
    }

    String clusterName() {
        return clusterName;
    }

    String nodeName() {
        return nodeName;
    }

    int maxTraceDepth() {
        return maxTraceDepth;
    }

    int maxSpans() {
        return maxSpans;
    }

    boolean recordExceptionStacks() {
        return recordExceptionStacks;
    }

    /** Whether the name of a span is excluded from tracing by the configured include and exclude patterns. */
    boolean isFilteredOut(String spanName) {
        return filterAutomaton.run(spanName) == false;
    }

    /** Whether the value of an attribute with the given key is sensitive and must be redacted. */
    boolean isRedacted(String attributeKey) {
        return labelFilterAutomaton.run(attributeKey);
    }

    private static CharacterRunAutomaton buildAutomaton(List<String> includePatterns, List<String> excludePatterns) {
        Automaton includeAutomaton = patternsToAutomaton(includePatterns);
        Automaton excludeAutomaton = patternsToAutomaton(excludePatterns);

        if (includeAutomaton == null) {
            includeAutomaton = Automata.makeAnyString();
        }

        final Automaton finalAutomaton = excludeAutomaton == null
            ? includeAutomaton
            : Operations.minus(includeAutomaton, excludeAutomaton, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);

        return new CharacterRunAutomaton(MinimizationOperations.minimize(finalAutomaton, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT));
    }

    private static Automaton patternsToAutomaton(List<String> patterns) {
        final List<Automaton> automata = patterns.stream().map(s -> {
            final String regex = s.replace(".", "\\.").replace("*", ".*");
            return new RegExp(regex, RegExp.ALL | RegExp.DEPRECATED_COMPLEMENT).toAutomaton();
        }).toList();
        if (automata.isEmpty()) {
            return null;
        }
        if (automata.size() == 1) {
            return automata.get(0);
        }
        return Operations.union(automata);
    }

}
