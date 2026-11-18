/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.tracing;

import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.TextMapPropagator;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.Build;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.telemetry.apm.internal.util.PatternAutomata;
import org.elasticsearch.telemetry.tracing.Span;
import org.elasticsearch.telemetry.tracing.Tracer;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * {@link Tracer} implementation provided by the Elasticsearch {@code apm} module ({@code modules/apm}). It records spans using the
 * OpenTelemetry API, which exports them over OTLP through an {@link io.opentelemetry.sdk.OpenTelemetrySdk} owned by this module.
 */
public class APMTracer implements Tracer {

    private final ThreadContext threadContext;
    private final Supplier<TracerProvider> tracerProvider;
    private final AtomicBoolean enabled;
    private final AtomicBoolean recordExceptionStacks;

    private final TextMapPropagator propagator = W3CTraceContextPropagator.getInstance();

    @Nullable
    private volatile io.opentelemetry.api.trace.Tracer otelTracer;

    private List<String> includeNames = List.of();
    private List<String> excludeNames = List.of();
    /** Built using {@link #includeNames} and {@link #excludeNames}, and filters out spans based on their name. */
    private volatile CharacterRunAutomaton filterAutomaton = PatternAutomata.compile(List.of(), List.of());
    private volatile int maxTraceDepth;
    private volatile int maxSpans;
    private volatile CharacterRunAutomaton labelFilterAutomaton = PatternAutomata.compile(List.of(), List.of());
    private volatile String clusterName;
    private volatile String nodeName;

    public APMTracer(
        ThreadContext threadContext,
        Supplier<TracerProvider> tracerProvider,
        AtomicBoolean enabled,
        AtomicBoolean recordExceptionStacks
    ) {
        this.threadContext = threadContext;
        this.tracerProvider = tracerProvider;
        this.enabled = enabled;
        this.recordExceptionStacks = recordExceptionStacks;
    }

    @Override
    public APMSpanBuilder spanBuilder(String name) {
        assert name != null : "span name cannot be null";
        return new APMSpanBuilder(this, requireNonNull(name, "span name"));
    }

    @Override
    public Span currentSpan() {
        var span = threadContext.getSpan();
        assert span != null : "span cannot be null";
        if (span == null || span == Span.noop()) {
            return Span.noop();
        }
        assert span instanceof AbstractSpan : "only AbstractSpan instances are allowed in ThreadContext; found" + span.getClass().getName();
        return span;
    }

    public synchronized void setIncludeNames(List<String> includeNames) {
        this.includeNames = includeNames;
        this.filterAutomaton = PatternAutomata.compile(includeNames, excludeNames);
    }

    public synchronized void setExcludeNames(List<String> excludeNames) {
        this.excludeNames = excludeNames;
        this.filterAutomaton = PatternAutomata.compile(includeNames, excludeNames);
    }

    public void setLabelFilters(List<String> labelFilters) {
        this.labelFilterAutomaton = PatternAutomata.compile(labelFilters, List.of());
    }

    public void setMaxTraceDepth(int maxTraceDepth) {
        this.maxTraceDepth = maxTraceDepth;
    }

    public void setMaxSpans(int maxSpans) {
        this.maxSpans = maxSpans;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    ThreadContext threadContext() {
        return threadContext;
    }

    io.opentelemetry.api.trace.Tracer otelTracer() {
        var otelTracer = this.otelTracer;
        if (otelTracer == null) {
            // no need for double-checked locking, the SDK will always return the same tracer instance for given scope
            this.otelTracer = otelTracer = tracerProvider.get().get("elasticsearch", Build.current().version());
        }
        return otelTracer;
    }

    boolean isEnabled() {
        return enabled.get();
    }

    String clusterName() {
        return clusterName;
    }

    String nodeName() {
        return nodeName;
    }

    /** Whether the name of a span is excluded from tracing by the configured include and exclude patterns. */
    boolean isFilteredOut(String spanName) {
        return filterAutomaton.run(spanName) == false;
    }

    int maxTraceDepth() {
        return maxTraceDepth;
    }

    int maxSpans() {
        return maxSpans;
    }

    /** Whether the value of an attribute with the given key is sensitive and must be redacted. */
    boolean isRedacted(String attributeKey) {
        return labelFilterAutomaton.run(attributeKey);
    }

    boolean recordExceptionStacks() {
        return recordExceptionStacks.get();
    }

    TextMapPropagator propagator() {
        return propagator;
    }
}
