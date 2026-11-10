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
import io.opentelemetry.context.Context;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.Build;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.PatternAutomata;
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
    private final Supplier<OpenTelemetry> openTelemetry;
    private final AtomicBoolean enabled;
    private final AtomicBoolean recordExceptionStacks;

    private volatile CharacterRunAutomaton labelFilterAutomaton = PatternAutomata.compile(List.of(), List.of());
    private volatile String clusterName;
    private volatile String nodeName;

    public APMTracer(
        ThreadContext threadContext,
        Supplier<OpenTelemetry> openTelemetry,
        AtomicBoolean enabled,
        AtomicBoolean recordExceptionStacks
    ) {
        this.threadContext = threadContext;
        this.openTelemetry = openTelemetry;
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

    io.opentelemetry.api.trace.Tracer otelTracer() {
        return openTelemetry.get().getTracer("elasticsearch", Build.current().version());
    }

    OpenTelemetry openTelemetry() {
        return openTelemetry.get();
    }

    boolean isEnabled() {
        return enabled.get();
    }

    public void setLabelFilters(List<String> labelFilters) {
        this.labelFilterAutomaton = PatternAutomata.compile(labelFilters, List.of());
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    String clusterName() {
        return clusterName;
    }

    String nodeName() {
        return nodeName;
    }

    /** Whether the value of an attribute with the given key is sensitive and must be redacted. */
    boolean isRedacted(String attributeKey) {
        return labelFilterAutomaton.run(attributeKey);
    }

    boolean recordExceptionStacks() {
        return recordExceptionStacks.get();
    }
}
