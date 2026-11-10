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
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.instrumentation.api.instrumenter.SpanStatusBuilder;
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
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.lucene.util.automaton.MinimizationOperations;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;
import org.elasticsearch.telemetry.apm.internal.export.TraceSupplier;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkExportTracerSupplier;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.OtelSdkSettings;
import org.elasticsearch.telemetry.tracing.Traceable;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * {@link org.elasticsearch.telemetry.tracing.Tracer} implementation provided by the Elasticsearch {@code apm}
 * module ({@code modules/apm}). It records spans using the OpenTelemetry API and exports them over OTLP through
 * the {@link OtelSdkExportTracerSupplier}, an {@link io.opentelemetry.sdk.OpenTelemetrySdk} owned by this module.
 */
public class APMTracer extends AbstractLifecycleComponent implements org.elasticsearch.telemetry.tracing.Tracer {

    private static final Logger logger = LogManager.getLogger(APMTracer.class);

    /** Tracks per-span local depth in Context to enforce {@link OtelSdkSettings#TELEMETRY_TRACING_MAX_DEPTH}. */
    private static final ContextKey<Integer> SPAN_LOCAL_DEPTH_KEY = ContextKey.named("es.apm.span.local_depth");

    /** Counter shared by all spans under the same local root to enforce {@link OtelSdkSettings#TELEMETRY_TRACING_MAX_SPANS}. */
    private static final ContextKey<AtomicInteger> SPAN_LOCAL_COUNT_KEY = ContextKey.named("es.apm.span.local_count");

    /** Holds in-flight span information. */
    private final Map<String, Context> spans = ConcurrentCollections.newConcurrentMap();

    final ThreadContext threadContext;
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
    record APMServices(Tracer tracer, OpenTelemetry openTelemetry) {}

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
        this.spans.clear();// discard in-flight spans
    }

    @Override
    public void startTrace(Traceable traceable, String spanName, @Nullable Map<String, Object> attributes) {
        String spanId = traceable.getSpanId();
        assert spanId != null;
        assert spanName != null;

        var services = this.services;
        if (services == null) {
            // tracing has been disabled, there is nothing to keep, see storeSpanInThreadContext
            return;
        }

        spans.computeIfAbsent(spanId, _spanId -> startSpan(services, spanId, spanName, attributes));
    }

    /**
     * Starts the span, if it is to be traced, and returns the context that stands for the decision: it holds the span if it is recorded,
     * and a span that is not recorded otherwise. This is what makes the decision part of what is propagated to the descendants of the
     * span, both local and remote ones, once the context is made the current one by {@link #storeSpanInThreadContext}.
     */
    private Context startSpan(APMServices services, String spanId, String spanName, @Nullable Map<String, Object> attributes) {
        final Context localParentContext = Objects.requireNonNullElse(threadContext.getTransient(Task.APM_TRACE_CONTEXT), Context.root());
        final Span localParentSpan = Span.fromContextOrNull(localParentContext);

        // The whole local trace is dropped when its root span was not sampled: propagate its context to disable any further child spans
        if (localParentSpan != null && localParentSpan.getSpanContext().isSampled() == false) {
            logger.trace("Skipping span [{}] [{}] as its local trace was dropped", spanId, spanName);
            return localParentContext;
        }

        // todo: this will become a part of sampler logic later on
        if (filterAutomaton.run(spanName) == false) {
            logger.trace("Skipping tracing [{}] [{}] as it has been filtered out", spanId, spanName);
            return localParentContext.with(Span.getInvalid());
        }

        // max_depth and max_spans apply per local root: the first span of a request on this node, even if it continues a
        // remote trace. A child gets its parent's depth + 1 and shares its parent's span counter, which excludes the root.
        final Integer parentDepth = localParentContext.get(SPAN_LOCAL_DEPTH_KEY);
        final int localDepth = localParentSpan == null ? 0 : (parentDepth == null ? 0 : parentDepth) + 1;

        final AtomicInteger parentSpanCount = localParentContext.get(SPAN_LOCAL_COUNT_KEY);
        final AtomicInteger localSpanCount = parentSpanCount == null ? new AtomicInteger() : parentSpanCount;

        if (localDepth > maxTraceDepth) {
            logger.trace("Skipping span [{}] [{}] at local depth {} (maxTraceDepth={})", spanId, spanName, localDepth, maxTraceDepth);
            return localParentContext.with(Span.getInvalid());
        }

        if (localDepth > 0 && (localSpanCount.get() > maxSpans || localSpanCount.incrementAndGet() > maxSpans)) {
            logger.trace("Skipping span [{}] [{}] as the local root reached maxSpans={}", spanId, spanName, maxSpans);
            return localParentContext.with(Span.getInvalid());
        }

        logger.trace("Tracing [{}] [{}]", spanId, spanName);
        final SpanBuilder spanBuilder = services.tracer.spanBuilder(spanName);

        // A span can have a parent span, which is important for seeing a complete trace in the APM UI.
        // Try to use the local parent first, otherwise look for a remote parent in the request headers; otherwise use Context.root()
        final Context parentContext = localParentSpan != null ? localParentContext : getRemoteParentContext(services);
        spanBuilder.setParent(parentContext);

        setSpanAttributes(attributes, spanBuilder);
        setAttributesFromHeaders(spanBuilder);

        if (localParentSpan == null) {
            Instant startTime = threadContext.getTransient(Task.LOCAL_TRACE_START_TIME);
            if (startTime != null) {
                spanBuilder.setStartTimestamp(startTime);
            }
        }

        final Span span = spanBuilder.startSpan();
        if (span.isRecording() == false) {
            logger.trace("Span [{}] [{}] will not be recorded due to sampling", spanId, spanName);
        }
        return parentContext.with(span).with(SPAN_LOCAL_DEPTH_KEY, localDepth).with(SPAN_LOCAL_COUNT_KEY, localSpanCount);
    }

    @Override
    public ThreadContext.StoredContext storeSpanInThreadContext(Traceable traceable) {
        final var services = this.services;
        final Context context = spans.get(traceable.getSpanId());
        if (context == null && services != null) {
            // not traced, so there is nothing to make current
            return () -> {};
        }

        final ThreadContext.StoredContext storedContext = threadContext.newStoredContextPreservingResponseHeaders();
        // if tracing is disabled, any trace-related info is dropped from the thread context
        updateThreadContext(services, context != null ? context : Context.root());
        return storedContext;
    }

    private void updateThreadContext(@Nullable APMServices services, Context context) {
        threadContext.putTransientAllowOverwrite(Task.APM_TRACE_CONTEXT, context);

        // the propagator does not write an empty tracestate, so make sure that no stale value is left behind
        threadContext.removeHeaders(Task.TRACE_PARENT_HTTP_HEADER, Task.TRACE_STATE);

        if (services == null) {
            return;
        }
        // propagate current trace headers into thread context
        services.openTelemetry.getPropagators()
            .getTextMapPropagator()
            .inject(context, threadContext, (tc, key, value) -> requireNonNull(tc).putHeaderAllowOverwrite(key, value));
    }

    private Context getRemoteParentContext(APMServices services) {
        return services.openTelemetry.getPropagators()
            .getTextMapPropagator()
            .extract(Context.root(), threadContext, (tc, key) -> requireNonNull(tc).getHeader(key));
    }

    private void setSpanAttributes(@Nullable Map<String, Object> spanAttributes, SpanBuilder spanBuilder) {
        if (spanAttributes != null) {
            for (Map.Entry<String, Object> entry : spanAttributes.entrySet()) {
                final String key = entry.getKey();
                final Object value = entry.getValue();

                if (this.labelFilterAutomaton.run(key)) {
                    spanBuilder.setAttribute(key, "[REDACTED]");
                    continue;
                }

                if (value instanceof String) {
                    spanBuilder.setAttribute(key, (String) value);
                } else if (value instanceof Long) {
                    spanBuilder.setAttribute(key, (Long) value);
                } else if (value instanceof Integer) {
                    spanBuilder.setAttribute(key, (Integer) value);
                } else if (value instanceof Double) {
                    spanBuilder.setAttribute(key, (Double) value);
                } else if (value instanceof Boolean) {
                    spanBuilder.setAttribute(key, (Boolean) value);
                } else if (value == null) {
                    throw new IllegalArgumentException("span attributes cannot have a null value");
                } else {
                    throw new IllegalArgumentException(
                        "span attributes do not support value type of [" + value.getClass().getCanonicalName() + "]"
                    );
                }
            }

            final boolean isHttpSpan = spanAttributes.keySet().stream().anyMatch(key -> key.startsWith("http."));
            spanBuilder.setSpanKind(isHttpSpan ? SpanKind.SERVER : SpanKind.INTERNAL);
        } else {
            spanBuilder.setSpanKind(SpanKind.INTERNAL);
        }

        spanBuilder.setAttribute(org.elasticsearch.telemetry.tracing.Tracer.AttributeKeys.NODE_NAME, nodeName);
        spanBuilder.setAttribute(org.elasticsearch.telemetry.tracing.Tracer.AttributeKeys.CLUSTER_NAME, clusterName);
    }

    private void setAttributesFromHeaders(SpanBuilder spanBuilder) {
        final String xOpaqueId = threadContext.getHeader(Task.X_OPAQUE_ID_HTTP_HEADER);
        if (xOpaqueId != null) {
            spanBuilder.setAttribute("es.x-opaque-id", xOpaqueId);
        }

        final String projectId = threadContext.getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER);
        if (projectId != null) {
            spanBuilder.setAttribute("project.id", projectId);
        }
    }

    private Span getSpan(Traceable traceable) {
        final Context context = spans.get(traceable.getSpanId());
        return context == null ? Span.getInvalid() : Span.fromContext(context);
    }

    @Override
    public void addError(Traceable traceable, Throwable throwable) {
        final var span = getSpan(traceable);
        if (recordExceptionStacks) {
            span.recordException(throwable);
            return;
        }
        AttributesBuilder attrs = Attributes.builder().put("exception.type", throwable.getClass().getName());
        Optional.ofNullable(throwable.getMessage()).ifPresent(m -> attrs.put("exception.message", m));
        span.addEvent("exception", attrs.build());
    }

    @Override
    public void setAttribute(Traceable traceable, String key, boolean value) {
        getSpan(traceable).setAttribute(key, value);
    }

    @Override
    public void setAttribute(Traceable traceable, String key, double value) {
        getSpan(traceable).setAttribute(key, value);
    }

    @Override
    public void setAttribute(Traceable traceable, String key, long value) {
        getSpan(traceable).setAttribute(key, value);
    }

    @Override
    public void setAttribute(Traceable traceable, String key, String value) {
        getSpan(traceable).setAttribute(key, value);
    }

    public void setAttributes(Traceable traceable, Attributes attributes) {
        getSpan(traceable).setAllAttributes(attributes);
    }

    public SpanStatusBuilder spanStatusBuilder(Traceable traceable) {
        return new APMSpanStatusBuilder(getSpan(traceable));
    }

    private record APMSpanStatusBuilder(Span span) implements SpanStatusBuilder {

        @Override
        public SpanStatusBuilder setStatus(StatusCode statusCode, String description) {
            span.setStatus(statusCode, description);
            return this;
        }
    }

    @Override
    public void stopTrace(Traceable traceable) {
        final String spanId = traceable.getSpanId();
        final Context context = spans.remove(spanId);
        if (context != null) {
            logger.trace("Finishing trace [{}]", spanId);
            Span.fromContext(context).end();
        }
    }

    // VisibleForTesting
    Map<String, Context> getSpans() {
        return spans;
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
