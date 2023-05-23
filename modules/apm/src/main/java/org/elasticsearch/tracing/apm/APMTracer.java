/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tracing.apm;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.Version;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.lucene.RegExp;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tracing.SpanId;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.tracing.apm.APMAgentSettings.APM_ENABLED_SETTING;
import static org.elasticsearch.tracing.apm.APMAgentSettings.APM_TRACING_NAMES_EXCLUDE_SETTING;
import static org.elasticsearch.tracing.apm.APMAgentSettings.APM_TRACING_NAMES_INCLUDE_SETTING;
import static org.elasticsearch.tracing.apm.APMAgentSettings.APM_TRACING_SANITIZE_FIELD_NAMES;

/**
 * This is an implementation of the {@link org.elasticsearch.tracing.Tracer} interface, which uses
 * the OpenTelemetry API to capture spans.
 * <p>
 * This module doesn't provide an implementation of the OTel API. Normally that would mean that the
 * API's default, no-op implementation would be used. However, when the APM Java is attached, it
 * intercepts the {@link GlobalOpenTelemetry} class and provides its own implementation instead.
 */
public class APMTracer extends AbstractLifecycleComponent implements org.elasticsearch.tracing.Tracer {

    private static final Logger logger = LogManager.getLogger(APMTracer.class);

    /** Holds in-flight span information. */
    private final Map<SpanId, Context> spans = ConcurrentCollections.newConcurrentMap();

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

    public APMTracer(Settings settings) {
        this.includeNames = APM_TRACING_NAMES_INCLUDE_SETTING.get(settings);
        this.excludeNames = APM_TRACING_NAMES_EXCLUDE_SETTING.get(settings);
        this.labelFilters = APM_TRACING_SANITIZE_FIELD_NAMES.get(settings);

        this.filterAutomaton = buildAutomaton(includeNames, excludeNames);
        this.labelFilterAutomaton = buildAutomaton(labelFilters, List.of());
        this.enabled = APM_ENABLED_SETTING.get(settings);
    }

    void setEnabled(boolean enabled) {
        this.enabled = enabled;
        if (enabled) {
            this.services = createApmServices();
        } else {
            destroyApmServices();
        }
    }

    void setIncludeNames(List<String> includeNames) {
        this.includeNames = includeNames;
        this.filterAutomaton = buildAutomaton(includeNames, excludeNames);
    }

    void setExcludeNames(List<String> excludeNames) {
        this.excludeNames = excludeNames;
        this.filterAutomaton = buildAutomaton(includeNames, excludeNames);
    }

    void setLabelFilters(List<String> labelFilters) {
        this.labelFilters = labelFilters;
        this.labelFilterAutomaton = buildAutomaton(labelFilters, List.of());
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
        destroyApmServices();
    }

    @Override
    protected void doClose() {}

    // package-private for tests
    APMServices createApmServices() {
        assert this.enabled;
        assert this.services == null;

        return AccessController.doPrivileged((PrivilegedAction<APMServices>) () -> {
            var openTelemetry = GlobalOpenTelemetry.get();
            var tracer = openTelemetry.getTracer("elasticsearch", Version.CURRENT.toString());

            return new APMServices(tracer, openTelemetry);
        });
    }

    private void destroyApmServices() {
        this.services = null;
        this.spans.clear();// discard in-flight spans
    }

    @Override
    public void startTrace(ThreadContext threadContext, SpanId spanId, String spanName, @Nullable Map<String, Object> attributes) {
        assert threadContext != null;
        assert spanId != null;
        assert spanName != null;

        // If tracing has been disabled, return immediately
        var services = this.services;
        if (services == null) {
            return;
        }

        if (filterAutomaton.run(spanName) == false) {
            logger.trace("Skipping tracing [{}] [{}] as it has been filtered out", spanId, spanName);
            return;
        }

        spans.computeIfAbsent(spanId, _spanId -> AccessController.doPrivileged((PrivilegedAction<Context>) () -> {
            logger.trace("Tracing [{}] [{}]", spanId, spanName);
            final SpanBuilder spanBuilder = services.tracer.spanBuilder(spanName);

            // A span can have a parent span, which here is modelled though a parent span context.
            // Setting this is important for seeing a complete trace in the APM UI.
            final Context parentContext = getParentContext(threadContext);
            if (parentContext != null) {
                spanBuilder.setParent(parentContext);
            }

            setSpanAttributes(threadContext, attributes, spanBuilder);

            Instant startTime = threadContext.getTransient(Task.TRACE_START_TIME);
            if (startTime != null) {
                spanBuilder.setStartTimestamp(startTime);
            }
            final Span span = spanBuilder.startSpan();
            final Context contextForNewSpan = Context.current().with(span);

            updateThreadContext(threadContext, services, contextForNewSpan);

            return contextForNewSpan;
        }));
    }

    /**
     * Called when a span starts. This version of the method relies on context to assign the span a parent.
     *
     * @param name       the name of the span. Sent to the tracing system
     * @param attributes
     */
    @Override
    public void startTrace(String name, Map<String, Object> attributes) {
        // If tracing has been disabled, return immediately
        var services = this.services;
        if (services == null) {
            return;
        }

        SpanBuilder spanBuilder = services.tracer.spanBuilder(name);
        setSpanAttributes(attributes, spanBuilder);
        spanBuilder.startSpan();
    }

    private static void updateThreadContext(ThreadContext threadContext, APMServices services, Context context) {
        // The new span context can be used as the parent context directly within the same Java process...
        threadContext.putTransient(Task.APM_TRACE_CONTEXT, context);

        // ...whereas for tasks sent to other ES nodes, we need to put trace HTTP headers into the threadContext so
        // that they can be propagated.
        services.openTelemetry.getPropagators().getTextMapPropagator().inject(context, threadContext, (tc, key, value) -> {
            if (isSupportedContextKey(key)) {
                tc.putHeader(key, value);
            }
        });
    }

    private Context getParentContext(ThreadContext threadContext) {
        // https://github.com/open-telemetry/opentelemetry-java/discussions/2884#discussioncomment-381870
        // If you just want to propagate across threads within the same process, you don't need context propagators (extract/inject).
        // You can just pass the Context object directly to another thread (it is immutable and thus thread-safe).

        // Attempt to fetch a local parent context first, otherwise look for a remote parent
        Context parentContext = threadContext.getTransient("parent_" + Task.APM_TRACE_CONTEXT);
        if (parentContext == null) {
            final String traceParentHeader = threadContext.getTransient("parent_" + Task.TRACE_PARENT_HTTP_HEADER);
            final String traceStateHeader = threadContext.getTransient("parent_" + Task.TRACE_STATE);

            if (traceParentHeader != null) {
                final Map<String, String> traceContextMap = Maps.newMapWithExpectedSize(2);
                // traceparent and tracestate should match the keys used by W3CTraceContextPropagator
                traceContextMap.put(Task.TRACE_PARENT_HTTP_HEADER, traceParentHeader);
                if (traceStateHeader != null) {
                    traceContextMap.put(Task.TRACE_STATE, traceStateHeader);
                }
                parentContext = services.openTelemetry.getPropagators()
                    .getTextMapPropagator()
                    .extract(Context.current(), traceContextMap, new MapKeyGetter());
            }
        }
        return parentContext;
    }

    /**
     * Most of the examples of how to use the OTel API look something like this, where the span context
     * is automatically propagated:
     *
     * <pre>{@code
     * Span span = tracer.spanBuilder("parent").startSpan();
     * try (Scope scope = parentSpan.makeCurrent()) {
     *     // ...do some stuff, possibly creating further spans
     * } finally {
     *     span.end();
     * }
     * }</pre>
     * This typically isn't useful in Elasticsearch, because a {@link Scope} can't be used across threads.
     * However, if a scope is active, then the APM agent can capture additional information, so this method
     * exists to make it possible to use scopes in the few situation where it makes sense.
     *
     * @param spanId the ID of a currently-open span for which to open a scope.
     * @return a method to close the scope when you are finished with it.
     */
    @Override
    public Releasable withScope(SpanId spanId) {
        final Context context = spans.get(spanId);
        if (context != null) {
            var scope = context.makeCurrent();
            return scope::close;
        }
        return () -> {};
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

        spanBuilder.setAttribute(org.elasticsearch.tracing.Tracer.AttributeKeys.NODE_NAME, nodeName);
        spanBuilder.setAttribute(org.elasticsearch.tracing.Tracer.AttributeKeys.CLUSTER_NAME, clusterName);
    }

    private void setSpanAttributes(ThreadContext threadContext, @Nullable Map<String, Object> spanAttributes, SpanBuilder spanBuilder) {
        setSpanAttributes(spanAttributes, spanBuilder);

        final String xOpaqueId = threadContext.getHeader(Task.X_OPAQUE_ID_HTTP_HEADER);
        if (xOpaqueId != null) {
            spanBuilder.setAttribute("es.x-opaque-id", xOpaqueId);
        }
    }

    @Override
    public void addError(SpanId spanId, Throwable throwable) {
        final var span = Span.fromContextOrNull(spans.get(spanId));
        if (span != null) {
            span.recordException(throwable);
        }
    }

    @Override
    public void setAttribute(SpanId spanId, String key, boolean value) {
        final var span = Span.fromContextOrNull(spans.get(spanId));
        if (span != null) {
            span.setAttribute(key, value);
        }
    }

    @Override
    public void setAttribute(SpanId spanId, String key, double value) {
        final var span = Span.fromContextOrNull(spans.get(spanId));
        if (span != null) {
            span.setAttribute(key, value);
        }
    }

    @Override
    public void setAttribute(SpanId spanId, String key, long value) {
        final var span = Span.fromContextOrNull(spans.get(spanId));
        if (span != null) {
            span.setAttribute(key, value);
        }
    }

    @Override
    public void setAttribute(SpanId spanId, String key, String value) {
        final var span = Span.fromContextOrNull(spans.get(spanId));
        if (span != null) {
            span.setAttribute(key, value);
        }
    }

    @Override
    public void stopTrace(SpanId spanId) {
        final var span = Span.fromContextOrNull(spans.remove(spanId));
        if (span != null) {
            logger.trace("Finishing trace [{}]", spanId);
            span.end();
        }
    }

    /**
     * Called when a span ends. This version of the method relies on context to select the span to stop.
     */
    @Override
    public void stopTrace() {
        Span.current().end();
    }

    @Override
    public void addEvent(SpanId spanId, String eventName) {
        final var span = Span.fromContextOrNull(spans.get(spanId));
        if (span != null) {
            span.addEvent(eventName);
        }
    }

    private static class MapKeyGetter implements TextMapGetter<Map<String, String>> {

        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet().stream().filter(APMTracer::isSupportedContextKey).collect(Collectors.toSet());
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier.get(key);
        }
    }

    private static boolean isSupportedContextKey(String key) {
        return Task.TRACE_PARENT_HTTP_HEADER.equals(key) || Task.TRACE_STATE.equals(key);
    }

    // VisibleForTesting
    Map<SpanId, Context> getSpans() {
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

        return new CharacterRunAutomaton(finalAutomaton);
    }

    private static Automaton patternsToAutomaton(List<String> patterns) {
        final List<Automaton> automata = patterns.stream().map(s -> {
            final String regex = s.replaceAll("\\.", "\\\\.").replaceAll("\\*", ".*");
            return new RegExp(regex).toAutomaton();
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
