/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apm;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tracing.Traceable;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.apm.APMAgentSettings.APM_ENABLED_SETTING;
import static org.elasticsearch.xpack.apm.APMAgentSettings.APM_TRACING_NAMES_EXCLUDE_SETTING;
import static org.elasticsearch.xpack.apm.APMAgentSettings.APM_TRACING_NAMES_INCLUDE_SETTING;

public class APMTracer extends AbstractLifecycleComponent implements org.elasticsearch.tracing.Tracer {

    private static final Logger LOGGER = LogManager.getLogger(APMTracer.class);

    private final Map<String, Context> spans = ConcurrentCollections.newConcurrentMap();
    private final ClusterService clusterService;

    private volatile boolean enabled;
    private volatile APMServices services;

    private List<String> includeNames;
    private List<String> excludeNames;
    private volatile CharacterRunAutomaton filterAutomaton;

    /**
     * This class is required to make all open telemetry services visible at once
     */
    record APMServices(Tracer tracer, OpenTelemetry openTelemetry) {}

    public APMTracer(Settings settings, ClusterService clusterService) {
        this.includeNames = APM_TRACING_NAMES_INCLUDE_SETTING.get(settings);
        this.excludeNames = APM_TRACING_NAMES_EXCLUDE_SETTING.get(settings);
        this.filterAutomaton = buildAutomaton(includeNames, excludeNames);
        this.enabled = APM_ENABLED_SETTING.get(settings);
        this.clusterService = clusterService;
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

    private APMServices createApmServices() {
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
    public void onTraceStarted(ThreadContext threadContext, Traceable traceable) {
        assert threadContext != null;
        assert traceable != null;

        // If tracing has been disabled, return immediately
        var services = this.services;
        if (services == null) {
            return;
        }

        if (filterAutomaton.run(traceable.getSpanName()) == false) {
            return;
        }

        spans.computeIfAbsent(traceable.getSpanId(), spanId -> AccessController.doPrivileged((PrivilegedAction<Context>) () -> {
            final SpanBuilder spanBuilder = services.tracer.spanBuilder(traceable.getSpanName());

            final Context parentContext = getParentContext(threadContext);
            if (parentContext != null) {
                spanBuilder.setParent(parentContext);
            }

            setSpanAttributes(threadContext, traceable, spanBuilder);
            final Span span = spanBuilder.startSpan();
            final Context contextForNewSpan = Context.current().with(span);

            final Map<String, String> spanHeaders = new HashMap<>();
            services.openTelemetry.getPropagators().getTextMapPropagator().inject(contextForNewSpan, spanHeaders, Map::put);
            spanHeaders.keySet().removeIf(k -> isSupportedContextKey(k) == false);

            // The span context can be used as the parent context directly within the same Java process
            threadContext.putTransient(Task.APM_TRACE_CONTEXT, contextForNewSpan);
            // Whereas for tasks sent to other ES nodes, we need to put trace headers into the threadContext so that they can be
            // propagated
            threadContext.putHeader(spanHeaders);

            return contextForNewSpan;
        }));
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
                final Map<String, String> traceContextMap = new HashMap<>(2);
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

    @Override
    public Releasable withScope(Traceable traceable) {
        final Context context = spans.get(traceable.getSpanId());
        if (context != null) {
            var scope = context.makeCurrent();
            return scope::close;
        }
        return () -> {};
    }

    private void setSpanAttributes(ThreadContext threadContext, Traceable traceable, SpanBuilder spanBuilder) {
        final Map<String, Object> spanAttributes = traceable.getAttributes();

        for (Map.Entry<String, Object> entry : spanAttributes.entrySet()) {
            final String key = entry.getKey();
            final Object value = entry.getValue();
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

        spanBuilder.setAttribute(Traceable.AttributeKeys.NODE_NAME, clusterService.getNodeName());
        spanBuilder.setAttribute(Traceable.AttributeKeys.CLUSTER_NAME, clusterService.getClusterName().value());

        final String xOpaqueId = threadContext.getHeader(Task.X_OPAQUE_ID_HTTP_HEADER);
        if (xOpaqueId != null) {
            spanBuilder.setAttribute("es.x-opaque-id", xOpaqueId);
        }
    }

    @Override
    public void onTraceException(Traceable traceable, Throwable throwable) {
        final var span = Span.fromContextOrNull(spans.get(traceable.getSpanId()));
        if (span != null) {
            span.recordException(throwable);
        }
    }

    @Override
    public void setAttribute(Traceable traceable, String key, boolean value) {
        final var span = Span.fromContextOrNull(spans.get(traceable.getSpanId()));
        if (span != null) {
            span.setAttribute(key, value);
        }
    }

    @Override
    public void setAttribute(Traceable traceable, String key, double value) {
        final var span = Span.fromContextOrNull(spans.get(traceable.getSpanId()));
        if (span != null) {
            span.setAttribute(key, value);
        }
    }

    @Override
    public void setAttribute(Traceable traceable, String key, long value) {
        final var span = Span.fromContextOrNull(spans.get(traceable.getSpanId()));
        if (span != null) {
            span.setAttribute(key, value);
        }
    }

    @Override
    public void setAttribute(Traceable traceable, String key, String value) {
        final var span = Span.fromContextOrNull(spans.get(traceable.getSpanId()));
        if (span != null) {
            span.setAttribute(key, value);
        }
    }

    @Override
    public void onTraceStopped(Traceable traceable) {
        final var span = Span.fromContextOrNull(spans.remove(traceable.getSpanId()));
        if (span != null) {
            span.end();
        }
    }

    @Override
    public void onTraceEvent(Traceable traceable, String eventName) {
        final var span = Span.fromContextOrNull(spans.get(traceable.getSpanId()));
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
    Map<String, Context> getSpans() {
        return spans;
    }

    static CharacterRunAutomaton buildAutomaton(List<String> includeNames, List<String> excludeNames) {
        Automaton includeAutomaton = patternsToAutomaton(includeNames);
        Automaton excludeAutomaton = patternsToAutomaton(excludeNames);

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
