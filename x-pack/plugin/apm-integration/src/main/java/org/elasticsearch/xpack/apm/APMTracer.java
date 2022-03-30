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
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Traceable;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.common.settings.Setting.Property.Dynamic;
import static org.elasticsearch.common.settings.Setting.Property.NodeScope;
import static org.elasticsearch.xpack.apm.APM.TRACE_HEADERS;

public class APMTracer extends AbstractLifecycleComponent implements org.elasticsearch.tracing.Tracer {

    private static final Logger LOGGER = LogManager.getLogger(APMTracer.class);

    static final Setting<Boolean> APM_ENABLED_SETTING = Setting.boolSetting("xpack.apm.tracing.enabled", false, Dynamic, NodeScope);
    static final Setting<List<String>> APM_TRACING_NAMES_INCLUDE_SETTING = Setting.listSetting(
        "xpack.apm.tracing.names.include",
        Collections.emptyList(),
        Function.identity(),
        Dynamic,
        NodeScope
    );

    private final Map<String, Span> spans = ConcurrentCollections.newConcurrentMap();
    private final ThreadPool threadPool;
    private final ClusterService clusterService;

    private volatile boolean enabled;
    private volatile APMServices services;

    private List<String> includeNames;

    /**
     * This class is required to make all open telemetry services visible at once
     */
    private record APMServices(Tracer tracer, OpenTelemetry openTelemetry) {}

    public APMTracer(Settings settings, ThreadPool threadPool, ClusterService clusterService) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.enabled = APM_ENABLED_SETTING.get(settings);
        this.includeNames = APM_TRACING_NAMES_INCLUDE_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(APM_ENABLED_SETTING, this::setEnabled);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(APM_TRACING_NAMES_INCLUDE_SETTING, this::setIncludeNames);
    }

    public boolean isEnabled() {
        return enabled;
    }

    private void setEnabled(boolean enabled) {
        this.enabled = enabled;
        if (enabled) {
            createApmServices();
        } else {
            destroyApmServices();
        }
    }

    private void setIncludeNames(List<String> includeNames) {
        this.includeNames = includeNames;
    }

    @Override
    protected void doStart() {
        if (enabled) {
            createApmServices();
        }
    }

    @Override
    protected void doStop() {
        destroyApmServices();
    }

    @Override
    protected void doClose() {

    }

    private void createApmServices() {
        assert this.enabled;
        assert this.services == null;

        this.services = AccessController.doPrivileged((PrivilegedAction<APMServices>) () -> {
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
        var services = this.services;
        if (services == null) {
            return;
        }

        if (isSpanNameIncluded(traceable.getSpanName()) == false) {
            return;
        }

        spans.computeIfAbsent(traceable.getSpanId(), spanId -> AccessController.doPrivileged((PrivilegedAction<Span>) () -> {
            // services might be in shutdown state by this point, but this is handled by the open telemetry internally
            final SpanBuilder spanBuilder = services.tracer.spanBuilder(traceable.getSpanName());
            Context parentContext = getParentSpanContext();
            if (parentContext != null) {
                spanBuilder.setParent(parentContext);
            }

            for (Map.Entry<String, Object> entry : traceable.getAttributes().entrySet()) {
                final Object value = entry.getValue();
                if (value instanceof String) {
                    spanBuilder.setAttribute(entry.getKey(), (String) value);
                } else if (value instanceof Long) {
                    spanBuilder.setAttribute(entry.getKey(), (Long) value);
                } else if (value instanceof Integer) {
                    spanBuilder.setAttribute(entry.getKey(), (Integer) value);
                } else if (value instanceof Double) {
                    spanBuilder.setAttribute(entry.getKey(), (Double) value);
                } else if (value instanceof Boolean) {
                    spanBuilder.setAttribute(entry.getKey(), (Boolean) value);
                } else {
                    throw new IllegalArgumentException(
                        "span attributes do not support value type of [" + value.getClass().getCanonicalName() + "]"
                    );
                }
            }

            // These attributes don't apply to HTTP spans. The APM server can infer a number of things
            // when "http." attributes are present
            if (traceable.getAttributes().keySet().stream().anyMatch(key -> key.startsWith("http.")) == false) {
                // hack transactions to avoid the 'custom' transaction type
                // this one is not part of OTel semantic attributes
                spanBuilder.setAttribute("type", "elasticsearch");
                // hack spans to avoid the 'app' span.type, will make it use external/elasticsearch
                // also allows to set destination resource name in map
                spanBuilder.setAttribute(SemanticAttributes.MESSAGING_SYSTEM, "elasticsearch");
                spanBuilder.setAttribute(SemanticAttributes.MESSAGING_DESTINATION, clusterService.getNodeName());
            }

            // spanBuilder.setAttribute(SemanticAttributes.DB_SYSTEM, "elasticsearch");
            // spanBuilder.setAttribute(SemanticAttributes.DB_NAME, clusterService.getNodeName());

            // this will duplicate the "resource attributes" that are defined globally
            // but providing them as span attributes allow easier mapping through labels as otel attributes are stored as-is only in
            // 7.16.
            spanBuilder.setAttribute(Traceable.AttributeKeys.NODE_NAME, clusterService.getNodeName());
            spanBuilder.setAttribute(Traceable.AttributeKeys.CLUSTER_NAME, clusterService.getClusterName().value());

            final String xOpaqueId = threadPool.getThreadContext().getHeader(Task.X_OPAQUE_ID_HTTP_HEADER);
            if (xOpaqueId != null) {
                spanBuilder.setAttribute("es.x-opaque-id", xOpaqueId);
            }
            final Span span = spanBuilder.startSpan();

            final Map<String, String> spanHeaders = new HashMap<>();
            final Context contextForNewSpan = Context.current().with(span);
            services.openTelemetry.getPropagators().getTextMapPropagator().inject(contextForNewSpan, spanHeaders, Map::put);
            spanHeaders.keySet().removeIf(k -> isSupportedContextKey(k) == false);

            threadContext.putHeader(spanHeaders);

            // logGraphviz(span);

            return span;
        }));
    }

    private static final Set<String> CACHE = new HashSet<>();

    @Override
    public void onTraceException(Traceable traceable, Throwable throwable) {
        final var span = spans.get(traceable.getSpanId());
        if (span != null) {
            span.recordException(throwable);
        }
    }

    @Override
    public void setAttribute(Traceable traceable, String key, boolean value) {
        final var span = spans.get(traceable.getSpanId());
        if (span != null) {
            span.setAttribute(key, value);
        }
    }

    @Override
    public void setAttribute(Traceable traceable, String key, double value) {
        final var span = spans.get(traceable.getSpanId());
        if (span != null) {
            span.setAttribute(key, value);
        }
    }

    @Override
    public void setAttribute(Traceable traceable, String key, long value) {
        final var span = spans.get(traceable.getSpanId());
        if (span != null) {
            span.setAttribute(key, value);
        }
    }

    @Override
    public void setAttribute(Traceable traceable, String key, String value) {
        final var span = spans.get(traceable.getSpanId());
        if (span != null) {
            span.setAttribute(key, value);
        }
    }

    private boolean isSpanNameIncluded(String name) {
        // Alternatively we could use automata here but it is much more complex
        // and it needs wrapping like done for use in the security plugin.
        return includeNames.isEmpty() || Regex.simpleMatch(includeNames, name);
    }

    private Context getParentSpanContext() {
        // Check for a parent context in the thread context.
        final ThreadContext threadContext = threadPool.getThreadContext();
        final String traceParentHeader = threadContext.getTransient("parent_" + Task.TRACE_PARENT_HTTP_HEADER);
        final String traceStateHeader = threadContext.getTransient("parent_" + Task.TRACE_STATE);

        if (traceParentHeader != null) {
            final Map<String, String> traceContextMap = new HashMap<>(2);
            // traceparent and tracestate should match the keys used by W3CTraceContextPropagator
            traceContextMap.put(Task.TRACE_PARENT_HTTP_HEADER, traceParentHeader);
            if (traceStateHeader != null) {
                traceContextMap.put(Task.TRACE_STATE, traceStateHeader);
            }
            return services.openTelemetry.getPropagators()
                .getTextMapPropagator()
                .extract(Context.current(), traceContextMap, new MapKeyGetter());
        }
        return null;
    }

    @Override
    public void onTraceStopped(Traceable traceable) {
        final var span = spans.remove(traceable.getSpanId());
        if (span != null) {
            span.end();
        }
    }

    @Override
    public void onTraceEvent(Traceable traceable, String eventName) {
        final var span = spans.get(traceable.getSpanId());
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
        return TRACE_HEADERS.contains(key);
    }

    private static void logGraphviz(Span span) {
        final String spanStr = span.toString();

        int i = spanStr.indexOf("spanId=");
        int j = spanStr.indexOf(",", i);
        String spanId = spanStr.substring(i + 7, j);

        String parentSpanId = null;
        i = spanStr.indexOf("spanId=", j);
        if (i > -1) {
            j = spanStr.indexOf(",", i);
            parentSpanId = spanStr.substring(i + 7, j);
        }

        i = spanStr.indexOf("name=", j);
        j = spanStr.indexOf(",", i);
        String spanName = spanStr.substring(i + 5, j);

        if (CACHE.add(spanId)) {
            Map<String, String> attrs = new HashMap<>();
            attrs.put("label", spanName);
            if (spanName.startsWith("internal:")) {
                attrs.put("style", "filled");
                attrs.put("fillcolor", "pink");
            }
            final String attrsString = attrs.entrySet()
                .stream()
                .map(each -> each.getKey() + "=\"" + each.getValue() + "\"")
                .collect(Collectors.joining(","));
            LOGGER.warn("BADGER: __{} [{}]", spanId, attrsString);
        }

        if (parentSpanId != null) {
            LOGGER.warn("BADGER: __{} -> __{}", spanId, parentSpanId);
        }

    }
}
