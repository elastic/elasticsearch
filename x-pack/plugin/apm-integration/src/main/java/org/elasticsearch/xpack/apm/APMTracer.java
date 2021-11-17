/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apm;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.plugins.TracingPlugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class APMTracer extends AbstractLifecycleComponent implements TracingPlugin.Tracer {

    public static final CapturingSpanExporter CAPTURING_SPAN_EXPORTER = new CapturingSpanExporter();

    static final Setting<SecureString> APM_ENDPOINT_SETTING = SecureSetting.secureString("xpack.apm.endpoint", null);
    static final Setting<SecureString> APM_TOKEN_SETTING = SecureSetting.secureString("xpack.apm.token", null);

    private final Map<String, Span> spans = ConcurrentCollections.newConcurrentMap();
    private final ClusterService clusterService;
    private final SecureString endpoint;
    private final SecureString token;

    private volatile SdkTracerProvider provider;
    private volatile Tracer tracer;
    private volatile OpenTelemetry openTelemetry;
    private final ThreadPool threadPool;

    public APMTracer(Settings settings, ThreadPool threadPool, ClusterService clusterService) {
        this.endpoint = APM_ENDPOINT_SETTING.get(settings);
        this.token = APM_TOKEN_SETTING.get(settings);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.clusterService = Objects.requireNonNull(clusterService);
    }

    @Override
    protected void doStart() {
        final String nodeName = clusterService.getNodeName();
        final String endpoint = this.endpoint.toString();
        final String token = this.token.toString();

        this.provider = AccessController.doPrivileged(
            (PrivilegedAction<SdkTracerProvider>) () -> SdkTracerProvider.builder()
                .setResource(
                    Resource.create(
                        Attributes.of(
                            ResourceAttributes.SERVICE_NAME,
                            nodeName,
                            ResourceAttributes.SERVICE_VERSION,
                            Version.CURRENT.toString(),
                            ResourceAttributes.DEPLOYMENT_ENVIRONMENT,
                            "dev"
                        )
                    )
                )
                .addSpanProcessor(createSpanProcessor(endpoint, token))
                .build()
        );

        openTelemetry = OpenTelemetrySdk.builder()
            .setTracerProvider(provider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .build();
        tracer = openTelemetry.getTracer("elasticsearch", Version.CURRENT.toString());
    }

    @Override
    protected void doStop() {
        final SdkTracerProvider provider = this.provider;
        if (provider != null) {
            provider.shutdown().join(30L, TimeUnit.SECONDS);
        }
    }

    @Override
    protected void doClose() {}

    @Override
    public void onTraceStarted(TracingPlugin.Traceable traceable) {
        final Tracer tracer = this.tracer;
        final OpenTelemetry openTelemetry = this.openTelemetry;
        if (openTelemetry != null && tracer != null) {
            spans.computeIfAbsent(traceable.getSpanId(), spanId -> {
                final SpanBuilder spanBuilder = tracer.spanBuilder(traceable.getSpanName());
                Context parentContext = getParentSpanContext(openTelemetry);
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
                return spanBuilder.startSpan();
            });
        }
    }

    private Context getParentSpanContext(OpenTelemetry openTelemetry) {
        // If we already have a non-root span context that should be the parent
        if (Context.current() != Context.root()) {
            return Context.current();
        }

        // If not let us check for a parent context in the thread context
        String traceParent = threadPool.getThreadContext().getHeader(Task.TRACE_PARENT);
        String traceState = threadPool.getThreadContext().getHeader(Task.TRACE_STATE);
        if (traceParent != null) {
            Map<String, String> traceContextMap = new HashMap<>();
            // traceparent and tracestate should match the keys used by W3CTraceContextPropagator
            traceContextMap.put(Task.TRACE_PARENT, traceParent);
            if (traceState != null) {
                traceContextMap.put(Task.TRACE_STATE, traceState);
            }
            return openTelemetry.getPropagators().getTextMapPropagator().extract(Context.current(), traceContextMap, new MapKeyGetter());
        }
        return null;
    }

    public Map<String, String> getSpanHeadersById(String id) {
        var span = spans.get(id);
        if (span == null) {
            return null;
        }
        try (Scope scope = span.makeCurrent()) {
            Map<String, String> spanHeaders = new HashMap<>();
            openTelemetry.getPropagators().getTextMapPropagator().inject(Context.current(), spanHeaders, Map::put);
            spanHeaders.keySet().removeIf(k -> isSupportedContextKey(k) == false);
            return spanHeaders;
        }
    }

    @Override
    public void onTraceStopped(TracingPlugin.Traceable traceable) {
        final Span span = spans.remove(traceable.getSpanId());
        if (span != null) {
            span.end();
        }
    }

    private static SpanProcessor createSpanProcessor(String endpoint, String token) {
        SpanProcessor processor = SimpleSpanProcessor.create(CAPTURING_SPAN_EXPORTER);
        if (Strings.hasLength(endpoint) == false || Strings.hasLength(token) == false) {
            return processor;
        }

        final OtlpGrpcSpanExporter exporter = AccessController.doPrivileged(
            (PrivilegedAction<OtlpGrpcSpanExporter>) () -> OtlpGrpcSpanExporter.builder()
                .setEndpoint(endpoint)
                .addHeader("Authorization", "Bearer " + token)
                .build()
        );
        return SpanProcessor.composite(
            processor,
            AccessController.doPrivileged(
                (PrivilegedAction<BatchSpanProcessor>) () -> BatchSpanProcessor.builder(exporter)
                    .setScheduleDelay(100, TimeUnit.MILLISECONDS)
                    .build()
            )
        );
    }

    public static class CapturingSpanExporter implements SpanExporter {

        private Queue<SpanData> capturedSpans = ConcurrentCollections.newQueue();

        public void clear() {
            capturedSpans.clear();
        }

        public List<SpanData> getCapturedSpans() {
            return List.copyOf(capturedSpans);
        }

        public Stream<SpanData> findSpan(Predicate<SpanData> predicate) {
            return getCapturedSpans().stream().filter(predicate);
        }

        public Stream<SpanData> findSpanByName(String name) {
            return findSpan(span -> Objects.equals(span.getName(), name));
        }

        public Stream<SpanData> findSpanBySpanId(String spanId) {
            return findSpan(span -> Objects.equals(span.getSpanId(), spanId));
        }

        public Stream<SpanData> findSpanByParentSpanId(String parentSpanId) {
            return findSpan(span -> Objects.equals(span.getParentSpanId(), parentSpanId));
        }

        @Override
        public CompletableResultCode export(Collection<SpanData> spans) {
            capturedSpans.addAll(spans);
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode flush() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode shutdown() {
            return CompletableResultCode.ofSuccess();
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
        return APM.TRACE_HEADERS.contains(key);
    }
}
