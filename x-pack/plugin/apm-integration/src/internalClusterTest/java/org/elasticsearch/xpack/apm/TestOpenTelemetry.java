/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apm;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.ContextPropagators;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

// Shut up, IntelliJ
@SuppressWarnings("NullableProblems")
public class TestOpenTelemetry implements OpenTelemetry {

    public static final OpenTelemetry INSTANCE = new TestOpenTelemetry();

    private final Tracer tracer;

    public TestOpenTelemetry() {
        this.tracer = new TestTracer();
    }

    public Tracer getTracer() {
        return tracer;
    }

    @Override
    public TracerProvider getTracerProvider() {
        return new TracerProvider() {
            @Override
            public Tracer get(String instrumentationScopeName) {
                return tracer;
            }

            @Override
            public Tracer get(String instrumentationScopeName, String instrumentationScopeVersion) {
                return tracer;
            }
        };
    }

    @Override
    public Tracer getTracer(String instrumentationScopeName) {
        return this.tracer;
    }

    @Override
    public Tracer getTracer(String instrumentationScopeName, String instrumentationScopeVersion) {
        return this.tracer;
    }

    @Override
    public ContextPropagators getPropagators() {
        return ContextPropagators.noop();
    }

    class TestTracer implements Tracer {

        @Override
        public SpanBuilder spanBuilder(String spanName) {
            return new TestSpanBuilder(spanName);
        }
    }

    class TestSpanBuilder implements SpanBuilder {
        private final String spanName;
        private Context parentContext;
        private Map<String, Object> attributes = new HashMap<>();
        private SpanKind spanKind;
        private Long startTimestamp;

        TestSpanBuilder(String spanName) {
            this.spanName = spanName;
        }

        @Override
        public SpanBuilder setParent(Context context) {
            this.parentContext = context;
            return this;
        }

        @Override
        public SpanBuilder setNoParent() {
            this.parentContext = null;
            return this;
        }

        @Override
        public SpanBuilder addLink(SpanContext spanContext) {
            return this;
        }

        @Override
        public SpanBuilder addLink(SpanContext spanContext, Attributes attributes) {
            return this;
        }

        @Override
        public SpanBuilder setAttribute(String key, String value) {
            this.attributes.put(key, value);
            return this;
        }

        @Override
        public SpanBuilder setAttribute(String key, long value) {
            this.attributes.put(key, value);
            return this;
        }

        @Override
        public SpanBuilder setAttribute(String key, double value) {
            this.attributes.put(key, value);
            return this;
        }

        @Override
        public SpanBuilder setAttribute(String key, boolean value) {
            this.attributes.put(key, value);
            return this;
        }

        @Override
        public <T> SpanBuilder setAttribute(AttributeKey<T> key, T value) {
            this.attributes.put(key.getKey(), value);
            return this;
        }

        @Override
        public SpanBuilder setSpanKind(SpanKind spanKind) {
            this.spanKind = spanKind;
            return this;
        }

        @Override
        public SpanBuilder setStartTimestamp(long startTimestamp, TimeUnit unit) {
            this.startTimestamp = unit.toMillis(startTimestamp);
            return this;
        }

        @Override
        public Span startSpan() {
            if (this.startTimestamp == null) {
                this.startTimestamp = System.currentTimeMillis();
            }
            return new TestSpan(spanName, parentContext, attributes, spanKind, startTimestamp);
        }
    }

    class TestSpan implements Span {
        private String name;
        private final Context parentContext;
        private final Map<String, Object> attributes;
        private final SpanKind spanKind;
        private Throwable exception;
        private Long startTimestamp;
        private Long endTimestamp;

        TestSpan(String spanName, Context parentContext, Map<String, Object> attributes, SpanKind spanKind, Long startTimestamp) {
            this.name = spanName;
            this.parentContext = parentContext;
            this.attributes = attributes;
            this.spanKind = spanKind;
            this.startTimestamp = startTimestamp;
        }

        @Override
        public <T> Span setAttribute(AttributeKey<T> key, T value) {
            this.attributes.put(key.getKey(), value);
            return this;
        }

        @Override
        public Span addEvent(String name, Attributes attributes) {
            return this;
        }

        @Override
        public Span addEvent(String name, Attributes attributes, long timestamp, TimeUnit unit) {
            return this;
        }

        @Override
        public Span setStatus(StatusCode statusCode, String description) {
            return this;
        }

        @Override
        public Span recordException(Throwable exception, Attributes additionalAttributes) {
            this.exception = exception;
            return this;
        }

        @Override
        public Span updateName(String name) {
            this.name = name;
            return this;
        }

        @Override
        public void end() {
            this.endTimestamp = System.currentTimeMillis();
        }

        @Override
        public void end(long timestamp, TimeUnit unit) {
            this.endTimestamp = unit.toMillis(timestamp);
        }

        @Override
        public SpanContext getSpanContext() {
            return null;
        }

        @Override
        public boolean isRecording() {
            return this.endTimestamp != null;
        }
    }
}
