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
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.TracerBuilder;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.context.propagation.ContextPropagators;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Adds local capture policy while preserving the standard native span, context, and scope APIs. */
final class PolicyOpenTelemetry implements OpenTelemetry {
    private static final ContextKey<Integer> DEPTH = ContextKey.named("elasticsearch.local_span_depth");
    private final APMTracingService service;
    private final TracerProvider provider = new TracerProvider() {
        @Override
        public Tracer get(String name) {
            return wrap(() -> service.delegate().getTracer(name));
        }

        @Override
        public Tracer get(String name, String version) {
            return wrap(() -> service.delegate().getTracer(name, version));
        }

        @Override
        public TracerBuilder tracerBuilder(String name) {
            return new TracerBuilder() {
                private String version;
                private String schema;

                @Override
                public TracerBuilder setInstrumentationVersion(String value) {
                    version = value;
                    return this;
                }

                @Override
                public TracerBuilder setSchemaUrl(String value) {
                    schema = value;
                    return this;
                }

                @Override
                public Tracer build() {
                    String builtVersion = version;
                    String builtSchema = schema;
                    return wrap(
                        () -> service.delegate()
                            .tracerBuilder(name)
                            .setInstrumentationVersion(builtVersion)
                            .setSchemaUrl(builtSchema)
                            .build()
                    );
                }
            };
        }
    };

    PolicyOpenTelemetry(APMTracingService service) {
        this.service = service;
    }

    @Override
    public TracerProvider getTracerProvider() {
        return provider;
    }

    @Override
    public MeterProvider getMeterProvider() {
        return MeterProvider.noop();
    }

    @Override
    public ContextPropagators getPropagators() {
        return ContextPropagators.create(W3CTraceContextPropagator.getInstance());
    }

    private Tracer wrap(Supplier<Tracer> supplier) {
        return new Tracer() {
            @Override
            public boolean isEnabled() {
                return service.isEnabled();
            }

            @Override
            public SpanBuilder spanBuilder(String name) {
                String spanName = name == null || name.trim().isEmpty() ? "<unspecified span name>" : name;
                return new Builder(supplier.get().spanBuilder(spanName), spanName);
            }
        };
    }

    /** Local capture decisions must not be mistaken for distributed sampler decisions. */
    private final class Builder implements SpanBuilder {
        private final SpanBuilder delegate;
        private final String name;
        private Context parent;

        Builder(SpanBuilder delegate, String name) {
            this.delegate = delegate;
            this.name = name;
        }

        @Override
        public SpanBuilder setParent(Context context) {
            if (context != null) {
                parent = context;
            }
            delegate.setParent(context);
            return this;
        }

        @Override
        public SpanBuilder setNoParent() {
            parent = Context.root();
            delegate.setNoParent();
            return this;
        }

        @Override
        public SpanBuilder addLink(SpanContext context) {
            delegate.addLink(context);
            return this;
        }

        @Override
        public SpanBuilder addLink(SpanContext context, Attributes attributes) {
            delegate.addLink(context, attributes);
            return this;
        }

        @Override
        public SpanBuilder setAttribute(String key, String value) {
            delegate.setAttribute(key, value);
            return this;
        }

        @Override
        public SpanBuilder setAttribute(String key, long value) {
            delegate.setAttribute(key, value);
            return this;
        }

        @Override
        public SpanBuilder setAttribute(String key, double value) {
            delegate.setAttribute(key, value);
            return this;
        }

        @Override
        public SpanBuilder setAttribute(String key, boolean value) {
            delegate.setAttribute(key, value);
            return this;
        }

        @Override
        public <Value> SpanBuilder setAttribute(AttributeKey<Value> key, Value value) {
            delegate.setAttribute(key, value);
            return this;
        }

        @Override
        public SpanBuilder setSpanKind(SpanKind kind) {
            delegate.setSpanKind(kind);
            return this;
        }

        @Override
        public SpanBuilder setStartTimestamp(long timestamp, TimeUnit unit) {
            delegate.setStartTimestamp(timestamp, unit);
            return this;
        }

        @Override
        public Span startSpan() {
            Context effectiveParent = parent == null ? Context.current() : parent;
            var parentSpan = Span.fromContext(effectiveParent).getSpanContext();
            Integer parentDepth = effectiveParent.get(DEPTH);
            int depth = parentSpan.isValid() && parentSpan.isRemote() == false ? (parentDepth == null ? 0 : parentDepth) + 1 : 0;
            if (service.shouldRecord(name, depth) == false) {
                return new LocalSpan(Span.wrap(parentSpan), parentDepth);
            }
            Span span = delegate.setParent(effectiveParent)
                .setAttribute("es.node.name", service.nodeName())
                .setAttribute("es.cluster.name", service.clusterName())
                .startSpan();
            return new LocalSpan(span, depth);
        }
    }

    /** Native activation carries depth without requiring callers to enter an Elasticsearch-specific scope. */
    private final class LocalSpan implements Span {
        private final Span delegate;
        private final Integer depth;

        LocalSpan(Span delegate, Integer depth) {
            this.delegate = delegate;
            this.depth = depth;
        }

        @Override
        public Context storeInContext(Context context) {
            return Span.super.storeInContext(context).with(DEPTH, depth);
        }

        @Override
        public <Value> Span setAttribute(AttributeKey<Value> key, Value value) {
            delegate.setAttribute(key, value);
            return this;
        }

        @Override
        public Span addEvent(String name, Attributes attributes) {
            delegate.addEvent(name, attributes);
            return this;
        }

        @Override
        public Span addEvent(String name, Attributes attributes, long timestamp, TimeUnit unit) {
            delegate.addEvent(name, attributes, timestamp, unit);
            return this;
        }

        @Override
        public Span setStatus(StatusCode code, String description) {
            delegate.setStatus(code, description);
            return this;
        }

        @Override
        public Span updateName(String name) {
            delegate.updateName(name);
            return this;
        }

        @Override
        public void end() {
            delegate.end();
        }

        @Override
        public void end(long timestamp, TimeUnit unit) {
            delegate.end(timestamp, unit);
        }

        @Override
        public SpanContext getSpanContext() {
            return delegate.getSpanContext();
        }

        @Override
        public boolean isRecording() {
            return delegate.isRecording();
        }

        @Override
        public Span recordException(Throwable failure, Attributes attributes) {
            if (failure != null && isRecording()) {
                if (service.recordExceptionStacks()) {
                    delegate.recordException(failure, attributes);
                } else {
                    var event = Attributes.builder().put("exception.type", failure.getClass().getName());
                    if (failure.getMessage() != null) {
                        event.put("exception.message", failure.getMessage());
                    }
                    delegate.addEvent("exception", event.putAll(attributes).build());
                }
            }
            return this;
        }

        @Override
        public Span addLink(SpanContext context, Attributes attributes) {
            delegate.addLink(context, attributes);
            return this;
        }
    }
}
