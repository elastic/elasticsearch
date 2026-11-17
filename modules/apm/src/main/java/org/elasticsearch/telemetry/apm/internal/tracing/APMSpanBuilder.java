/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.tracing;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.tracing.AttributeKeys;
import org.elasticsearch.telemetry.tracing.Span;
import org.elasticsearch.telemetry.tracing.SpanBuilder;

import java.time.Instant;

import static java.util.Objects.requireNonNull;
import static org.elasticsearch.telemetry.apm.internal.tracing.AbstractSpan.isSpanRecording;

public class APMSpanBuilder implements SpanBuilder {

    private static final Logger logger = LogManager.getLogger(APMSpanBuilder.class);

    private static final String REDACTED = "[REDACTED]";

    private final APMTracer tracer;
    private final String name;

    @Nullable
    private Context parentContext;
    private boolean noParent = false;
    @Nullable
    private AttributesBuilder attributesBuilder;
    private SpanKind spanKind = SpanKind.INTERNAL;

    APMSpanBuilder(APMTracer tracer, String name) {
        this.tracer = tracer;
        this.name = name;
    }

    @Override
    public SpanBuilder setAttribute(String key, boolean value) {
        if (tracer.isRedacted(key)) {
            attributesBuilder().put(key, REDACTED);
        } else {
            attributesBuilder().put(key, value);
        }
        return this;
    }

    @Override
    public SpanBuilder setAttribute(String key, double value) {
        if (tracer.isRedacted(key)) {
            attributesBuilder().put(key, REDACTED);
        } else {
            attributesBuilder().put(key, value);
        }
        return this;
    }

    @Override
    public SpanBuilder setAttribute(String key, long value) {
        if (tracer.isRedacted(key)) {
            attributesBuilder().put(key, REDACTED);
        } else {
            attributesBuilder().put(key, value);
        }
        return this;
    }

    @Override
    public SpanBuilder setAttribute(String key, String value) {
        if (value == null) {
            assert false : "attribute " + key + " is null";
            return this;
        }
        if (tracer.isRedacted(key)) {
            attributesBuilder().put(key, REDACTED);
        } else {
            attributesBuilder().put(key, value);
        }
        return this;
    }

    public AttributesBuilder attributesBuilder() {
        if (attributesBuilder == null) {
            attributesBuilder = Attributes.builder();
        }
        return attributesBuilder;
    }

    @Override
    public SpanBuilder setParent(Span parentSpan) {
        if (parentSpan instanceof AbstractSpan abstractSpan) {
            this.parentContext = abstractSpan.context();
            // the last call wins, as it does in the OTel API
            this.noParent = false;
        }
        return this;
    }

    @Override
    public SpanBuilder setNoParent() {
        this.noParent = true;
        return this;
    }

    public APMSpanBuilder setSpanKind(SpanKind spanKind) {
        this.spanKind = spanKind;
        return this;
    }

    @Override
    public AbstractSpan startSpan() {
        Context parentOtelContext;
        if (noParent) {
            parentOtelContext = emptyContext();
        } else if (parentContext != null) {
            parentOtelContext = parentContext;
        } else if (tracer.currentSpan() instanceof AbstractSpan currentSpan) {
            parentOtelContext = currentSpan.context();
        } else {
            parentOtelContext = getRemoteParentContext();
        }

        var parentOtelSpan = io.opentelemetry.api.trace.Span.fromContextOrNull(parentOtelContext);

        // local parent span is not sampled, propagate its context to disable any further child spans
        if (parentOtelSpan != null && parentOtelSpan.getSpanContext().isSampled() == false) {
            logger.trace("Skipping span [{}] as its local trace was dropped", name);
            return readOnlySpan(parentOtelContext);
        }

        if (tracer.isFilteredOut(name)) {
            logger.trace("Skipping tracing [{}] as it has been filtered out", name);
            return readOnlySpan(parentOtelContext);
        }

        final var localDepth = LocalTraceDepth.get(parentOtelContext);
        final var localSpanCount = LocalSpanCount.getOrNull(parentOtelContext);
        if (localSpanCount == null) {
            assert false : "LocalSpanCount should always be populated, even in empty context";
            return readOnlySpan(parentOtelContext);
        }

        final int maxTraceDepth = tracer.maxTraceDepth();
        if (localDepth.depth() > maxTraceDepth) {
            logger.trace("Skipping span [{}] at local trace depth {} (maxTraceDepth={})", name, localDepth, maxTraceDepth);
            return readOnlySpan(parentOtelContext);
        }

        final int maxSpans = tracer.maxSpans();
        if (localSpanCount.incrementAndGet() > maxSpans) {
            logger.trace("Skipping span [{}] as the local trace reached maxSpans={}", name, maxSpans);
            return readOnlySpan(parentOtelContext);
        }

        final io.opentelemetry.api.trace.SpanBuilder otelSpanBuilder = tracer.otelTracer()
            .spanBuilder(name)
            .setSpanKind(spanKind)
            .setParent(parentOtelContext);

        if (attributesBuilder != null) {
            otelSpanBuilder.setAllAttributes(attributesBuilder.build());
        }

        setCommonAttributes(otelSpanBuilder);

        if (parentOtelSpan != null == false) {
            Instant startTime = tracer.threadContext().removeTransient(Task.LOCAL_TRACE_START_TIME);
            if (startTime != null) {
                otelSpanBuilder.setStartTimestamp(startTime);
            }
        }

        final io.opentelemetry.api.trace.Span otelSpan = otelSpanBuilder.startSpan();
        final Context contextForNewSpan = parentOtelContext.with(otelSpan).with(localDepth.increment());

        logger.trace("Starting span [{}]", name);
        return isSpanRecording(contextForNewSpan) ? new APMSpan(tracer, contextForNewSpan) : readOnlySpan(contextForNewSpan);
    }

    private void setCommonAttributes(io.opentelemetry.api.trace.SpanBuilder spanBuilder) {
        spanBuilder.setAttribute(AttributeKeys.NODE_NAME, tracer.nodeName());
        spanBuilder.setAttribute(AttributeKeys.CLUSTER_NAME, tracer.clusterName());

        final var threadContext = tracer.threadContext();
        final String xOpaqueId = threadContext.getHeader(Task.X_OPAQUE_ID_HTTP_HEADER);
        if (xOpaqueId != null) {
            spanBuilder.setAttribute("es.x-opaque-id", xOpaqueId);
        }

        final String projectId = threadContext.getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER);
        if (projectId != null) {
            spanBuilder.setAttribute("project.id", projectId);
        }
    }

    private static Context emptyContext() {
        return Context.root().with(new LocalSpanCount());
    }

    /**
     * The remote parent of a span that starts a local trace: the context that was propagated to this node in the request headers, or
     * {@link #emptyContext()} if there is none.
     */
    private Context getRemoteParentContext() {
        return tracer.openTelemetry()
            .getPropagators()
            .getTextMapPropagator()
            .extract(emptyContext(), tracer.threadContext(), (tc, key) -> requireNonNull(tc).getHeader(key));
    }

    /**
     * Return a read only wrapper of the OTel span in {@code parentOtelContext}. The read only span can be propagated across threads or
     * nodes, but cannot record any new observation. It preserves the sampling state present in passed {@code parentOtelContext}.
     *
     * <p>For example:
     * <ul>
     *     <li>A not sampled span will not record anything, and pass its sampling decision down the trace tree.
     *     <li>A child span was not created because of local trace limits (e.g. {@code max_spans}): the parent span context is passed down,
     *         but the instrumentation that creates the child span must not be able to overwrite parent span attributes/fields; at the same
     *         time, it must be able to propagate the trace context to another node.
     * </ul>
     */
    private PropagatedSpan readOnlySpan(Context parentOtelContext) {
        return new PropagatedSpan(tracer, parentOtelContext);
    }
}
