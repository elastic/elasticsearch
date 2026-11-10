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
import static java.util.Objects.requireNonNullElseGet;

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
        if (parentSpan instanceof APMSpan apmSpan) {
            this.parentContext = apmSpan.context();
            // the last call wins, as it does in the OTel API
            this.noParent = false;
        }
        return this;
    }

    @Override
    public SpanBuilder setNoParent() {
        this.parentContext = emptyContext();
        this.noParent = true;
        return this;
    }

    public APMSpanBuilder setSpanKind(SpanKind spanKind) {
        this.spanKind = spanKind;
        return this;
    }

    @Override
    public Span startSpan() {
        // The span is returned even if it is not sampled, with the context that stands for that decision. This is what makes the
        // decision part of what is propagated to the descendants of the span, both local and remote ones, when it is stored in a thread
        // context.
        return new APMSpan(tracer, startSpanContext());
    }

    private Context startSpanContext() {
        if (tracer.isEnabled() == false) {
            // invalid span will break the propagation chain, which is what we want in this case
            return emptyContext().with(io.opentelemetry.api.trace.Span.getInvalid());
        }

        final var threadContext = tracer.threadContext();
        final Context localParentContext = parentContext != null
            ? parentContext
            : requireNonNullElseGet(threadContext.getTransient(Task.APM_TRACE_CONTEXT), APMSpanBuilder::emptyContext);
        var localParentSpan = io.opentelemetry.api.trace.Span.fromContextOrNull(localParentContext);

        // local parent span is not sampled, propagate its context to disable any further child spans
        if (localParentSpan != null && localParentSpan.getSpanContext().isSampled() == false) {
            logger.trace("Skipping span [{}] as its local trace was dropped", name);
            return localParentContext;
        }

        // todo: this will be moved to a sampler
        if (tracer.isFilteredOut(name)) {
            logger.trace("Skipping tracing [{}] as it has been filtered out", name);
            return contextWithInvalidSpan(localParentContext);
        }

        final var localDepth = LocalTraceDepth.get(localParentContext);
        final var localSpanCount = LocalSpanCount.getOrNull(localParentContext);
        if (localSpanCount == null) {
            assert false : "LocalSpanCount should always be populated, even in empty context";
            return contextWithInvalidSpan(localParentContext);
        }

        // todo: this will be moved to a sampler
        final int maxTraceDepth = tracer.maxTraceDepth();
        if (localDepth.depth() > maxTraceDepth) {
            logger.trace("Skipping span [{}] at local trace depth {} (maxTraceDepth={})", name, localDepth, maxTraceDepth);
            return contextWithInvalidSpan(localParentContext);
        }

        // todo: this will be moved to a sampler
        final int maxSpans = tracer.maxSpans();
        if (localDepth.depth() > 0 && localSpanCount.incrementAndGet() > maxSpans) {
            logger.trace("Skipping span [{}] as the local trace reached maxSpans={}", name, maxSpans);
            return contextWithInvalidSpan(localParentContext);
        }

        final io.opentelemetry.api.trace.SpanBuilder otelSpanBuilder = tracer.otelTracer().spanBuilder(name).setSpanKind(spanKind);

        // A span can have a parent span, which is important for seeing a complete trace in the APM UI.
        // Try to use the local parent first, otherwise look for a remote parent in the request headers; otherwise use emptyContext().
        // A span that was explicitly started without a parent is the root of a new trace: nothing is inherited, not even a remote parent.
        final Context newSpanParent;
        if (noParent) {
            newSpanParent = Context.root();
            otelSpanBuilder.setNoParent();
        } else {
            newSpanParent = localParentSpan != null ? localParentContext : getRemoteParentContext();
            otelSpanBuilder.setParent(newSpanParent);
        }

        if (attributesBuilder != null) {
            otelSpanBuilder.setAllAttributes(attributesBuilder.build());
        }

        setCommonAttributes(otelSpanBuilder);

        if (localParentSpan == null) {
            Instant startTime = threadContext.removeTransient(Task.LOCAL_TRACE_START_TIME);
            if (startTime != null) {
                otelSpanBuilder.setStartTimestamp(startTime);
            }
        }

        final io.opentelemetry.api.trace.Span otelSpan = otelSpanBuilder.startSpan();
        final Context contextForNewSpan = newSpanParent.with(otelSpan).with(localDepth.increment());

        logger.trace("Starting span [{}]", name);
        return contextForNewSpan;
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

    /** Returns an OTel {@link Context} that contains an invalid span instance; this prevents child spans from being started. */
    private static Context contextWithInvalidSpan(Context context) {
        return context.with(io.opentelemetry.api.trace.Span.getInvalid());
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
}
