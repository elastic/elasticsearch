/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apm;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.logging.LoggingSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.plugins.TracingPlugin;

public class APMTracer extends AbstractLifecycleComponent implements TracingPlugin.Tracer {

    private static final Logger logger = LogManager.getLogger(APMTracer.class);

    private volatile Tracer tracer;

    public APMTracer() {}

    @Override
    protected void doStart() {
        SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(new LoggingSpanExporter()))
            .build();

        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
            .setTracerProvider(sdkTracerProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .build();
        tracer = openTelemetry.getTracer("elasticsearch", Version.CURRENT.toString());
        tracer.spanBuilder("startup").startSpan().end();
    }

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {}

    @Override
    public void trace(String something) {
        final Tracer tracer = this.tracer;
        if (tracer == null) {
            return;
        }
        final Span span = tracer.spanBuilder("something").startSpan();
        span.end();
    }
}
