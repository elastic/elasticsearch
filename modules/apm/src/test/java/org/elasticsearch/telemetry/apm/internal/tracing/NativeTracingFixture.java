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
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.apm.internal.export.TraceSupplier;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.SanitizingSpanExporter;

/** Runs the production policy and privacy boundaries against a real SDK without an external collector. */
public final class NativeTracingFixture implements AutoCloseable {
    public final InMemorySpanExporter exporter = InMemorySpanExporter.create();
    public final APMTracingService service;
    public final OpenTelemetrySdk sdk;
    public final OpenTelemetry api;

    public NativeTracingFixture(Settings settings) {
        this(settings, Sampler.parentBased(Sampler.alwaysOn()));
    }

    public NativeTracingFixture(Settings settings, Sampler sampler) {
        service = new APMTracingService(
            Settings.builder().put("telemetry.tracing.enabled", true).put("telemetry.tracing.max_depth", 10).put(settings).build(),
            new TraceSupplier() {
                @Override
                public OpenTelemetry get() {
                    return sdk;
                }

                @Override
                public void close() {
                    sdk.close();
                }
            }
        );
        sdk = OpenTelemetrySdk.builder()
            .setTracerProvider(
                SdkTracerProvider.builder()
                    .setSampler(sampler)
                    .addSpanProcessor(SimpleSpanProcessor.create(new SanitizingSpanExporter(exporter, service::sanitize)))
                    .build()
            )
            .build();
        api = service.getOpenTelemetry();
    }

    /** Requires one completed span so missing or duplicated instrumentation cannot silently satisfy assertions. */
    public SpanData span(String name) {
        var spans = exporter.getFinishedSpanItems().stream().filter(span -> span.getName().equals(name)).toList();
        if (spans.size() != 1) {
            throw new AssertionError("expected one " + name + " span, got " + spans);
        }
        return spans.getFirst();
    }

    @Override
    public void close() {
        service.close();
    }
}
