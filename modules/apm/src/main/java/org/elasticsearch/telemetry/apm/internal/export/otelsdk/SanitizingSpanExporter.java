/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.DelegatingSpanData;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;

import java.util.Collection;
import java.util.List;
import java.util.function.UnaryOperator;

/** Protects every exporter input, regardless of the instrumentation or API overload which produced it. */
public final class SanitizingSpanExporter implements SpanExporter {
    private final SpanExporter delegate;
    private final UnaryOperator<Attributes> sanitizer;

    public SanitizingSpanExporter(SpanExporter delegate, UnaryOperator<Attributes> sanitizer) {
        this.delegate = delegate;
        this.sanitizer = sanitizer;
    }

    @Override
    public CompletableResultCode export(Collection<SpanData> spans) {
        List<SpanData> sanitized = spans.stream().<SpanData>map(span -> {
            Attributes attributes = sanitizer.apply(span.getAttributes());
            List<EventData> events = span.getEvents()
                .stream()
                .map(
                    event -> EventData.create(
                        event.getEpochNanos(),
                        event.getName(),
                        sanitizer.apply(event.getAttributes()),
                        event.getTotalAttributeCount()
                    )
                )
                .toList();
            List<LinkData> links = span.getLinks()
                .stream()
                .map(link -> LinkData.create(link.getSpanContext(), sanitizer.apply(link.getAttributes()), link.getTotalAttributeCount()))
                .toList();
            return new DelegatingSpanData(span) {
                @Override
                public Attributes getAttributes() {
                    return attributes;
                }

                @Override
                public List<EventData> getEvents() {
                    return events;
                }

                @Override
                public List<LinkData> getLinks() {
                    return links;
                }
            };
        }).toList();
        return delegate.export(sanitized);
    }

    @Override
    public CompletableResultCode flush() {
        return delegate.flush();
    }

    @Override
    public CompletableResultCode shutdown() {
        return delegate.shutdown();
    }
}
