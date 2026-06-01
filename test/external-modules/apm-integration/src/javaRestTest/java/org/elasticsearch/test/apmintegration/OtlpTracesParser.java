/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Parses OTLP protobuf traces and produces protocol-neutral {@link ReceivedTelemetry} so that
 * tests can assert in a format-independent way.
 */
public final class OtlpTracesParser extends OtlpParser {

    private OtlpTracesParser() {}

    /**
     * Parse an OTLP traces request into a list of received telemetry events.
     *
     * @param input OTLP protobuf ExportTraceServiceRequest stream
     * @return list of ReceivedTelemetry (one per span)
     * @throws IOException if the stream is not valid OTLP protobuf
     */
    public static List<ReceivedTelemetry> parse(InputStream input) throws IOException {
        ExportTraceServiceRequest request = ExportTraceServiceRequest.parseFrom(input);
        List<ReceivedTelemetry> result = new ArrayList<>();
        for (ResourceSpans resourceSpans : request.getResourceSpansList()) {
            // Resource attributes pass through unchanged: no "otel.attributes." prefix
            // (unlike extractSpanAttributes below, which adds it to mimic the APM intake shape).
            // AbstractTracesIT.REQUIRED_RESOURCE_KEYS asserts on the OTel SemConv names directly.
            result.add(new ReceivedTelemetry.ReceivedResource(extractRawAttributes(resourceSpans.getResource().getAttributesList())));
            for (ScopeSpans scopeSpans : resourceSpans.getScopeSpansList()) {
                for (Span span : scopeSpans.getSpansList()) {
                    String traceId = toHex(span.getTraceId().toByteArray());
                    String spanId = toHex(span.getSpanId().toByteArray());
                    Optional<String> parentSpanId = span.getParentSpanId().isEmpty()
                        ? Optional.empty()
                        : Optional.of(toHex(span.getParentSpanId().toByteArray()));
                    Map<String, Object> attributes = extractSpanAttributes(span);
                    result.add(new ReceivedTelemetry.ReceivedSpan(span.getName(), traceId, spanId, parentSpanId, attributes));
                }
            }
        }
        return result;
    }

    /**
     * Test-only normalisation that brings raw OTLP span attributes into the same shape the APM intake
     * parser produces, so a single contract assertion in {@link AbstractTracesIT} can compare them:
     * <ul>
     *   <li>each OTel attribute key is prefixed with {@code otel.attributes.} (e.g. {@code http.method}
     *       becomes {@code otel.attributes.http.method}), matching how the APM agent nests attributes
     *       in its intake NDJSON;</li>
     *   <li>the {@link Span.SpanKind} enum is surfaced as an {@code otel.span_kind} entry
     *       (e.g. {@code "SERVER"}), matching the same intake nesting.</li>
     * </ul>
     * <p>
     * The translation lives in the test parser, not on the production wire: the OTLP wire format
     * carries flat OTel keys natively. Whether downstream consumers querying APM-Server-stored
     * documents see equivalent fields from both export paths depends on APM Server's own intake-to-
     * storage normalisation; that question is a separate verification effort outside this code.
     */
    private static Map<String, Object> extractSpanAttributes(Span span) {
        Map<String, Object> attributes = new LinkedHashMap<>();
        String kindName = span.getKind().name(); // e.g. "SPAN_KIND_SERVER"
        if (kindName.startsWith("SPAN_KIND_")) {
            attributes.put("otel.span_kind", kindName.substring("SPAN_KIND_".length()));
        }
        for (KeyValue kv : span.getAttributesList()) {
            attributes.put("otel.attributes." + kv.getKey(), toJavaValue(kv.getValue()));
        }
        return Map.copyOf(attributes);
    }

    private static String toHex(byte[] bytes) {
        return HexFormat.of().formatHex(bytes);
    }
}
