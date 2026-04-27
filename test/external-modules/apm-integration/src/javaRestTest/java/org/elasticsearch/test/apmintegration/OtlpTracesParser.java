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
import io.opentelemetry.proto.common.v1.AnyValue;
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
public final class OtlpTracesParser {

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
            for (ScopeSpans scopeSpans : resourceSpans.getScopeSpansList()) {
                for (Span span : scopeSpans.getSpansList()) {
                    String traceId = toHex(span.getTraceId().toByteArray());
                    String spanId = toHex(span.getSpanId().toByteArray());
                    Optional<String> parentSpanId = span.getParentSpanId().isEmpty()
                        ? Optional.empty()
                        : Optional.of(toHex(span.getParentSpanId().toByteArray()));
                    Map<String, Object> attributes = extractAttributes(span);
                    result.add(new ReceivedTelemetry.ReceivedSpan(span.getName(), traceId, spanId, parentSpanId, attributes));
                }
            }
        }
        return result;
    }

    private static Map<String, Object> extractAttributes(Span span) {
        Map<String, Object> attributes = new LinkedHashMap<>();
        for (KeyValue kv : span.getAttributesList()) {
            attributes.put(kv.getKey(), toJavaValue(kv.getValue()));
        }
        return Map.copyOf(attributes);
    }

    private static Object toJavaValue(AnyValue value) {
        return switch (value.getValueCase()) {
            case STRING_VALUE -> value.getStringValue();
            case INT_VALUE -> value.getIntValue();
            case DOUBLE_VALUE -> value.getDoubleValue();
            case BOOL_VALUE -> value.getBoolValue();
            default -> value.toString();
        };
    }

    private static String toHex(byte[] bytes) {
        return HexFormat.of().formatHex(bytes);
    }
}
