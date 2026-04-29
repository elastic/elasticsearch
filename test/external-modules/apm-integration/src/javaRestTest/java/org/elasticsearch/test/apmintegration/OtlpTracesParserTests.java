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
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

/**
 * Unit tests for {@link OtlpTracesParser}'s normalisation logic. The parser brings raw OTLP
 * protobuf into the same shape the APM intake parser produces so a single contract assertion in
 * {@link AbstractTracesIT} can compare both export paths. These tests exercise that bridge in
 * isolation, against synthetic {@link io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest}
 * payloads.
 */
public class OtlpTracesParserTests extends ESTestCase {

    /**
     * Span attribute keys are prefixed with {@code otel.attributes.} and the {@link Span.SpanKind}
     * enum is surfaced as an {@code otel.span_kind} entry. The raw, un-prefixed form must not leak
     * into the parser output.
     */
    public void testSpanAttributesAreNormalisedToApmIntakeShape() throws IOException {
        ExportTraceServiceRequest request = ExportTraceServiceRequest.newBuilder()
            .addResourceSpans(
                ResourceSpans.newBuilder()
                    .setResource(Resource.newBuilder().build())
                    .addScopeSpans(
                        ScopeSpans.newBuilder()
                            .addSpans(
                                Span.newBuilder()
                                    .setName("GET /_nodes/stats")
                                    .setKind(Span.SpanKind.SPAN_KIND_SERVER)
                                    .setTraceId(com.google.protobuf.ByteString.copyFrom(new byte[16]))
                                    .setSpanId(com.google.protobuf.ByteString.copyFrom(new byte[8]))
                                    .addAttributes(stringAttribute("http.method", "GET"))
                                    .addAttributes(intAttribute("http.status_code", 200))
                            )
                    )
            )
            .build();

        List<ReceivedTelemetry> result = OtlpTracesParser.parse(new ByteArrayInputStream(request.toByteArray()));

        ReceivedTelemetry.ReceivedSpan span = result.stream()
            .filter(ReceivedTelemetry.ReceivedSpan.class::isInstance)
            .map(ReceivedTelemetry.ReceivedSpan.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("expected a ReceivedSpan in parser output"));

        Map<String, Object> attrs = span.attributes();
        assertThat(attrs, hasEntry("otel.attributes.http.method", "GET"));
        assertThat(attrs, hasEntry("otel.attributes.http.status_code", 200L));
        assertThat(attrs, hasEntry("otel.span_kind", "SERVER"));
        // Raw (un-prefixed) keys must not leak through; the parser's job is to deliver the intake-shaped form.
        assertThat(attrs.containsKey("http.method"), is(false));
        assertThat(attrs.containsKey("http.status_code"), is(false));
    }

    /**
     * Resource attribute keys are passed through verbatim — no {@code otel.attributes.} prefix —
     * because the cross-path contract uses raw OTel Semantic Convention names directly at the
     * resource level (e.g. {@code service.name}).
     */
    public void testResourceAttributesPassThroughVerbatim() throws IOException {
        ExportTraceServiceRequest request = ExportTraceServiceRequest.newBuilder()
            .addResourceSpans(
                ResourceSpans.newBuilder()
                    .setResource(
                        Resource.newBuilder()
                            .addAttributes(stringAttribute("service.name", "elasticsearch"))
                            .addAttributes(stringAttribute("telemetry.sdk.language", "java"))
                            .build()
                    )
            )
            .build();

        List<ReceivedTelemetry> result = OtlpTracesParser.parse(new ByteArrayInputStream(request.toByteArray()));

        ReceivedTelemetry.ReceivedResource resource = result.stream()
            .filter(ReceivedTelemetry.ReceivedResource.class::isInstance)
            .map(ReceivedTelemetry.ReceivedResource.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("expected a ReceivedResource in parser output"));

        Map<String, Object> attrs = resource.attributes();
        assertThat(attrs, hasEntry("service.name", "elasticsearch"));
        assertThat(attrs, hasEntry("telemetry.sdk.language", "java"));
        // Resource keys must not be prefixed; the cross-path contract uses raw OTel SemConv names.
        assertThat(attrs.containsKey("otel.attributes.service.name"), is(false));
    }

    private static KeyValue stringAttribute(String key, String value) {
        return KeyValue.newBuilder().setKey(key).setValue(AnyValue.newBuilder().setStringValue(value).build()).build();
    }

    private static KeyValue intAttribute(String key, long value) {
        return KeyValue.newBuilder().setKey(key).setValue(AnyValue.newBuilder().setIntValue(value).build()).build();
    }
}
