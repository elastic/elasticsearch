/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;

import com.google.protobuf.ByteString;

import java.util.List;

/**
 * Test helpers for constructing OTLP trace payloads with syntactically valid IDs.
 */
public class OtlpTraceUtils {

    private static final ByteString EMPTY_TRACE_ID = ByteString.copyFrom(new byte[16]);
    private static final ByteString EMPTY_SPAN_ID = ByteString.copyFrom(new byte[8]);

    private static Resource createResource(List<KeyValue> attributes) {
        return Resource.newBuilder().addAllAttributes(attributes).build();
    }

    private static InstrumentationScope createScope(String name, String version) {
        return InstrumentationScope.newBuilder().setName(name).setVersion(version).build();
    }

    public static Span createSpan(String name) {
        return createSpan(name, List.of());
    }

    public static Span createSpan(String name, List<KeyValue> attributes) {
        return Span.newBuilder()
            .setName(name)
            .setTraceId(EMPTY_TRACE_ID)
            .setSpanId(EMPTY_SPAN_ID)
            .setStartTimeUnixNano(System.nanoTime())
            .setEndTimeUnixNano(System.nanoTime())
            .setKind(Span.SpanKind.SPAN_KIND_INTERNAL)
            .setStatus(Status.newBuilder().setCode(Status.StatusCode.STATUS_CODE_OK).build())
            .addAllAttributes(attributes)
            .build();
    }

    public static ResourceSpans createResourceSpans(List<KeyValue> resourceAttributes, List<ScopeSpans> scopeSpans) {
        return ResourceSpans.newBuilder().setResource(createResource(resourceAttributes)).addAllScopeSpans(scopeSpans).build();
    }

    public static ScopeSpans createScopeSpans(String name, String version, List<Span> spans) {
        return ScopeSpans.newBuilder().setScope(createScope(name, version)).addAllSpans(spans).build();
    }

    public static ExportTraceServiceRequest createTracesRequest(List<Span> spans) {
        return createTracesRequest(List.of(OtlpUtils.keyValue("service.name", "test-service")), spans);
    }

    public static ExportTraceServiceRequest createTracesRequest(List<KeyValue> resourceAttributes, List<Span> spans) {
        return ExportTraceServiceRequest.newBuilder()
            .addResourceSpans(createResourceSpans(resourceAttributes, List.of(createScopeSpans("test", "1.0.0", spans))))
            .build();
    }
}
