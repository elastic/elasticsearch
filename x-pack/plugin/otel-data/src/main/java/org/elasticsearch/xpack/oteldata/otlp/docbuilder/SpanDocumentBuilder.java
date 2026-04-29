/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;
import io.opentelemetry.proto.trace.v1.Status.StatusCode;

import com.google.protobuf.ByteString;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.List;

/**
 * This class constructs an Elasticsearch document representation of an OTel span.
 */
public class SpanDocumentBuilder extends OTelDocumentBuilder {

    public SpanDocumentBuilder(BufferedByteStringAccessor byteStringAccessor) {
        super(byteStringAccessor);
    }

    public void buildSpanDocument(
        XContentBuilder builder,
        Resource resource,
        ByteString resourceSchemaUrl,
        InstrumentationScope scope,
        ByteString scopeSchemaUrl,
        TargetIndex targetIndex,
        Span span
    ) throws IOException {
        builder.startObject();
        long timestamp = span.getStartTimeUnixNano();
        if (timestamp == 0) {
            timestamp = span.getEndTimeUnixNano();
        }
        // OTLP semantically requires span timestamps, but proto3 can still carry zeroes.
        // If both are zero, keep the malformed span indexable and let @timestamp remain epoch.
        addEpochMillisNanosField(builder, "@timestamp", timestamp);
        addHexFieldIfNotEmpty(builder, "trace_id", span.getTraceId());
        addHexFieldIfNotEmpty(builder, "span_id", span.getSpanId());
        addHexFieldIfNotEmpty(builder, "parent_span_id", span.getParentSpanId());
        addFieldIfNotEmpty(builder, "trace_state", span.getTraceStateBytes());
        addFieldIfNotEmpty(builder, "name", span.getNameBytes());
        builder.field("kind", normalizeSpanKind(span.getKind()));
        if (span.getStartTimeUnixNano() != 0 && span.getEndTimeUnixNano() != 0) {
            builder.field("duration", span.getEndTimeUnixNano() - span.getStartTimeUnixNano());
        }
        if (span.getDroppedEventsCount() > 0) {
            builder.field("dropped_events_count", span.getDroppedEventsCount());
        }
        if (span.getDroppedLinksCount() > 0) {
            builder.field("dropped_links_count", span.getDroppedLinksCount());
        }
        buildResource(resource, resourceSchemaUrl, builder);
        buildDataStream(builder, targetIndex);
        buildScope(builder, scope, scopeSchemaUrl);
        buildAttributes(builder, span.getAttributesList(), span.getDroppedAttributesCount());
        buildLinks(builder, span.getLinksList());
        buildStatus(builder, span);
        builder.endObject();
    }

    private void buildLinks(XContentBuilder builder, List<Span.Link> links) throws IOException {
        if (links.isEmpty()) {
            return;
        }
        builder.startArray("links");
        for (int i = 0, size = links.size(); i < size; i++) {
            Span.Link link = links.get(i);
            builder.startObject();
            addHexFieldIfNotEmpty(builder, "trace_id", link.getTraceId());
            addHexFieldIfNotEmpty(builder, "span_id", link.getSpanId());
            addFieldIfNotEmpty(builder, "trace_state", link.getTraceStateBytes());
            buildAttributes(builder, link.getAttributesList(), link.getDroppedAttributesCount());
            builder.endObject();
        }
        builder.endArray();
    }

    private void buildStatus(XContentBuilder builder, Span span) throws IOException {
        Status status = span.getStatus();
        boolean hasCode = status.getCode() != StatusCode.STATUS_CODE_UNSET;
        if (hasCode == false) {
            // OTel status messages are only meaningful with an error code, so skip message-only unset statuses.
            return;
        }
        builder.startObject("status");
        builder.field("code", normalizeStatusCode(status.getCode()));
        addFieldIfNotEmpty(builder, "message", status.getMessageBytes());
        builder.endObject();
    }

    private static String normalizeSpanKind(Span.SpanKind kind) {
        return switch (kind) {
            case SPAN_KIND_UNSPECIFIED -> "Unspecified";
            case SPAN_KIND_INTERNAL -> "Internal";
            case SPAN_KIND_SERVER -> "Server";
            case SPAN_KIND_CLIENT -> "Client";
            case SPAN_KIND_PRODUCER -> "Producer";
            case SPAN_KIND_CONSUMER -> "Consumer";
            case UNRECOGNIZED -> kind.name();
        };
    }

    private static String normalizeStatusCode(StatusCode statusCode) {
        return switch (statusCode) {
            case STATUS_CODE_UNSET -> "Unset";
            case STATUS_CODE_OK -> "Ok";
            case STATUS_CODE_ERROR -> "Error";
            case UNRECOGNIZED -> statusCode.name();
        };
    }
}
