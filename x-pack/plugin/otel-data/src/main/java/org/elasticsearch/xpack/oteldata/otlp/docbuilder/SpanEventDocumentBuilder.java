/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;

import com.google.protobuf.ByteString;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This class constructs an Elasticsearch document representation of an OTel span event.
 */
public class SpanEventDocumentBuilder extends OTelDocumentBuilder {

    private static final String EVENT_NAME_ATTRIBUTE = "event.name";

    public SpanEventDocumentBuilder(BufferedByteStringAccessor byteStringAccessor) {
        super(byteStringAccessor);
    }

    public void buildSpanEventDocument(
        XContentBuilder builder,
        Resource resource,
        ByteString resourceSchemaUrl,
        InstrumentationScope scope,
        ByteString scopeSchemaUrl,
        TargetIndex targetIndex,
        Span span,
        Span.Event event
    ) throws IOException {
        builder.startObject();
        builder.field("@timestamp", TimeUnit.NANOSECONDS.toMillis(event.getTimeUnixNano()));
        buildDataStream(builder, targetIndex);
        addHexFieldIfNotEmpty(builder, "trace_id", span.getTraceId());
        addHexFieldIfNotEmpty(builder, "span_id", span.getSpanId());
        addFieldIfNotEmpty(builder, "event_name", event.getNameBytes());
        buildAttributesWithGeoPoints(builder, attributesWithEventName(event), event.getDroppedAttributesCount());
        buildResourceWithGeoPoints(resource, resourceSchemaUrl, builder);
        buildScopeWithGeoPoints(builder, scope, scopeSchemaUrl);
        builder.endObject();
    }

    private static List<KeyValue> attributesWithEventName(Span.Event event) {
        if (event.getNameBytes().isEmpty()) {
            return event.getAttributesList();
        }
        List<KeyValue> attributes = new ArrayList<>(event.getAttributesCount() + 1);
        List<KeyValue> existingAttributes = event.getAttributesList();
        for (int i = 0, size = existingAttributes.size(); i < size; i++) {
            KeyValue attribute = existingAttributes.get(i);
            if (EVENT_NAME_ATTRIBUTE.equals(attribute.getKey()) == false) {
                attributes.add(attribute);
            }
        }
        attributes.add(
            KeyValue.newBuilder()
                .setKey(EVENT_NAME_ATTRIBUTE)
                .setValue(AnyValue.newBuilder().setStringValue(event.getName()).build())
                .build()
        );
        return attributes;
    }
}
