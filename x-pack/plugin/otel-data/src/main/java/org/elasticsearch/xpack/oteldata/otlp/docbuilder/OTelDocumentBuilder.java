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

import com.google.protobuf.ByteString;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.List;

/**
 * Base class for constructing Elasticsearch document representations of OTel data (metrics, logs, traces).
 * Provides shared logic for building resource, scope, attribute, and data stream fields.
 */
public abstract class OTelDocumentBuilder {

    private final BufferedByteStringAccessor byteStringAccessor;

    public OTelDocumentBuilder(BufferedByteStringAccessor byteStringAccessor) {
        this.byteStringAccessor = byteStringAccessor;
    }

    protected void buildResource(Resource resource, ByteString schemaUrl, XContentBuilder builder) throws IOException {
        builder.startObject("resource");
        addFieldIfNotEmpty(builder, "schema_url", schemaUrl);
        buildAttributes(builder, resource.getAttributesList(), resource.getDroppedAttributesCount());
        builder.endObject();
    }

    protected void buildScope(XContentBuilder builder, InstrumentationScope scope, ByteString schemaUrl) throws IOException {
        builder.startObject("scope");
        addFieldIfNotEmpty(builder, "schema_url", schemaUrl);
        addFieldIfNotEmpty(builder, "name", scope.getNameBytes());
        addFieldIfNotEmpty(builder, "version", scope.getVersionBytes());
        buildAttributes(builder, scope.getAttributesList(), scope.getDroppedAttributesCount());
        builder.endObject();
    }

    protected void addFieldIfNotEmpty(XContentBuilder builder, String name, ByteString value) throws IOException {
        if (value != null && value.isEmpty() == false) {
            builder.field(name);
            byteStringAccessor.utf8Value(builder, value);
        }
    }

    protected void buildDataStream(XContentBuilder builder, TargetIndex targetIndex) throws IOException {
        if (targetIndex.isDataStream() == false) {
            return;
        }
        builder.startObject("data_stream");
        builder.field("type", targetIndex.type());
        builder.field("dataset", targetIndex.dataset());
        builder.field("namespace", targetIndex.namespace());
        builder.endObject();
    }

    protected void buildAttributes(XContentBuilder builder, List<KeyValue> attributes, int droppedAttributesCount) throws IOException {
        if (droppedAttributesCount > 0) {
            builder.field("dropped_attributes_count", droppedAttributesCount);
        }
        builder.startObject("attributes");
        for (int i = 0, size = attributes.size(); i < size; i++) {
            KeyValue attribute = attributes.get(i);
            String key = attribute.getKey();
            if (isIgnoredAttribute(key) == false) {
                builder.field(key);
                buildAnyValue(builder, attribute.getValue());
            }
        }
        builder.endObject();
    }

    /**
     * Checks if the given attribute key is an ignored attribute.
     * Ignored attributes are well-known Elastic-specific attributes
     * that influence how the documents are indexed but are not stored themselves.
     *
     * @param attributeKey the attribute key to check
     * @return true if the attribute is ignored, false otherwise
     */
    public static boolean isIgnoredAttribute(String attributeKey) {
        return TargetIndex.isTargetIndexAttribute(attributeKey) || MappingHints.isMappingHintsAttribute(attributeKey);
    }

    protected void buildAnyValue(XContentBuilder builder, AnyValue value) throws IOException {
        switch (value.getValueCase()) {
            case STRING_VALUE -> byteStringAccessor.utf8Value(builder, value.getStringValueBytes());
            case BOOL_VALUE -> builder.value(value.getBoolValue());
            case INT_VALUE -> builder.value(value.getIntValue());
            case DOUBLE_VALUE -> builder.value(value.getDoubleValue());
            case ARRAY_VALUE -> {
                builder.startArray();
                List<AnyValue> valuesList = value.getArrayValue().getValuesList();
                for (int i = 0, valuesListSize = valuesList.size(); i < valuesListSize; i++) {
                    AnyValue arrayValue = valuesList.get(i);
                    buildAnyValue(builder, arrayValue);
                }
                builder.endArray();
            }
            default -> throw new IllegalArgumentException("Unsupported attribute value type: " + value.getValueCase());
        }
    }
}
