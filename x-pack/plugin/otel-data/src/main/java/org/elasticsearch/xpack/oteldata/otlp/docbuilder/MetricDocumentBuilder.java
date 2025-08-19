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
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPoint;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This class constructs an Elasticsearch document representation of a metric data point group.
 * It also handles dynamic templates for metrics based on their attributes.
 */
public class MetricDocumentBuilder {

    private final BufferedByteStringAccessor byteStringAccessor;

    public MetricDocumentBuilder(BufferedByteStringAccessor byteStringAccessor) {
        this.byteStringAccessor = byteStringAccessor;
    }

    public HashMap<String, String> buildMetricDocument(XContentBuilder builder, DataPointGroupingContext.DataPointGroup dataPointGroup)
        throws IOException {
        HashMap<String, String> dynamicTemplates = new HashMap<>();
        List<DataPoint> dataPoints = dataPointGroup.dataPoints();
        builder.startObject();
        builder.field("@timestamp", TimeUnit.NANOSECONDS.toMillis(dataPointGroup.getTimestampUnixNano()));
        if (dataPointGroup.getStartTimestampUnixNano() != 0) {
            builder.field("start_timestamp", TimeUnit.NANOSECONDS.toMillis(dataPointGroup.getStartTimestampUnixNano()));
        }
        buildResource(dataPointGroup.resource(), dataPointGroup.resourceSchemaUrl(), builder);
        if (dataPointGroup.targetIndex().isDataStream()) {
            buildDataStream(builder, dataPointGroup.targetIndex());
        }
        buildScope(builder, dataPointGroup.scopeSchemaUrl(), dataPointGroup.scope());
        buildDataPointAttributes(builder, dataPointGroup.dataPointAttributes(), dataPointGroup.unit());
        builder.field("_metric_names_hash", dataPointGroup.getMetricNamesHash());

        long docCount = 0;
        builder.startObject("metrics");
        for (int i = 0, dataPointsSize = dataPoints.size(); i < dataPointsSize; i++) {
            DataPoint dataPoint = dataPoints.get(i);
            builder.field(dataPoint.getMetricName());
            MappingHints mappingHints = MappingHints.fromAttributes(dataPoint.getAttributes());
            dataPoint.buildMetricValue(mappingHints, builder);
            String dynamicTemplate = dataPoint.getDynamicTemplate(mappingHints);
            if (dynamicTemplate != null) {
                dynamicTemplates.put("metrics." + dataPoint.getMetricName(), dynamicTemplate);
            }
            if (mappingHints.docCount()) {
                docCount = dataPoint.getDocCount();
            }
        }
        builder.endObject();
        if (docCount > 0) {
            builder.field("_doc_count", docCount);
        }
        builder.endObject();
        return dynamicTemplates;
    }

    private void buildResource(Resource resource, ByteString schemaUrl, XContentBuilder builder) throws IOException {
        builder.startObject("resource");
        addFieldIfNotEmpty(builder, "schema_url", schemaUrl);
        if (resource.getDroppedAttributesCount() > 0) {
            builder.field("dropped_attributes_count", resource.getDroppedAttributesCount());
        }
        builder.startObject("attributes");
        buildAttributes(builder, resource.getAttributesList());
        builder.endObject();
        builder.endObject();
    }

    private void buildScope(XContentBuilder builder, ByteString schemaUrl, InstrumentationScope scope) throws IOException {
        builder.startObject("scope");
        addFieldIfNotEmpty(builder, "schema_url", schemaUrl);
        if (scope.getDroppedAttributesCount() > 0) {
            builder.field("dropped_attributes_count", scope.getDroppedAttributesCount());
        }
        addFieldIfNotEmpty(builder, "name", scope.getNameBytes());
        addFieldIfNotEmpty(builder, "version", scope.getVersionBytes());
        builder.startObject("attributes");
        buildAttributes(builder, scope.getAttributesList());
        builder.endObject();
        builder.endObject();
    }

    private void addFieldIfNotEmpty(XContentBuilder builder, String name, ByteString value) throws IOException {
        if (value != null && value.isEmpty() == false) {
            builder.field(name);
            byteStringAccessor.utf8Value(builder, value);
        }
    }

    private void buildDataPointAttributes(XContentBuilder builder, List<KeyValue> attributes, String unit) throws IOException {
        builder.startObject("attributes");
        buildAttributes(builder, attributes);
        builder.endObject();
        builder.field("unit", unit);
    }

    private void buildDataStream(XContentBuilder builder, TargetIndex targetIndex) throws IOException {
        if (targetIndex.isDataStream() == false) {
            return;
        }
        builder.startObject("data_stream");
        builder.field("type", targetIndex.type());
        builder.field("dataset", targetIndex.dataset());
        builder.field("namespace", targetIndex.namespace());
        builder.endObject();
    }

    private void buildAttributes(XContentBuilder builder, List<KeyValue> attributes) throws IOException {
        for (int i = 0, size = attributes.size(); i < size; i++) {
            KeyValue attribute = attributes.get(i);
            String key = attribute.getKey();
            if (key.equals(TargetIndex.ELASTICSEARCH_INDEX)
                || key.equals(TargetIndex.DATA_STREAM_DATASET)
                || key.equals(TargetIndex.DATA_STREAM_NAMESPACE)
                || key.equals(MappingHints.MAPPING_HINTS)) {
                // ignore
                continue;
            }
            builder.field(attribute.getKey());
            attributeValue(builder, attribute.getValue());
        }
    }

    private void attributeValue(XContentBuilder builder, AnyValue value) throws IOException {
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
                    attributeValue(builder, arrayValue);
                }
                builder.endArray();
            }
            default -> throw new IllegalArgumentException("Unsupported attribute value type: " + value.getValueCase());
        }
    }

}
