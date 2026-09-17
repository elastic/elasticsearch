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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.BufferedMurmur3Hasher;
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.escf.EscfRowBuffer;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPoint;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Columnar counterpart to {@link MetricDocumentBuilder}: writes OTel metric data-point groups directly
 * into an {@link EscfRowBuffer} without going through an intermediate XContent representation.
 *
 * <p>The method {@link #buildMetricRow} returns {@code true} on success. It returns {@code false} when the
 * data-point group cannot be fully expressed as scalar ESCF columns — currently when any data point is not a
 * scalar number (histogram/summary/exponential-histogram) or any attribute value is non-scalar
 * (ARRAY/KVLIST/BYTES). Callers must treat a {@code false} return as a signal to fall back to the
 * {@link MetricDocumentBuilder} XContent path; the row-buffer is left in an undefined state and must not be
 * committed.
 *
 * <p>The field layout emitted by this class is identical to that of {@link MetricDocumentBuilder}:
 * same field names, same nesting depth, same ordering. The {@code testTsidForBulkIsSame} integration tests
 * in {@code OTLPMetricsIndexingRestIT} guard this contract.
 */
public final class MetricColumnarBuilder {

    private final MappingHints defaultMappingHints;
    private final BufferedMurmur3Hasher hasher = new BufferedMurmur3Hasher(0);

    public MetricColumnarBuilder(MappingHints defaultMappingHints) {
        this.defaultMappingHints = defaultMappingHints;
    }

    /**
     * Writes a single metric data-point group into the next row of {@code builder}.
     *
     * <p>Calls {@link EscfBatchBuilder#beginRow()} before writing and leaves the row staged (not committed).
     * The caller is responsible for calling {@link EscfBatchBuilder#commit(int)} if this method returns
     * {@code true}, and for discarding the staged row (via the next {@link EscfBatchBuilder#beginRow()} call)
     * if it returns {@code false}.
     *
     * @param batchBuilder       the batch builder whose current row will receive the fields
     * @param dataPointGroup     the group to write
     * @param dynamicTemplates   out-parameter: per-field dynamic-template names (same semantics as in
     *                           {@link MetricDocumentBuilder#buildMetricDocument})
     * @param dynamicTemplateParams out-parameter: per-field dynamic-template parameters
     * @return {@code true} if the row was written successfully; {@code false} if the group contains
     *         non-scalar values and the caller should fall back to the XContent path
     */
    public boolean buildMetricRow(
        EscfBatchBuilder batchBuilder,
        DataPointGroupingContext.DataPointGroup dataPointGroup,
        Map<String, String> dynamicTemplates,
        Map<String, Map<String, String>> dynamicTemplateParams
    ) {
        // Pre-flight: check that every data point has a scalar value and every attribute is scalar.
        // Do this before opening the row to avoid leaving a partially-filled buffer on failure.
        List<DataPoint> dataPoints = dataPointGroup.dataPoints();
        for (int i = 0; i < dataPoints.size(); i++) {
            if (dataPoints.get(i).supportsColumnarValue() == false) {
                return false;
            }
        }
        if (hasNonScalarAttributes(dataPointGroup.dataPointAttributes())) {
            return false;
        }
        if (hasNonScalarAttributes(dataPointGroup.resource().getAttributesList())) {
            return false;
        }
        if (hasNonScalarAttributes(dataPointGroup.scope().getAttributesList())) {
            return false;
        }

        EscfRowBuffer row = batchBuilder.beginRow();
        try {
            return writeRow(row, batchBuilder, dataPointGroup, dynamicTemplates, dynamicTemplateParams);
        } catch (IllegalArgumentException e) {
            // EscfRowBuffer throws IllegalArgumentException on duplicate field names.
            return false;
        }
    }

    private boolean writeRow(
        EscfRowBuffer row,
        EscfBatchBuilder batchBuilder,
        DataPointGroupingContext.DataPointGroup dataPointGroup,
        Map<String, String> dynamicTemplates,
        Map<String, Map<String, String>> dynamicTemplateParams
    ) {
        List<DataPoint> dataPoints = dataPointGroup.dataPoints();

        row.longField("@timestamp", TimeUnit.NANOSECONDS.toMillis(dataPointGroup.getTimestampUnixNano()));
        if (dataPointGroup.getStartTimestampUnixNano() != 0) {
            row.longField("start_timestamp", TimeUnit.NANOSECONDS.toMillis(dataPointGroup.getStartTimestampUnixNano()));
        }

        // resource
        writeResource(row, dataPointGroup.resource(), dataPointGroup.resourceSchemaUrl());

        // data_stream
        writeDataStream(row, dataPointGroup.targetIndex());

        // scope
        writeScope(row, dataPointGroup.scope(), dataPointGroup.scopeSchemaUrl());

        // datapoint attributes (top-level "attributes" object)
        writeAttributes(row, dataPointGroup.dataPointAttributes(), 0);

        if (Strings.hasLength(dataPointGroup.unit())) {
            writeAsciiStringField(row, MetricDocumentBuilder.UNIT_FIELD, dataPointGroup.unit());
        }

        String temporality = MetricDocumentBuilder.temporalityToString(dataPointGroup.temporality());
        if (temporality != null) {
            writeAsciiStringField(row, MetricDocumentBuilder.TEMPORALITY_FIELD, temporality);
        }

        String metricNamesHash = dataPointGroup.getMetricNamesHash(hasher);
        writeAsciiStringField(row, "_metric_names_hash", metricNamesHash);

        // metrics object
        row.startObject("metrics");
        for (int i = 0; i < dataPoints.size(); i++) {
            DataPoint dataPoint = dataPoints.get(i);
            String metricName = dataPoint.getMetricName();
            dataPoint.writeColumnarValue(row, metricName);

            MappingHints mappingHints = defaultMappingHints.withConfigFromAttributes(dataPoint.getAttributes());
            String dynamicTemplate = dataPoint.getDynamicTemplate(mappingHints);
            if (dynamicTemplate != null) {
                String metricFieldPath = "metrics." + metricName;
                dynamicTemplates.put(metricFieldPath, dynamicTemplate);
                String unit = dataPointGroup.unit();
                if (unit != null && unit.isEmpty() == false) {
                    dynamicTemplateParams.put(metricFieldPath, Map.of(MetricDocumentBuilder.UNIT_FIELD, unit));
                }
            }
        }
        row.endObject();

        row.finishRow();
        return true;
    }

    // -------------------------------------------------------------------------
    // Resource / Scope / DataStream / Attributes
    // -------------------------------------------------------------------------

    private void writeResource(EscfRowBuffer row, Resource resource, ByteString schemaUrl) {
        row.startObject("resource");
        writeByteStringFieldIfNotEmpty(row, "schema_url", schemaUrl);
        writeAttributes(row, resource.getAttributesList(), resource.getDroppedAttributesCount());
        row.endObject();
    }

    private void writeScope(EscfRowBuffer row, InstrumentationScope scope, ByteString schemaUrl) {
        row.startObject("scope");
        writeByteStringFieldIfNotEmpty(row, "schema_url", schemaUrl);
        writeByteStringFieldIfNotEmpty(row, "name", scope.getNameBytes());
        writeByteStringFieldIfNotEmpty(row, "version", scope.getVersionBytes());
        writeAttributes(row, scope.getAttributesList(), scope.getDroppedAttributesCount());
        row.endObject();
    }

    private void writeDataStream(EscfRowBuffer row, TargetIndex targetIndex) {
        if (targetIndex.isDataStream() == false) {
            return;
        }
        row.startObject("data_stream");
        writeAsciiStringField(row, "type", targetIndex.type());
        writeAsciiStringField(row, "dataset", targetIndex.dataset());
        writeAsciiStringField(row, "namespace", targetIndex.namespace());
        row.endObject();
    }

    private void writeAttributes(EscfRowBuffer row, List<KeyValue> attributes, int droppedCount) {
        if (droppedCount > 0) {
            row.longField("dropped_attributes_count", droppedCount);
        }
        row.startObject("attributes");
        for (int i = 0; i < attributes.size(); i++) {
            KeyValue kv = attributes.get(i);
            if (OTelDocumentBuilder.isIgnoredAttribute(kv.getKey()) == false) {
                writeScalarAnyValue(row, kv.getKey(), kv.getValue());
            }
        }
        row.endObject();
    }

    /**
     * Writes a scalar {@link AnyValue} field. Non-scalar types (ARRAY, KVLIST, BYTES, VALUE_NOT_SET)
     * are silently skipped — the pre-flight check in {@link #buildMetricRow} ensures they do not appear.
     */
    private void writeScalarAnyValue(EscfRowBuffer row, String fieldName, AnyValue value) {
        switch (value.getValueCase()) {
            case STRING_VALUE -> {
                ByteString sv = value.getStringValueBytes();
                byte[] bytes = sv.toByteArray();
                row.stringField(fieldName, bytes, 0, bytes.length);
            }
            case BOOL_VALUE -> row.booleanField(fieldName, value.getBoolValue());
            case INT_VALUE -> row.longField(fieldName, value.getIntValue());
            case DOUBLE_VALUE -> row.doubleField(fieldName, value.getDoubleValue());
            // Non-scalar types should have been rejected by hasNonScalarAttributes() before this point.
            default -> {
                /* skip */ }
        }
    }

    private static void writeByteStringFieldIfNotEmpty(EscfRowBuffer row, String name, ByteString value) {
        if (value != null && value.isEmpty() == false) {
            row.stringField(name, value.toByteArray(), 0, value.size());
        }
    }

    private static void writeAsciiStringField(EscfRowBuffer row, String name, String value) {
        if (value != null && value.isEmpty() == false) {
            byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            row.stringField(name, bytes, 0, bytes.length);
        }
    }

    // -------------------------------------------------------------------------
    // Pre-flight checks
    // -------------------------------------------------------------------------

    /**
     * Returns {@code true} if any attribute in the list has a non-scalar value (ARRAY, KVLIST, BYTES,
     * or VALUE_NOT_SET). Such values cannot be written as plain columns in the current ESCF row API.
     */
    public static boolean hasNonScalarAttributes(List<KeyValue> attributes) {
        for (int i = 0; i < attributes.size(); i++) {
            switch (attributes.get(i).getValue().getValueCase()) {
                case ARRAY_VALUE, KVLIST_VALUE, BYTES_VALUE, VALUE_NOT_SET -> {
                    return true;
                }
                default -> {
                    /* scalar — ok */ }
            }
        }
        return false;
    }
}
