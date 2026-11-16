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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.BufferedMurmur3Hasher;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.sourcebatch.SourceBatchEncodeHelper;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPoint;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Columnar counterpart to {@link MetricDocumentBuilder}: writes OTel metric data-point groups directly into
 * an ESCF batch without going through an intermediate XContent representation.
 *
 * <p>{@link #buildMetricRow} writes each group <b>optimistically</b> and returns {@code false} the moment it
 * encounters something that can't be expressed as ESCF columns — a non-scalar data point
 * (histogram/summary/exponential-histogram) or unsupported attributes — rather than scanning the group
 * upfront. A {@code false} return leaves the builder with a half-written row, so callers must discard the
 * builder and fall back to the {@link MetricDocumentBuilder} XContent path.
 *
 * <p>The field layout emitted by this class is identical to that of {@link MetricDocumentBuilder}: same field
 * names, same nesting depth, same ordering. The {@code OTLPMetricsEscfComparisonRestIT} integration test
 * guards this contract by comparing {@code _id}, {@code _tsid}, and {@code _source} for each document across
 * the two paths.
 */
public final class OTLPMetricBatchBuilder extends AbstractOTLPBatchBuilder {

    private final MappingHints defaultMappingHints;
    private final BufferedMurmur3Hasher hasher = new BufferedMurmur3Hasher(0);

    private final int resourceParentCol;
    private final int resourceAttributesParentCol;
    private final int scopeParentCol;
    private final int scopeAttributesParentCol;
    private final int attributesParentCol;
    private final int metricsParentCol;

    private final int timestampCol;
    private final int dataStreamTypeCol;
    private final int dataStreamDatasetCol;
    private final int dataStreamNamespaceCol;
    private final int metricNamesHashCol;

    private int startTimestampCol = NOT_REGISTERED;
    private int resourceSchemaUrlCol = NOT_REGISTERED;
    private int resourceDroppedAttributesCountCol = NOT_REGISTERED;
    private int scopeSchemaUrlCol = NOT_REGISTERED;
    private int scopeNameCol = NOT_REGISTERED;
    private int scopeVersionCol = NOT_REGISTERED;
    private int scopeDroppedAttributesCountCol = NOT_REGISTERED;
    private int unitCol = NOT_REGISTERED;
    private int temporalityCol = NOT_REGISTERED;
    private int docCountCol = NOT_REGISTERED;

    /** Creates a builder using {@code recycler} to back column byte buffers for high-throughput paths. */
    public OTLPMetricBatchBuilder(Recycler<BytesRef> recycler, MappingHints defaultMappingHints) {
        super(recycler);
        this.defaultMappingHints = defaultMappingHints;

        resourceParentCol = objectField("resource", ROOT);
        resourceAttributesParentCol = objectField("attributes", resourceParentCol);
        int dataStreamParentCol = objectField("data_stream", ROOT);
        scopeParentCol = objectField("scope", ROOT);
        scopeAttributesParentCol = objectField("attributes", scopeParentCol);
        attributesParentCol = objectField("attributes", ROOT);
        metricsParentCol = objectField("metrics", ROOT);

        timestampCol = leafField("@timestamp", ROOT);
        dataStreamTypeCol = leafField("type", dataStreamParentCol);
        dataStreamDatasetCol = leafField("dataset", dataStreamParentCol);
        dataStreamNamespaceCol = leafField("namespace", dataStreamParentCol);
        metricNamesHashCol = leafField(MetricDocumentBuilder.METRIC_NAMES_HASH_FIELD, ROOT);
    }

    /**
     * Writes a single metric data-point group as the next row and commits it.
     *
     * @param dataPointGroup        the group to write
     * @param dynamicTemplates      out-parameter: per-field dynamic-template names (same semantics as in
     *                              {@link MetricDocumentBuilder#buildMetricDocument})
     * @param dynamicTemplateParams out-parameter: per-field dynamic-template parameters
     * @return {@code true} if the row was written and committed; {@code false} if the group contains data unsupported in batch mode,
     * in which case this builder must be discarded and the caller should fall back to the XContent path
     */
    public boolean buildMetricRow(
        DataPointGroupingContext.DataPointGroup dataPointGroup,
        Map<String, String> dynamicTemplates,
        Map<String, Map<String, String>> dynamicTemplateParams
    ) {
        try {
            writeRow(dataPointGroup, dynamicTemplates, dynamicTemplateParams);
            return true;
        } catch (RowIneligibleException e) {
            return false;
        }
    }

    private void writeRow(
        DataPointGroupingContext.DataPointGroup dataPointGroup,
        Map<String, String> dynamicTemplates,
        Map<String, Map<String, String>> dynamicTemplateParams
    ) {
        List<DataPoint> dataPoints = dataPointGroup.dataPoints();

        longField(timestampCol, TimeUnit.NANOSECONDS.toMillis(dataPointGroup.getTimestampUnixNano()));
        if (dataPointGroup.getStartTimestampUnixNano() != 0) {
            longField(startTimestampCol(), TimeUnit.NANOSECONDS.toMillis(dataPointGroup.getStartTimestampUnixNano()));
        }

        writeResource(dataPointGroup.resource(), dataPointGroup.resourceSchemaUrl());
        writeDataStream(dataPointGroup.targetIndex());
        writeScope(dataPointGroup.scope(), dataPointGroup.scopeSchemaUrl());
        // datapoint attributes (top-level "attributes" object)
        writeAttributes(AttributeKind.DATA_POINT, dataPointGroup.dataPointAttributes(), 0);

        String unit = dataPointGroup.unit();
        if (Strings.hasLength(unit)) {
            stringField(unitCol(), unit);
        }
        String temporality = MetricDocumentBuilder.temporalityToString(dataPointGroup.temporality());
        if (Strings.hasLength(temporality)) {
            stringField(temporalityCol(), temporality);
        }
        String metricNamesHash = dataPointGroup.getMetricNamesHash(hasher);
        if (Strings.hasLength(metricNamesHash)) {
            stringField(metricNamesHashCol, metricNamesHash);
        }

        // metrics object
        long docCount = 0;
        for (int i = 0; i < dataPoints.size(); i++) {
            DataPoint dataPoint = dataPoints.get(i);
            if (dataPoint.supportsColumnarValue() == false) {
                throw RowIneligibleException.INSTANCE;
            }
            String metricName = dataPoint.getMetricName();
            dataPoint.writeColumnarValue(this, leafField(metricName, metricsParentCol));

            MappingHints mappingHints = defaultMappingHints.withConfigFromAttributes(dataPoint.getAttributes());
            String dynamicTemplate = dataPoint.getDynamicTemplate(mappingHints);
            if (dynamicTemplate != null) {
                String metricFieldPath = "metrics." + metricName;
                dynamicTemplates.put(metricFieldPath, dynamicTemplate);
                if (Strings.hasLength(unit)) {
                    dynamicTemplateParams.put(metricFieldPath, Map.of(MetricDocumentBuilder.UNIT_FIELD, unit));
                }
            }
            if (mappingHints.docCount()) {
                docCount = dataPoint.getDocCount();
            }
        }
        if (docCount > 0) {
            longField(docCountCol(), docCount);
        }

        commitRow();
    }

    // -------------------------------------------------------------------------
    // Resource / Scope / DataStream / Attributes
    // -------------------------------------------------------------------------

    private void writeResource(Resource resource, ByteString schemaUrl) {
        if (hasLength(schemaUrl)) {
            stringField(resourceSchemaUrlCol(), schemaUrl);
        }
        writeAttributes(AttributeKind.RESOURCE, resource.getAttributesList(), resource.getDroppedAttributesCount());
    }

    private void writeScope(InstrumentationScope scope, ByteString schemaUrl) {
        if (hasLength(schemaUrl)) {
            stringField(scopeSchemaUrlCol(), schemaUrl);
        }
        ByteString name = scope.getNameBytes();
        if (hasLength(name)) {
            stringField(scopeNameCol(), name);
        }
        ByteString version = scope.getVersionBytes();
        if (hasLength(version)) {
            stringField(scopeVersionCol(), version);
        }
        writeAttributes(AttributeKind.SCOPE, scope.getAttributesList(), scope.getDroppedAttributesCount());
    }

    private void writeDataStream(TargetIndex targetIndex) {
        if (targetIndex.isDataStream() == false) {
            return;
        }
        if (Strings.hasLength(targetIndex.type())) {
            stringField(dataStreamTypeCol, targetIndex.type());
        }
        if (Strings.hasLength(targetIndex.dataset())) {
            stringField(dataStreamDatasetCol, targetIndex.dataset());
        }
        if (Strings.hasLength(targetIndex.namespace())) {
            stringField(dataStreamNamespaceCol, targetIndex.namespace());
        }
    }

    /** Which fixed attribute bucket a {@link #writeAttributes} call targets. */
    private enum AttributeKind {
        RESOURCE,
        SCOPE,
        DATA_POINT
    }

    private void writeAttributes(AttributeKind kind, List<KeyValue> attributes, int droppedCount) {
        if (droppedCount > 0) {
            switch (kind) {
                case RESOURCE -> longField(resourceDroppedAttributesCountCol(), droppedCount);
                case SCOPE -> longField(scopeDroppedAttributesCountCol(), droppedCount);
                // Callers never pass a non-zero droppedCount for data-point attributes (OTel data points
                // don't carry a dropped-attributes count); a non-zero value here would be a caller bug.
                case DATA_POINT -> throw new AssertionError("data-point attributes have no dropped-count field");
            }
        }
        int parent = switch (kind) {
            case RESOURCE -> resourceAttributesParentCol;
            case SCOPE -> scopeAttributesParentCol;
            case DATA_POINT -> attributesParentCol;
        };
        for (int i = 0; i < attributes.size(); i++) {
            KeyValue kv = attributes.get(i);
            if (OTelDocumentBuilder.isIgnoredAttribute(kv.getKey()) == false) {
                writeAttributeValue(parent, kv.getKey(), kv.getValue());
            }
        }
    }

    /** Writes attribute {@code key} into the object at {@code parentIdx} as a single leaf column. */
    private void writeAttributeValue(int parentIdx, String key, AnyValue value) {
        // An unset value is dropped rather than registering a column: it contributes nothing to the tsid, creates
        // no mapping, and indexes no value, so the only trace it could leave is in a stored _source which we are not interested in
        if (value.getValueCase() == AnyValue.ValueCase.VALUE_NOT_SET) {
            return;
        }
        int col = leafField(key, parentIdx);
        // A repeated key can't be represented in a column; keep the first value and drop the rest.
        // The OpenTelemetry specification by default does not allow duplicate keys,
        // however it leaves the door open for SDKs to opt-in into allowing duplicate keys for e.g. performance reasons.
        // Backends in this case are free to do what they want, we took the "drop" approach here
        // See https://opentelemetry.io/docs/specs/otel/common/#attribute-collections
        if (hasValue(col)) {
            return;
        }
        switch (value.getValueCase()) {
            case STRING_VALUE -> stringField(col, value.getStringValueBytes());
            case BOOL_VALUE -> booleanField(col, value.getBoolValue());
            case INT_VALUE -> longField(col, value.getIntValue());
            case DOUBLE_VALUE -> doubleField(col, value.getDoubleValue());
            case ARRAY_VALUE -> arrayField(col, packArray(value.getArrayValue().getValuesList()));
            case BYTES_VALUE, KVLIST_VALUE -> throw RowIneligibleException.INSTANCE;
            case VALUE_NOT_SET -> throw new AssertionError("an unset value is dropped before a column is resolved");
        }
    }

    /**
     * Packs an OTLP array attribute into inline array bytes. Element kinds mirror
     * {@link OTelDocumentBuilder#buildAnyValue} so the array renders identically, and
     * {@link SourceBatchEncodeHelper#packAccumulatedElements} picks the columnar {@code FIXED_ARRAY} encoding
     * whenever every element shares one fixed-width kind.
     *
     * @throws RowIneligibleException if any element is a bytes value, a KVLIST, or an array; see the class
     *                               Javadoc
     */
    private SourceBatchEncodeHelper.PackedArray packArray(List<AnyValue> values) {
        int size = values.size();
        byte[] elementTypes = new byte[size];
        long[] elementNumerics = new long[size];
        Object[] elementVars = new Object[size];
        for (int i = 0; i < size; i++) {
            AnyValue value = values.get(i);
            switch (value.getValueCase()) {
                case STRING_VALUE -> {
                    elementTypes[i] = SourceValueType.STRING;
                    elementVars[i] = utf8(value.getStringValueBytes());
                }
                case BOOL_VALUE -> elementTypes[i] = value.getBoolValue() ? SourceValueType.TRUE : SourceValueType.FALSE;
                case INT_VALUE -> {
                    elementTypes[i] = SourceValueType.LONG;
                    elementNumerics[i] = value.getIntValue();
                }
                case DOUBLE_VALUE -> {
                    elementTypes[i] = SourceValueType.DOUBLE;
                    elementNumerics[i] = Double.doubleToRawLongBits(value.getDoubleValue());
                }
                case VALUE_NOT_SET -> elementTypes[i] = SourceValueType.NULL;
                // TODO: Nested arrays are currently unsupported, they seem to cause trouble in the TSID computation
                case ARRAY_VALUE, BYTES_VALUE, KVLIST_VALUE -> throw RowIneligibleException.INSTANCE;
            }
        }
        return SourceBatchEncodeHelper.packAccumulatedElements(elementTypes, elementNumerics, elementVars, size, false);
    }

    private static XContentString.UTF8Bytes utf8(ByteString value) {
        byte[] bytes = value.toByteArray();
        return new XContentString.UTF8Bytes(bytes, 0, bytes.length);
    }

    // -------------------------------------------------------------------------
    // Lazily registered columns
    // -------------------------------------------------------------------------

    private int startTimestampCol() {
        return startTimestampCol = lazyLeafField(startTimestampCol, "start_timestamp", ROOT);
    }

    private int resourceSchemaUrlCol() {
        return resourceSchemaUrlCol = lazyLeafField(resourceSchemaUrlCol, "schema_url", resourceParentCol);
    }

    private int resourceDroppedAttributesCountCol() {
        return resourceDroppedAttributesCountCol = lazyLeafField(
            resourceDroppedAttributesCountCol,
            "dropped_attributes_count",
            resourceParentCol
        );
    }

    private int scopeSchemaUrlCol() {
        return scopeSchemaUrlCol = lazyLeafField(scopeSchemaUrlCol, "schema_url", scopeParentCol);
    }

    private int scopeNameCol() {
        return scopeNameCol = lazyLeafField(scopeNameCol, "name", scopeParentCol);
    }

    private int scopeVersionCol() {
        return scopeVersionCol = lazyLeafField(scopeVersionCol, "version", scopeParentCol);
    }

    private int scopeDroppedAttributesCountCol() {
        return scopeDroppedAttributesCountCol = lazyLeafField(scopeDroppedAttributesCountCol, "dropped_attributes_count", scopeParentCol);
    }

    private int unitCol() {
        return unitCol = lazyLeafField(unitCol, OTelTsdbDocumentBuilder.UNIT_FIELD, ROOT);
    }

    private int temporalityCol() {
        return temporalityCol = lazyLeafField(temporalityCol, OTelTsdbDocumentBuilder.TEMPORALITY_FIELD, ROOT);
    }

    private int docCountCol() {
        return docCountCol = lazyLeafField(docCountCol, "_doc_count", ROOT);
    }

    private static boolean hasLength(ByteString value) {
        return value != null && value.isEmpty() == false;
    }

    /**
     * Control-flow signal thrown when {@link #writeRow} encounters data that cannot be expressed as scalar
     * ESCF columns. Stack trace capture is suppressed since it's used purely for control flow on a hot path.
     */
    private static final class RowIneligibleException extends RuntimeException {
        static final RowIneligibleException INSTANCE = new RowIneligibleException();

        private RowIneligibleException() {
            super(null, null, false, false);
        }
    }
}
