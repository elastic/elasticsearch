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
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.DocumentMetadata;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Base class for constructing Elasticsearch document representations of OTel data (metrics, logs, traces).
 * Provides shared logic for building resource, scope, attribute, and data stream fields.
 */
public abstract class OTelDocumentBuilder {

    private static final String DATA_STREAM_TYPE = "data_stream.type";
    private static final String ELASTIC_MAPPING_MODE = "elastic.mapping.mode";
    private static final HexFormat HEX = HexFormat.of();
    private final BufferedByteStringAccessor byteStringAccessor;

    public OTelDocumentBuilder(BufferedByteStringAccessor byteStringAccessor) {
        this.byteStringAccessor = byteStringAccessor;
    }

    protected void buildResource(Resource resource, ByteString schemaUrl, XContentBuilder builder) throws IOException {
        buildResource(resource, schemaUrl, builder, false);
    }

    protected void buildResourceWithGeoPoints(Resource resource, ByteString schemaUrl, XContentBuilder builder) throws IOException {
        buildResource(resource, schemaUrl, builder, true);
    }

    private void buildResource(Resource resource, ByteString schemaUrl, XContentBuilder builder, boolean mergeGeoLocationAttributes)
        throws IOException {
        builder.startObject("resource");
        addFieldIfNotEmpty(builder, "schema_url", schemaUrl);
        buildAttributes(builder, resource.getAttributesList(), resource.getDroppedAttributesCount(), mergeGeoLocationAttributes);
        builder.endObject();
    }

    protected void buildScope(XContentBuilder builder, InstrumentationScope scope, ByteString schemaUrl) throws IOException {
        buildScope(builder, scope, schemaUrl, false);
    }

    protected void buildScopeWithGeoPoints(XContentBuilder builder, InstrumentationScope scope, ByteString schemaUrl) throws IOException {
        buildScope(builder, scope, schemaUrl, true);
    }

    private void buildScope(XContentBuilder builder, InstrumentationScope scope, ByteString schemaUrl, boolean mergeGeoLocationAttributes)
        throws IOException {
        builder.startObject("scope");
        addFieldIfNotEmpty(builder, "schema_url", schemaUrl);
        addFieldIfNotEmpty(builder, "name", scope.getNameBytes());
        addFieldIfNotEmpty(builder, "version", scope.getVersionBytes());
        buildAttributes(builder, scope.getAttributesList(), scope.getDroppedAttributesCount(), mergeGeoLocationAttributes);
        builder.endObject();
    }

    protected void addFieldIfNotEmpty(XContentBuilder builder, String name, ByteString value) throws IOException {
        if (value != null && value.isEmpty() == false) {
            builder.field(name);
            byteStringAccessor.utf8Value(builder, value);
        }
    }

    protected void addHexFieldIfNotEmpty(XContentBuilder builder, String name, ByteString value) throws IOException {
        if (value != null && value.isEmpty() == false) {
            builder.field(name, MessageDigests.toHexString(value.toByteArray()));
        }
    }

    protected void addEpochMillisNanosField(XContentBuilder builder, String fieldName, long unixNanos) throws IOException {
        long millis = TimeUnit.NANOSECONDS.toMillis(unixNanos);
        long nanosRemainder = unixNanos - TimeUnit.MILLISECONDS.toNanos(millis);
        if (nanosRemainder == 0) {
            builder.field(fieldName, millis);
        } else {
            builder.field(fieldName, Strings.format("%d.%06d", millis, nanosRemainder));
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

    /**
     * Builds attributes as received from OTLP, except for Elastic control attributes that are filtered out.
     * Geo attributes are not merged; use {@link #buildAttributesWithGeoPoints} when paired geo attributes
     * should be converted into {@code geo_point} values.
     */
    protected void buildAttributes(XContentBuilder builder, List<KeyValue> attributes, int droppedAttributesCount) throws IOException {
        buildAttributes(builder, attributes, droppedAttributesCount, false);
    }

    /**
     * Builds attributes while merging paired double {@code *.geo.location.lon} and {@code *.geo.location.lat}
     * values into {@code *.geo.location} arrays in {@code [lon, lat]} order.
     */
    protected void buildAttributesWithGeoPoints(XContentBuilder builder, List<KeyValue> attributes, int droppedAttributesCount)
        throws IOException {
        buildAttributes(builder, attributes, droppedAttributesCount, true);
    }

    private void buildAttributes(
        XContentBuilder builder,
        List<KeyValue> attributes,
        int droppedAttributesCount,
        boolean mergeGeoLocationAttributes
    ) throws IOException {
        if (droppedAttributesCount > 0) {
            builder.field("dropped_attributes_count", droppedAttributesCount);
        }
        builder.startObject("attributes");
        GeoLocationAttributes geoLocationAttributes = mergeGeoLocationAttributes ? new GeoLocationAttributes() : null;
        for (int i = 0, size = attributes.size(); i < size; i++) {
            KeyValue attribute = attributes.get(i);
            String key = attribute.getKey();
            if (isIgnoredAttribute(key) == false) {
                if (geoLocationAttributes != null && geoLocationAttributes.addIfGeoLocationAttribute(attribute)) {
                    continue;
                }
                builder.field(key);
                buildAnyValue(builder, attribute.getValue());
            }
        }
        if (geoLocationAttributes != null) {
            geoLocationAttributes.write(builder);
        }
        builder.endObject();
    }

    /**
     * Checks if the given attribute key is an ignored attribute.
     * Ignored attributes are well-known Elastic-specific attributes
     * that control routing, mapping, or document metadata but are not stored themselves.
     *
     * @param attributeKey the attribute key to check
     * @return true if the attribute is ignored, false otherwise
     */
    public static boolean isIgnoredAttribute(String attributeKey) {
        return TargetIndex.isTargetIndexAttribute(attributeKey)
            || MappingHints.isMappingHintsAttribute(attributeKey)
            || DocumentMetadata.isDocumentMetadataAttribute(attributeKey)
            || DATA_STREAM_TYPE.equals(attributeKey)
            || ELASTIC_MAPPING_MODE.equals(attributeKey);
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
            case KVLIST_VALUE -> {
                builder.startObject();
                List<KeyValue> kvList = value.getKvlistValue().getValuesList();
                for (int i = 0, kvListSize = kvList.size(); i < kvListSize; i++) {
                    KeyValue kv = kvList.get(i);
                    builder.field(kv.getKey());
                    buildAnyValue(builder, kv.getValue());
                }
                builder.endObject();
            }
            case BYTES_VALUE -> builder.value(value.getBytesValue().toByteArray());
            case VALUE_NOT_SET -> builder.nullValue();
        }
    }

    protected void addSpanId(XContentBuilder builder, byte[] spanId) throws IOException {
        addHexIdIfNotEmpty(builder, "span_id", spanId);
    }

    protected void addTraceId(XContentBuilder builder, byte[] traceId) throws IOException {
        addHexIdIfNotEmpty(builder, "trace_id", traceId);
    }

    private static void addHexIdIfNotEmpty(XContentBuilder builder, String fieldName, byte[] id) throws IOException {
        if (id.length > 0) {
            builder.field(fieldName, HEX.formatHex(id));
        }
    }

    private static class GeoLocationAttributes {
        private static final String GEO_LOCATION = "geo.location";
        private static final String GEO_LOCATION_LAT = "geo.location.lat";
        private static final String GEO_LOCATION_LON = "geo.location.lon";
        private static final String NAMESPACED_GEO_LOCATION_LAT = "." + GEO_LOCATION_LAT;
        private static final String NAMESPACED_GEO_LOCATION_LON = "." + GEO_LOCATION_LON;

        private Map<String, GeoLocation> locationsByPrefix;

        boolean addIfGeoLocationAttribute(KeyValue attribute) {
            AnyValue value = attribute.getValue();
            if (value.getValueCase() != AnyValue.ValueCase.DOUBLE_VALUE) {
                return false;
            }
            String key = attribute.getKey();
            String prefix;
            boolean isLon;
            if (GEO_LOCATION_LON.equals(key)) {
                prefix = "";
                isLon = true;
            } else if (GEO_LOCATION_LAT.equals(key)) {
                prefix = "";
                isLon = false;
            } else if (key.endsWith(NAMESPACED_GEO_LOCATION_LON)) {
                prefix = key.substring(0, key.length() - NAMESPACED_GEO_LOCATION_LON.length());
                isLon = true;
            } else if (key.endsWith(NAMESPACED_GEO_LOCATION_LAT)) {
                prefix = key.substring(0, key.length() - NAMESPACED_GEO_LOCATION_LAT.length());
                isLon = false;
            } else {
                return false;
            }
            if (locationsByPrefix == null) {
                locationsByPrefix = new LinkedHashMap<>();
            }
            GeoLocation location = locationsByPrefix.computeIfAbsent(prefix, ignored -> new GeoLocation());
            if (isLon) {
                location.lon = value.getDoubleValue();
                location.lonKey = key;
            } else {
                location.lat = value.getDoubleValue();
                location.latKey = key;
            }
            return true;
        }

        void write(XContentBuilder builder) throws IOException {
            if (locationsByPrefix == null) {
                return;
            }
            for (Map.Entry<String, GeoLocation> entry : locationsByPrefix.entrySet()) {
                String prefix = entry.getKey();
                GeoLocation location = entry.getValue();
                if (location.lonKey != null && location.latKey != null) {
                    builder.field(prefix.isEmpty() ? GEO_LOCATION : prefix + "." + GEO_LOCATION);
                    builder.startArray();
                    builder.value(location.lon);
                    builder.value(location.lat);
                    builder.endArray();
                } else if (location.lonKey != null) {
                    builder.field(location.lonKey, location.lon);
                } else {
                    builder.field(location.latKey, location.lat);
                }
            }
        }
    }

    private static class GeoLocation {
        private double lon;
        private double lat;
        private String lonKey;
        private String latKey;
    }
}
