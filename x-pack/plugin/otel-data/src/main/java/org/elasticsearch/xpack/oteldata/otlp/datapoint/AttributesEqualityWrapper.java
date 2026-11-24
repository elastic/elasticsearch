/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;

import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MetricDocumentBuilder;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Base for map keys that group OTLP entities (resources, scopes, data points) by their time series identity, without having to
 * compute their TSID, which is only needed when not indexing in batch mode.
 * <p>
 * Equal wrappers are guaranteed to have the same TSID dimensions (see {@link org.elasticsearch.xpack.oteldata.otlp.tsid}).
 * The reverse does not hold in rare cases, for example for nested attributes that are equivalent to dotted keys, which merely
 * results in less efficient grouping.
 * <p>
 * Subclasses are expected to compute their hash code eagerly with {@link #computeHashCode()}, which is independent of the order
 * of the attributes. As most entities end up in a group of their own, {@link #attributesEqual} should only be called once the
 * hash codes and all other fields are known to match.
 */
abstract class AttributesEqualityWrapper {

    private static final Comparator<Dimension> BY_PATH = Comparator.comparing(Dimension::path);

    private final List<KeyValue> attributes;
    // null if not computed yet
    private List<Dimension> flattenedAttributes;

    AttributesEqualityWrapper(List<KeyValue> attributes) {
        this.attributes = attributes;
    }

    /**
     * Computes the hash code of the attributes. Subclasses that add fields to the identity combine it with the hash of those fields.
     */
    protected int computeHashCode() {
        return hashAttributes(attributes);
    }

    /** Skips ignored attributes and sums up the hashes of the others, which makes the result independent of their order. */
    private static int hashAttributes(List<KeyValue> attributes) {
        int hash = 0;
        for (int i = 0; i < attributes.size(); i++) {
            KeyValue attribute = attributes.get(i);
            if (MetricDocumentBuilder.isIgnoredAttribute(attribute.getKey()) == false) {
                hash += hashAttribute(attribute);
            }
        }
        return hash;
    }

    /** Mixes key and value non-linearly, so that the sum over all attributes does not collide when values are swapped. */
    private static int hashAttribute(KeyValue attribute) {
        return (int) MurmurHash3.fmix(((long) attribute.getKey().hashCode() << 32) | (hashValue(attribute.getValue()) & 0xFFFFFFFFL));
    }

    /**
     * Hashes string values by their UTF-8 bytes. Protobuf only decodes strings on first access, which {@link AnyValue#hashCode()}
     * would trigger for every data point, while {@link AnyValue#equals} only runs on a hash match.
     */
    private static int hashValue(AnyValue value) {
        return switch (value.getValueCase()) {
            case STRING_VALUE -> value.getStringValueBytes().hashCode();
            case BOOL_VALUE -> Boolean.hashCode(value.getBoolValue());
            case DOUBLE_VALUE -> Double.hashCode(value.getDoubleValue());
            case INT_VALUE -> Long.hashCode(value.getIntValue());
            case BYTES_VALUE -> value.getBytesValue().hashCode();
            // the order of array elements is part of the identity
            case ARRAY_VALUE -> {
                List<AnyValue> values = value.getArrayValue().getValuesList();
                int hash = 1;
                for (int i = 0; i < values.size(); i++) {
                    hash = 31 * hash + hashValue(values.get(i));
                }
                yield hash;
            }
            // like the flattened attributes, nested attributes are compared independently of their order
            case KVLIST_VALUE -> hashAttributes(value.getKvlistValue().getValuesList());
            case VALUE_NOT_SET -> 0;
        };
    }

    protected final boolean attributesEqual(AttributesEqualityWrapper that) {
        // identical attribute lists are the common case and don't require any allocations
        return attributes.equals(that.attributes) || flattenedAttributes().equals(that.flattenedAttributes());
    }

    /**
     * Returns the attributes the way the TSID sees them: nested attributes and arrays are flattened into dotted paths,
     * ignored attributes are skipped, and the result is sorted by path, retaining the order of values sharing a path.
     */
    private List<Dimension> flattenedAttributes() {
        if (flattenedAttributes == null) {
            List<Dimension> dimensions = new ArrayList<>();
            flattenAttributes(attributes, null, dimensions);
            // stable sort, retains the order of values with the same path, just like the TSID does
            dimensions.sort(BY_PATH);
            flattenedAttributes = dimensions;
        }
        return flattenedAttributes;
    }

    private static void flattenAttributes(List<KeyValue> attributes, @Nullable String parentPath, List<Dimension> dimensions) {
        for (int i = 0; i < attributes.size(); i++) {
            KeyValue attribute = attributes.get(i);
            String key = attribute.getKey();
            if (MetricDocumentBuilder.isIgnoredAttribute(key) == false) {
                flattenValue(parentPath == null ? key : parentPath + "." + key, attribute.getValue(), dimensions);
            }
        }
    }

    private static void flattenValue(String path, AnyValue value, List<Dimension> dimensions) {
        switch (value.getValueCase()) {
            case KVLIST_VALUE -> flattenAttributes(value.getKvlistValue().getValuesList(), path, dimensions);
            case ARRAY_VALUE -> {
                List<AnyValue> values = value.getArrayValue().getValuesList();
                for (int i = 0; i < values.size(); i++) {
                    flattenValue(path, values.get(i), dimensions);
                }
            }
            // also retains values that are not part of the TSID, such as bytes, which only makes the comparison stricter
            default -> dimensions.add(new Dimension(path, value));
        }
    }

    /** A flattened attribute, the value is neither an array nor a key-value list. */
    private record Dimension(String path, AnyValue value) {}
}
