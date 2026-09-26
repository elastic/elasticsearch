/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.attributes;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.PackDimBlock;
import org.elasticsearch.compute.data.PackDimValue;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Flat dimension operations over the logical record API. Reuses the prototype's leaf codec, not its nested-path
 * or null-as-delete semantics. A future binary leaf codec can replace this boundary without changing expressions.
 */
final class PackDimValues {
    private PackDimValues() {}

    static BytesRef encode(Block block, int position) throws IOException {
        Object value = PackDimValueCodec.jsonValue(BlockUtils.toJavaObject(block, position));
        if (value instanceof List<?> values) {
            for (Object item : values)
                validateScalar(item);
        } else {
            validateScalar(value);
        }
        return PackDimValueCodec.encode(value);
    }

    private static void validateScalar(Object value) {
        if (value == null || value instanceof String || value instanceof Boolean || value instanceof Integer || value instanceof Long)
            return;
        if (value instanceof Double number && Double.isFinite(number)) return;
        throw new IllegalArgumentException("packed dimensions require strings, booleans, integers or finite doubles");
    }

    static void get(PackDimValue record, BytesRef name, DataType type, Block.Builder output) throws IOException {
        BytesRef encoded = record.get(name, new BytesRef());
        Object value = encoded == null ? null : PackDimValueCodec.decode(encoded);
        if (value instanceof List<?> values) {
            if (values.isEmpty()) output.appendNull();
            else {
                var converted = new ArrayList<Object>(values.size());
                // Ordinary multivalue blocks have null positions, not null elements within a position.
                // The packed leaf retains those elements; extracting to an ordinary column omits them.
                for (Object item : values)
                    if (item != null) converted.add(convert(item, type));
                if (converted.isEmpty()) {
                    output.appendNull();
                    return;
                }
                if (converted.size() > 1) output.beginPositionEntry();
                for (Object item : converted)
                    BlockUtils.appendValue(output, item, elementType(type));
                if (converted.size() > 1) output.endPositionEntry();
            }
        } else {
            BlockUtils.appendValue(output, convert(value, type), elementType(type));
        }
    }

    private static Object convert(Object value, DataType type) {
        if (value == null) return null;
        if (type == DataType.KEYWORD && value instanceof String string) return new BytesRef(string);
        if (type == DataType.BOOLEAN && value instanceof Boolean) return value;
        if (type == DataType.INTEGER && (value instanceof Integer || value instanceof Long)) return Math.toIntExact(
            ((Number) value).longValue()
        );
        if (type == DataType.LONG && (value instanceof Integer || value instanceof Long)) return ((Number) value).longValue();
        if (type == DataType.DOUBLE && value instanceof Double) return value;
        throw new IllegalArgumentException("dimension value does not match declared type [" + type + "]");
    }

    static ElementType elementType(DataType type) {
        return switch (type) {
            case KEYWORD -> ElementType.BYTES_REF;
            case BOOLEAN -> ElementType.BOOLEAN;
            case INTEGER -> ElementType.INT;
            case LONG -> ElementType.LONG;
            case DOUBLE -> ElementType.DOUBLE;
            case NULL -> ElementType.NULL;
            default -> throw new IllegalArgumentException("unsupported dimension type [" + type + "]");
        };
    }

    /** Copies encoded entries without decoding unrelated values. Null replacement bytes are never a delete signal. */
    static void set(PackDimValue record, BytesRef name, BytesRef replacement, PackDimBlock.Builder output) {
        int size = record.size();
        BytesRef[] names = new BytesRef[size + 1];
        BytesRef[] values = new BytesRef[size + 1];
        int count = 0;
        boolean inserted = false;
        for (int i = 0; i < size; i++) {
            BytesRef current = record.nameAt(i, new BytesRef());
            int comparison = current.compareTo(name);
            if (inserted == false && comparison >= 0) {
                names[count] = name;
                values[count++] = replacement;
                inserted = true;
            }
            if (comparison != 0) {
                names[count] = current;
                values[count++] = record.valueAt(i, new BytesRef());
            }
        }
        if (inserted == false) {
            names[count] = name;
            values[count++] = replacement;
        }
        // TODO: share unchanged immutable entries rather than copying them into the result builder.
        output.append(Arrays.copyOf(names, count), Arrays.copyOf(values, count));
    }

    /** Sorted requested names allow multiple removals in one merge traversal. */
    @Evaluator(extraName = "Unset")
    static void unset(PackDimBlock.Builder output, PackDimValue record, @Fixed BytesRef[] removed) {
        BytesRef[] names = new BytesRef[record.size()];
        BytesRef[] values = new BytesRef[record.size()];
        int count = 0;
        int key = 0;
        for (int i = 0; i < record.size(); i++) {
            BytesRef current = record.nameAt(i, new BytesRef());
            while (key < removed.length && removed[key].compareTo(current) < 0)
                key++;
            if (key == removed.length || removed[key].equals(current) == false) {
                names[count] = current;
                values[count++] = record.valueAt(i, new BytesRef());
            }
        }
        // TODO: share retained immutable entries without changing explicit absence semantics.
        output.append(Arrays.copyOf(names, count), Arrays.copyOf(values, count));
    }
}
