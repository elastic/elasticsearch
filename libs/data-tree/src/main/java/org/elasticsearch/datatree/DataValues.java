/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datatree;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Map;

/**
 * Converts a generic Java "JSON tree", the shape produced by parsing JSON into nested maps and lists, into the
 * type-safe {@link DataValue} model.
 * <p>
 * The supported inputs are exactly the value types that such map parsing yields: {@code null}, {@link CharSequence},
 * {@link Boolean}, the standard {@link Number} types, {@link Map} (object), and {@link Iterable}/{@code Object[]}
 * (array). Any other type is rejected with an {@link IllegalArgumentException} rather than being silently coerced, which
 * preserves the model's guarantee that it never holds arbitrary {@code Object} values. Rejecting unknown types also
 * matches the existing audit behavior, where a request payload carrying a type the serializer does not understand is
 * allowed to fail rather than be logged in a degraded form.
 * <p>
 * Field and element order is preserved, so converting an ordered map (one parsed with the {@code ordered} flag set)
 * yields a {@link DataObject} whose iteration order matches the source. This keeps downstream encoding deterministic.
 */
public final class DataValues {

    private DataValues() {}

    /**
     * Converts a single generic Java value into a {@link DataValue}.
     *
     * @param value a value from a parsed JSON tree, or {@code null}
     * @return the corresponding {@link DataValue} ({@link DataNull#INSTANCE} for {@code null})
     * @throws IllegalArgumentException if {@code value} is of a type that has no JSON representation in this model
     */
    public static DataValue fromJava(Object value) {
        return switch (value) {
            case null -> DataNull.INSTANCE;
            // idempotent: allow already-converted values to pass through, which also covers nested DataObject/DataArray
            case DataValue dataValue -> dataValue;
            case CharSequence charSequence -> new DataString(charSequence.toString());
            case Boolean bool -> new DataBoolean(bool);
            case BigInteger bigInteger -> DataValue.of(bigInteger);
            case BigDecimal bigDecimal -> DataValue.of(bigDecimal);
            case Byte b -> DataValue.of(b.longValue());
            case Short s -> DataValue.of(s.longValue());
            case Integer i -> DataValue.of(i.longValue());
            case Long l -> DataValue.of(l);
            case Float f -> DataValue.of(f.doubleValue());
            case Double d -> DataValue.of(d);
            case Map<?, ?> map -> objectFromMap(map);
            case Object[] array -> arrayFrom(Arrays.asList(array));
            case Iterable<?> iterable -> arrayFrom(iterable);
            default -> throw new IllegalArgumentException(
                "Cannot convert value of type [" + value.getClass().getName() + "] to a DataValue"
            );
        };
    }

    /**
     * Converts a map into a {@link DataObject}, preserving the map's iteration order.
     *
     * @param map a map whose keys are strings and whose values are convertible via {@link #fromJava(Object)}
     * @return the corresponding {@link DataObject}
     * @throws IllegalArgumentException if any key is not a {@link String}, or any value has no JSON representation
     */
    public static DataObject objectFromMap(Map<?, ?> map) {
        DataObject object = new DataObject();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Object key = entry.getKey();
            if (key instanceof String name) {
                object.put(name, fromJava(entry.getValue()));
            } else {
                throw new IllegalArgumentException(
                    "DataObject field names must be strings but found [" + (key == null ? "null" : key.getClass().getName()) + "]"
                );
            }
        }
        return object;
    }

    /**
     * Converts a sequence into a {@link DataArray}, preserving order.
     *
     * @param values elements convertible via {@link #fromJava(Object)}
     * @return the corresponding {@link DataArray}
     * @throws IllegalArgumentException if any element has no JSON representation
     */
    public static DataArray arrayFrom(Iterable<?> values) {
        DataArray array = new DataArray();
        for (Object value : values) {
            array.add(fromJava(value));
        }
        return array;
    }
}
