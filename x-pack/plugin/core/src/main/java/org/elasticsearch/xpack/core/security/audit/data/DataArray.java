/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.audit.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * A mutable, ordered sequence of {@link DataValue}s. Mutation is exposed deliberately for building entries, while the
 * element invariants (no Java {@code null}) are preserved.
 */
public final class DataArray implements DataValue, Iterable<DataValue> {

    private final List<DataValue> values = new ArrayList<>();

    /**
     * Creates an array of the given string values, preserving order.
     *
     * @param values the string values to hold
     * @return a new array holding a {@link DataString} per value
     */
    public static DataArray of(Collection<String> values) {
        final DataArray array = new DataArray();
        for (final String value : values) {
            array.add(value);
        }
        return array;
    }

    public int size() {
        return values.size();
    }

    public DataValue get(int index) {
        return values.get(index);
    }

    /**
     * Appends a value.
     *
     * @return this array, to allow chaining
     */
    public DataArray add(DataValue value) {
        values.add(Objects.requireNonNull(value, "value"));
        return this;
    }

    public DataArray add(String value) {
        return add(DataValue.of(value));
    }

    public DataArray add(long value) {
        return add(DataValue.of(value));
    }

    public DataArray add(boolean value) {
        return add(DataValue.of(value));
    }

    public DataValue set(int index, DataValue value) {
        return values.set(index, Objects.requireNonNull(value, "value"));
    }

    public DataValue remove(int index) {
        return values.remove(index);
    }

    /**
     * @return an unmodifiable view over the elements
     */
    public List<DataValue> view() {
        return Collections.unmodifiableList(values);
    }

    @Override
    public Iterator<DataValue> iterator() {
        return view().iterator();
    }
}
