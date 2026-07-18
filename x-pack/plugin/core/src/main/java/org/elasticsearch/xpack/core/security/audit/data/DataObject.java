/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.audit.data;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * A mutable, insertion-ordered map of field names to {@link DataValue}s. Mutation is exposed deliberately for building
 * entries, while the field invariants (no Java {@code null} name or value) are preserved.
 */
public final class DataObject implements DataValue {

    // LinkedHashMap gives deterministic encoding and traversal.
    private final Map<String, DataValue> fields = new LinkedHashMap<>();

    public Optional<DataValue> get(String name) {
        return Optional.ofNullable(fields.get(name));
    }

    /**
     * Returns the string value of a field, or {@code null} if the field is absent or explicitly {@link DataNull}.
     * <p>
     * This is a convenience for the common case of reading flat, string-valued fields (as audit entries hold today).
     *
     * @param name the field name
     * @return the string value, or {@code null} if absent or null-valued
     * @throws IllegalStateException if the field is present but is not a string
     */
    public String getString(String name) {
        DataValue value = fields.get(name);
        if (value == null || value == DataNull.INSTANCE) {
            return null;
        }
        return value.requireString();
    }

    /**
     * @return the value for the given field
     * @throws NoSuchElementException if the field is not present
     */
    public DataValue require(String name) {
        DataValue value = fields.get(name);
        if (value == null) {
            throw new NoSuchElementException("Missing field [" + name + "]");
        }
        return value;
    }

    /**
     * Sets a field, overwriting any existing value.
     *
     * @return this object, to allow chaining
     */
    public DataObject put(String name, DataValue value) {
        fields.put(Objects.requireNonNull(name, "name"), Objects.requireNonNull(value, "value"));
        return this;
    }

    public DataObject with(String name, String value) {
        return put(name, DataValue.of(value));
    }

    public DataObject put(String name, String value) {
        return put(name, DataValue.of(value));
    }

    public DataObject put(String name, long value) {
        return put(name, DataValue.of(value));
    }

    public DataObject put(String name, boolean value) {
        return put(name, DataValue.of(value));
    }

    public Optional<DataValue> remove(String name) {
        return Optional.ofNullable(fields.remove(name));
    }

    /**
     * @return an unmodifiable view over the fields
     */
    public Map<String, DataValue> view() {
        return Collections.unmodifiableMap(fields);
    }

    public void forEach(BiConsumer<String, DataValue> consumer) {
        fields.forEach(consumer);
    }
}
