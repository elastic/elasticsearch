/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.cluster.metadata.DataSourceSetting;
import org.elasticsearch.common.ValidationException;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Base class for datasource configurations. Handles map-backed storage, unknown field
 * rejection, and toStoredSettings(). Subclasses provide cross-field validation via
 * {@link #validate(ValidationException)}.
 */
public abstract class DataSourceConfiguration {

    private final Map<String, DataSourceConfigDefinition> fieldDefs;
    private final Map<String, Object> values;

    /**
     * Parses, normalizes, and validates raw settings from a REST request or CRUD layer.
     * Rejects unknown fields, then calls subclass {@link #validate(ValidationException)} for
     * cross-field checks. Values preserve their original types (String, Integer, Boolean, etc.)
     * for non-lossy round-trips. Fields marked
     * {@link DataSourceConfigDefinition#caseInsensitive() caseInsensitive} are automatically
     * lowercased on input. All validation errors are accumulated and thrown together.
     *
     * <p>Note: {@code validate()} is a virtual call during base construction. This is safe
     * because subclasses must not add instance fields — all state is accessed via {@link #get}
     * from the already-populated base class. Subclass constructors should be private.
     *
     * @param raw the raw settings map from the REST request
     * @param fieldDefs the field definitions
     * @throws ValidationException if any validation errors are found
     */
    protected DataSourceConfiguration(Map<String, Object> raw, Map<String, DataSourceConfigDefinition> fieldDefs) {
        this.fieldDefs = fieldDefs;
        ValidationException errors = new ValidationException();
        DataSourceValidationUtils.rejectUnknownFields(raw, fieldDefs.keySet(), errors);
        Map<String, Object> parsed = new HashMap<>();
        for (var entry : raw.entrySet()) {
            if (fieldDefs.containsKey(entry.getKey()) && entry.getValue() != null) {
                Object value = entry.getValue();
                DataSourceConfigDefinition def = fieldDefs.get(entry.getKey());
                if (def.caseInsensitive() && value instanceof String s) {
                    value = s.toLowerCase(Locale.ROOT);
                }
                parsed.put(entry.getKey(), value);
            }
        }
        this.values = Map.copyOf(parsed);
        validate(errors);
        errors.throwIfValidationErrorsExist();
    }

    /** Cross-field validation. Accumulate errors into the provided exception. */
    protected abstract void validate(ValidationException errors);

    /** Returns true if any field marked as secret has a value set. Null values are already excluded. */
    protected boolean hasAnySecretValue() {
        for (var entry : values.entrySet()) {
            DataSourceConfigDefinition def = fieldDefs.get(entry.getKey());
            assert def != null : "values map should only contain known fields, got [" + entry.getKey() + "]";
            if (def.secret()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Builds a raw settings map from alternating field/value pairs, skipping nulls.
     * Returns null if all values are null. Used by {@code fromFields()} factory methods.
     */
    protected static Map<String, Object> buildRawMap(Object... fieldValuePairs) {
        if (fieldValuePairs.length % 2 != 0) {
            throw new IllegalArgumentException("fieldValuePairs must have even length");
        }
        Map<String, Object> raw = new HashMap<>();
        for (int i = 0; i < fieldValuePairs.length; i += 2) {
            if (fieldValuePairs[i] instanceof DataSourceConfigDefinition == false) {
                throw new IllegalArgumentException("expected DataSourceConfigDefinition at index " + i);
            }
            DataSourceConfigDefinition field = (DataSourceConfigDefinition) fieldValuePairs[i];
            Object value = fieldValuePairs[i + 1];
            if (value != null) {
                raw.put(field.name(), value);
            }
        }
        return raw.isEmpty() ? null : raw;
    }

    /** Returns the internal values map. Normalized, no nulls, types preserved. */
    public Map<String, Object> toMap() {
        return values;
    }

    /** Gets a setting value as a string. Returns {@code toString()} of the stored value, or null. */
    public String get(String key) {
        Object v = values.get(key);
        return v != null ? v.toString() : null;
    }

    /** Returns validated settings as a map from field name to {@link DataSourceSetting}. */
    public Map<String, DataSourceSetting> toStoredSettings() {
        Map<String, DataSourceSetting> result = new LinkedHashMap<>();
        for (var entry : values.entrySet()) {
            DataSourceConfigDefinition def = fieldDefs.get(entry.getKey());
            assert def != null : "values map should only contain known fields, got [" + entry.getKey() + "]";
            result.put(entry.getKey(), new DataSourceSetting(entry.getValue(), def.secret()));
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return values.equals(((DataSourceConfiguration) o).values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }
}
