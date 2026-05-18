/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.cluster.metadata.DataSourceSetting;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Base class for datasource configurations. Handles map-backed storage, unknown field
 * rejection, and toStoredSettings(). Subclasses provide cross-field validation via
 * {@link #validate(ValidationException)}.
 */
public abstract class DataSourceConfiguration {

    private static final Logger logger = LogManager.getLogger(DataSourceConfiguration.class);

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
     * Returns a copy of {@code raw} containing only entries whose key is in {@code fieldDefs},
     * paired with the set of keys that were kept. Used at query time where a query-time configuration map
     * carries a mix of storage and format options; the storage plugin must ignore keys it does
     * not own rather than reject them as unknown. Returns {@code null}/empty unchanged.
     *
     * <p>Dropped keys are logged at {@code DEBUG} so a user who misspells e.g. {@code accout} can
     * find out why the storage config came back with defaults. Only key <em>names</em> are
     * logged — values are never emitted, since this method is unaware of which keys are secrets.
     * Format keys (like {@code header_row}) will appear here too, which is expected.
     */
    protected static Configured<Map<String, Object>> filterKnown(
        Map<String, Object> raw,
        Map<String, DataSourceConfigDefinition> fieldDefs
    ) {
        if (raw == null || raw.isEmpty()) {
            return new Configured<>(raw, Set.of());
        }
        Map<String, Object> filtered = new HashMap<>(raw.size());
        Set<String> consumed = new HashSet<>();
        // Cache the debug flag so we don't re-check on every entry; an in-flight log-level change
        // is not worth tracking precisely here.
        boolean debug = logger.isDebugEnabled();
        List<String> dropped = null;
        for (Map.Entry<String, Object> entry : raw.entrySet()) {
            if (fieldDefs.containsKey(entry.getKey())) {
                filtered.put(entry.getKey(), entry.getValue());
                consumed.add(entry.getKey());
            } else if (debug) {
                if (dropped == null) {
                    dropped = new ArrayList<>();
                }
                dropped.add(entry.getKey());
            }
        }
        if (dropped != null) {
            logger.debug("filtered out unknown keys [{}] from datasource config; recognized fields are [{}]", dropped, fieldDefs.keySet());
        }
        return new Configured<>(filtered, consumed);
    }

    /**
     * Filters {@code raw} to keys in {@code fieldDefs} via {@link #filterKnown}, then constructs
     * a configuration from the kept entries (or {@code null} if none kept). Pairs the result with
     * the consumed-keys set. Use from each subclass's {@code fromQueryConfig} to eliminate the
     * filter-construct-pair pipeline boilerplate.
     */
    protected static <T> Configured<T> filterAndConstruct(
        Map<String, Object> raw,
        Map<String, DataSourceConfigDefinition> fieldDefs,
        Function<Map<String, Object>, T> constructor
    ) {
        Configured<Map<String, Object>> filtered = filterKnown(raw, fieldDefs);
        T value = (filtered.value() == null || filtered.value().isEmpty()) ? null : constructor.apply(filtered.value());
        return new Configured<>(value, filtered.consumedKeys());
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
