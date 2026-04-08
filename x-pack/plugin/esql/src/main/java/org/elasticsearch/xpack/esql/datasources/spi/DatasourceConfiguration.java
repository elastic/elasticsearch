/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Base class for datasource configurations. Handles map-backed storage, unknown field
 * rejection, value normalization, toMap(), and toSettingValues(). Subclasses provide
 * {@link #fields()} and {@link #validate()} for cross-field checks.
 *
 * <p>Subclass fromMap() should be a one-liner:
 * <pre>{@code
 * public static S3Configuration fromMap(Map<String, Object> raw) {
 *     return raw == null || raw.isEmpty() ? null : new S3Configuration(raw);
 * }
 * }</pre>
 */
public abstract class DatasourceConfiguration {

    private final Map<String, String> settings;

    /**
     * Validates and normalizes raw settings from a REST request or CRUD layer.
     * Rejects unknown fields, stringifies values, lowercases auth, then calls
     * subclass {@link #validate()} for cross-field checks.
     */
    protected DatasourceConfiguration(Map<String, Object> raw) {
        Map<String, String> parsed = new HashMap<>();
        for (var entry : raw.entrySet()) {
            if (fields().containsKey(entry.getKey()) == false) {
                throw new IllegalArgumentException(
                    "unknown datasource setting [" + entry.getKey() + "]; known settings: " + fields().keySet()
                );
            }
            if (entry.getValue() != null) {
                String value = entry.getValue().toString();
                if ("auth".equals(entry.getKey())) {
                    value = value.toLowerCase(Locale.ROOT);
                }
                parsed.put(entry.getKey(), value);
            }
        }
        this.settings = Map.copyOf(parsed);
        validate();
    }

    /** Field definitions: name → is secret. Subclasses must provide this. */
    public abstract Map<String, Boolean> fields();

    /** Cross-field validation. Called after construction. */
    protected abstract void validate();

    /** Returns the internal settings map. Normalized, no nulls. */
    public Map<String, String> toMap() {
        return settings;
    }

    /** Gets a setting value by name. */
    public String get(String key) {
        return settings.get(key);
    }

    /** Converts to a map of {@link SettingValue}s with secret classification from {@link #fields()}. */
    public Map<String, SettingValue> toSettingValues() {
        Map<String, SettingValue> result = new HashMap<>();
        for (var entry : settings.entrySet()) {
            result.put(entry.getKey(), new SettingValue(entry.getValue(), fields().getOrDefault(entry.getKey(), false)));
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return settings.equals(((DatasourceConfiguration) o).settings);
    }

    @Override
    public int hashCode() {
        return settings.hashCode();
    }
}
