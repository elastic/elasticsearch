/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Base class for datasource configurations. Handles map-backed storage, unknown field
 * rejection, and toConfigSettings(). Subclasses provide cross-field validation via
 * {@link #validate()} and value normalization via {@link #normalizeValue(String, String)}.
 */
public abstract class DatasourceConfiguration {

    private final Map<String, ConfigSetting> settingDefs;
    private final Map<String, Object> values;

    /**
     * Validates and normalizes raw settings from a REST request or CRUD layer.
     * Rejects unknown fields, calls {@link #normalizeValue} per entry, then
     * calls subclass {@link #validate()} for cross-field checks.
     *
     * @param raw the raw settings map from the REST request
     * @param settingDefs the setting definitions — passed explicitly to avoid
     *                    virtual method call in constructor
     */
    protected DatasourceConfiguration(Map<String, Object> raw, Map<String, ConfigSetting> settingDefs) {
        this.settingDefs = settingDefs;
        Map<String, Object> parsed = new HashMap<>();
        for (var entry : raw.entrySet()) {
            if (settingDefs.containsKey(entry.getKey()) == false) {
                throw new IllegalArgumentException(
                    "unknown datasource setting [" + entry.getKey() + "]; known settings: " + settingDefs.keySet()
                );
            }
            if (entry.getValue() != null) {
                parsed.put(entry.getKey(), normalizeValue(entry.getKey(), entry.getValue().toString()));
            }
        }
        this.values = Map.copyOf(parsed);
        validate();
    }

    /**
     * Normalizes a setting value. Override to apply type-specific normalization
     * (e.g. lowercasing auth modes). Default returns the value unchanged.
     */
    protected String normalizeValue(String key, String value) {
        return value;
    }

    /** Cross-field validation. Called after construction. */
    protected abstract void validate();

    /** Returns the setting definitions. */
    public Map<String, ConfigSetting> settings() {
        return settingDefs;
    }

    /** Returns the internal values map. Normalized, no nulls. */
    public Map<String, Object> toMap() {
        return values;
    }

    /** Gets a setting value by name. */
    public String get(String key) {
        Object v = values.get(key);
        return v != null ? v.toString() : null;
    }

    /** Returns validated settings as a map from definition to value. */
    public Map<ConfigSetting, Object> toConfigSettings() {
        Map<ConfigSetting, Object> result = new LinkedHashMap<>();
        for (var entry : values.entrySet()) {
            ConfigSetting def = settingDefs.get(entry.getKey());
            if (def != null) {
                result.put(def, entry.getValue());
            }
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return values.equals(((DatasourceConfiguration) o).values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }
}
