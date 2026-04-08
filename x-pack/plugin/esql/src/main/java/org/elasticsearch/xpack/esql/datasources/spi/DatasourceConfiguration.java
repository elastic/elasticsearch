/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Base class for datasource configurations. Handles map-backed storage, unknown field
 * rejection, value normalization, toMap(), and toConfigSettings(). Subclasses provide
 * {@link #settings()} and {@link #validate()} for cross-field checks.
 */
public abstract class DatasourceConfiguration {

    private final Map<String, String> values;

    /**
     * Validates and normalizes raw settings from a REST request or CRUD layer.
     * Rejects unknown fields, stringifies values, lowercases auth, then calls
     * subclass {@link #validate()} for cross-field checks.
     */
    protected DatasourceConfiguration(Map<String, Object> raw) {
        Map<String, ConfigSetting> defs = settings();
        Map<String, String> parsed = new HashMap<>();
        for (var entry : raw.entrySet()) {
            if (defs.containsKey(entry.getKey()) == false) {
                throw new IllegalArgumentException("unknown datasource setting [" + entry.getKey() + "]; known settings: " + defs.keySet());
            }
            if (entry.getValue() != null) {
                String value = entry.getValue().toString();
                if ("auth".equals(entry.getKey())) {
                    value = value.toLowerCase(Locale.ROOT);
                }
                parsed.put(entry.getKey(), value);
            }
        }
        this.values = Map.copyOf(parsed);
        validate();
    }

    /** Setting definitions keyed by name. Subclasses must provide this. */
    public abstract Map<String, ConfigSetting> settings();

    /** Cross-field validation. Called after construction. */
    protected abstract void validate();

    /** Returns the internal values map. Normalized, no nulls. */
    public Map<String, String> toMap() {
        return values;
    }

    /** Gets a setting value by name. */
    public String get(String key) {
        return values.get(key);
    }

    /** Returns validated settings as a list of {@link ConfigSetting}s with values populated. */
    public List<ConfigSetting> toConfigSettings() {
        Map<String, ConfigSetting> defs = settings();
        List<ConfigSetting> result = new ArrayList<>();
        for (var entry : values.entrySet()) {
            ConfigSetting def = defs.get(entry.getKey());
            if (def != null) {
                result.add(def.withValue(entry.getValue()));
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
