/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.InferredFrom;

import java.util.HashMap;
import java.util.Map;

/**
 * Identity of a dataset's cached resolution: the files it was resolved from, the reader that read them, the settings
 * that change what that reader reads, and where the files live. Two resolves share an entry exactly when all four
 * agree — when nothing the cached result depends on has changed — and a change to any of them is a miss.
 * <p>
 * The settings are those {@link SchemaCacheKey#affectsIdentity} names: the same identity the per-file cache already
 * keys each file's schema and statistics on. The entry holds a schema, but the settings it depends on are the ones
 * that change what is READ rather than only what is inferred — an error mode changes a row count without changing a
 * column — and sharing that rule means a changed setting misses here exactly where it would for a per-file statistic.
 * <p>
 * A plain value. Which files identify the result is decided where the discovery modes live —
 * {@code DatasetSchemaKeys} in the parent package — and handed in, so this package keeps its dependency on that one
 * pointing the right way.
 * <p>
 * The settings are held as a map rather than rendered into one string. A rendered {@code key=value,...} form is
 * ambiguous once a value may itself contain {@code ,} or {@code =} — a CSV delimiter can be a comma — so two different
 * configurations could render alike, and a collision here would serve one dataset another's schema.
 */
public record DatasetSchemaKey(
    InferredFrom inferredFrom,
    String formatType,
    Map<String, String> schemaSettings,
    String endpoint,
    String region
) {

    public DatasetSchemaKey {
        schemaSettings = Map.copyOf(schemaSettings);
    }

    /** The key for a resolution from {@code inferredFrom} by the {@code formatType} reader under {@code config}. */
    public static DatasetSchemaKey of(InferredFrom inferredFrom, String formatType, Map<String, Object> config) {
        EndpointRegion location = EndpointRegion.of(config);
        return new DatasetSchemaKey(inferredFrom, formatType, schemaSettings(config), location.endpoint(), location.region());
    }

    /**
     * The settings of {@code config} the per-file cache keys on, their values as strings. An allow-list: a setting
     * reaches the key only because it changes what is read, and a credential never does.
     */
    static Map<String, String> schemaSettings(@Nullable Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return Map.of();
        }
        Map<String, String> settings = new HashMap<>();
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            if (entry.getValue() != null && SchemaCacheKey.affectsIdentity(entry.getKey())) {
                settings.put(entry.getKey(), entry.getValue().toString());
            }
        }
        return settings;
    }
}
