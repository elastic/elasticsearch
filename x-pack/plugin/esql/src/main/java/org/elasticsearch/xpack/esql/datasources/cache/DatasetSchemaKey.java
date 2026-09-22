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
import java.util.Set;

/**
 * Identity of a dataset's cached inferred schema: the files it was inferred from, the reader that inferred it, the
 * settings that could change what that reader infers, and where the files live. Two resolves share an entry exactly
 * when all four agree, and each part is there because the schema depends on it.
 * <p>
 * A plain value. Which files identify the schema, and which settings can change it, are decided where the discovery
 * modes and the readers live — {@code DatasetSchemaKeys} in the parent package — and handed in, so this package keeps
 * its dependency on that one pointing the right way.
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

    /**
     * The key for a schema inferred from {@code inferredFrom} by the {@code formatType} reader under {@code config}.
     *
     * @param schemaKeys the allow-list of settings that can change the inferred schema; a setting outside it cannot
     *                   reach the key, which is also why a credential never does
     */
    public static DatasetSchemaKey of(InferredFrom inferredFrom, String formatType, Set<String> schemaKeys, Map<String, Object> config) {
        EndpointRegion location = EndpointRegion.of(config);
        return new DatasetSchemaKey(inferredFrom, formatType, schemaSettings(schemaKeys, config), location.endpoint(), location.region());
    }

    /** The settings of {@code config} named in {@code schemaKeys}, their values as strings. */
    static Map<String, String> schemaSettings(Set<String> schemaKeys, @Nullable Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return Map.of();
        }
        Map<String, String> settings = new HashMap<>();
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            if (entry.getValue() != null && schemaKeys.contains(entry.getKey())) {
                settings.put(entry.getKey(), entry.getValue().toString());
            }
        }
        return settings;
    }
}
