/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.cache.DatasetSchemaKey;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Composes the key a dataset's inferred schema is cached under from the two things that decide it: the discovery mode,
 * which says which files the schema depends on, and the reader, which says which of its settings change what it infers.
 * Neither is decided here — {@link SchemaBreadth#inferredFrom} and {@code FormatReader.schemaAffectingKeys} answer for
 * themselves — so nothing here branches on a mode or a format.
 */
public final class DatasetSchemaKeys {

    private DatasetSchemaKeys() {}

    /**
     * Resolver-level settings that change a dataset's inferred schema whatever the reader. {@code schema_resolution}
     * decides how per-file schemas combine, so {@code union_by_name} and {@code strict} over the same files are
     * different schemas. The partition keys stay in although partition columns are recomputed from the current listing
     * on every serve: nothing yet shows the detection mode leaves the inferred data schema untouched, and a key too
     * wide costs a miss where one too narrow serves a wrong schema.
     * <p>
     * Every other resolver-level setting is accounted for without appearing here — by the reader's own declaration, by
     * the key's format type, by the file identity, or as inert — and {@code DatasetSchemaKeysScopeTests} requires that
     * each one is, so a new resolver setting cannot be added without deciding which.
     */
    public static final Set<String> RESOLVER_SCHEMA_KEYS = Set.of(
        ExternalSourceResolver.CONFIG_SCHEMA_RESOLUTION,
        PartitionConfig.CONFIG_PARTITIONING_DETECTION,
        PartitionConfig.CONFIG_PARTITIONING_PATH
    );

    /**
     * The key for a dataset's schema over {@code listing}, or {@code null} when {@code breadth} caches nothing for it.
     *
     * @param readerSchemaKeys the reading format's {@code schemaAffectingKeys()}
     */
    @Nullable
    public static DatasetSchemaKey of(
        SchemaBreadth breadth,
        FileList listing,
        String formatType,
        Set<String> readerSchemaKeys,
        Map<String, Object> config
    ) {
        InferredFrom inferredFrom = breadth.inferredFrom(listing);
        if (inferredFrom == null) {
            return null;
        }
        Set<String> schemaKeys = new HashSet<>(readerSchemaKeys);
        schemaKeys.addAll(RESOLVER_SCHEMA_KEYS);
        return DatasetSchemaKey.of(inferredFrom, formatType, schemaKeys, config);
    }
}
