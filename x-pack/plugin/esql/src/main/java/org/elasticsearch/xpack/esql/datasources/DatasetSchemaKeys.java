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

import java.util.Map;

/**
 * Composes the key a dataset's resolution is cached under from what decides whether it can be reused: the files it
 * was resolved from, which {@link SchemaBreadth#inferredFrom} answers for each discovery mode, and the settings, which
 * the key takes from the per-file cache's own identity rule. Nothing here branches on a mode or a format.
 */
public final class DatasetSchemaKeys {

    private DatasetSchemaKeys() {}

    /** The key for a dataset's resolution over {@code listing}, or {@code null} when {@code breadth} caches nothing for it. */
    @Nullable
    public static DatasetSchemaKey of(SchemaBreadth breadth, FileList listing, String formatType, Map<String, Object> config) {
        InferredFrom inferredFrom = breadth.inferredFrom(listing);
        return inferredFrom == null ? null : DatasetSchemaKey.of(inferredFrom, formatType, config);
    }
}
