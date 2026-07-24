/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import java.util.ArrayList;
import java.util.List;

/**
 * Test-only bridge into {@link ExternalSourceCacheService}'s package-private schema cache, so
 * internal-cluster tests can surgically evict individual PER-FILE entries (deterministically simulating
 * LRU pressure) without widening the production API. Lives in the internalClusterTest source set only.
 */
public final class ExternalSourceCacheTestAccess {

    private ExternalSourceCacheTestAccess() {}

    /**
     * Invalidates every per-file schema-cache entry whose canonical path contains {@code pathSubstring},
     * leaving dataset-aggregate entries (marker-suffixed formatType) in place — the surgical arms of the
     * warm-fold regression tests must remove FILE entries, never the dataset aggregate under test.
     * Returns the number of entries invalidated.
     */
    public static int invalidatePerFileSchemaEntries(ExternalSourceCacheService service, String pathSubstring) {
        return invalidatePerFileSchemaEntries(service, pathSubstring, Integer.MAX_VALUE);
    }

    /**
     * Bounded variant of {@link #invalidatePerFileSchemaEntries(ExternalSourceCacheService, String)}:
     * invalidates at most {@code maxEntries} matching per-file entries. The deterministic PARTIAL
     * eviction arms use this to construct an exact missing subset — e.g. exactly ONE per-file entry,
     * the minimal trigger of the all-or-nothing multi-file stats merge — rather than sweeping
     * everything under a directory.
     */
    public static int invalidatePerFileSchemaEntries(ExternalSourceCacheService service, String pathSubstring, int maxEntries) {
        List<SchemaCacheKey> victims = new ArrayList<>();
        service.schemaCache().forEach((key, entry) -> {
            if (victims.size() < maxEntries && key.canonicalPath().contains(pathSubstring) && key.isDatasetAggregate() == false) {
                victims.add(key);
            }
        });
        for (SchemaCacheKey victim : victims) {
            service.schemaCache().invalidate(victim);
        }
        return victims.size();
    }
}
