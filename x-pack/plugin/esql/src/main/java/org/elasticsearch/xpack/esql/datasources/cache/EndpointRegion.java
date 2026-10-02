/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import java.util.Map;

/**
 * The object-identity dimensions shared by every external-source cache key: the storage {@code endpoint}
 * and {@code region} pulled from the WITH config. The same canonical path on a different endpoint (or
 * region) resolves to a different physical object, so both participate in key identity. Extracted so
 * {@link SchemaCacheKey}, {@link FileMetadataCacheKey}, and {@link ListingCacheKey} read them identically
 * and cannot drift on these dimensions.
 */
record EndpointRegion(String endpoint, String region) {
    static EndpointRegion of(Map<String, Object> config) {
        if (config == null) return new EndpointRegion("", "");
        // For dataset queries, connection params live in the _datasource sub-map rather than at the
        // top level. Fall back to the sub-map when the top-level key is absent so this reader is safe
        // even if the caller forgot to flatten the config via ExternalSourceResolver.storageConfig().
        // The string literal "_datasource" matches ExternalSourceResolver.DATASOURCE_CONFIG_KEY but
        // cannot reference it directly due to the package dependency direction.
        @SuppressWarnings("unchecked")
        Map<String, Object> ds = (Map<String, Object>) config.get("_datasource");
        String endpoint = String.valueOf(config.getOrDefault("endpoint", ds != null ? ds.getOrDefault("endpoint", "") : ""));
        String region = String.valueOf(config.getOrDefault("region", ds != null ? ds.getOrDefault("region", "") : ""));
        return new EndpointRegion(endpoint, region);
    }
}
