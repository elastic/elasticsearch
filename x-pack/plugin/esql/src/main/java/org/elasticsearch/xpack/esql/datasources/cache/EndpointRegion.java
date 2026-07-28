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
        String endpoint = config != null ? String.valueOf(config.getOrDefault("endpoint", "")) : "";
        String region = config != null ? String.valueOf(config.getOrDefault("region", "")) : "";
        return new EndpointRegion(endpoint, region);
    }
}
