/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import java.util.Map;

/**
 * Key for a cached {@link FileMetadata}. Deliberately credential-INDEPENDENT (endpoint + region only,
 * no access key / token) so the entry is shared across users exactly like the schema cache — the same
 * canonical path on the same endpoint/region resolves to the same object regardless of who asks. The
 * same canonical path on a different endpoint resolves to a different object, so endpoint and region
 * are part of the identity.
 */
public record FileMetadataCacheKey(String canonicalPath, String endpoint, String region) {
    public static FileMetadataCacheKey build(String canonicalPath, Map<String, Object> config) {
        EndpointRegion location = EndpointRegion.of(config);
        return new FileMetadataCacheKey(canonicalPath, location.endpoint(), location.region());
    }
}
