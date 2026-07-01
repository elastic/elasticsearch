/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.stateless.commits.BlobFile;

import java.util.Map;

@FunctionalInterface
public interface TimestampResolver<T> {

    long getTimestampMillis(T key);

    /**
     * Resolves a timestamp for a Lucene file, keyed by file name.
     */
    @FunctionalInterface
    interface FileTimestampResolver extends TimestampResolver<String> {}

    /**
     * Resolves a timestamp for a {@link BlobFile}.
     */
    @FunctionalInterface
    interface BlobFileTimestampResolver extends TimestampResolver<BlobFile> {

        /**
         * Resolver that returns {@link SharedBlobCacheService#UNKNOWN_TIMESTAMP} for every blob.
         */
        BlobFileTimestampResolver ALL_UNKNOWN = blobFile -> SharedBlobCacheService.UNKNOWN_TIMESTAMP;

        /**
         * Adapts a {@code Map<BlobFile, Long>} into a resolver. Missing entries resolve to {@link SharedBlobCacheService#UNKNOWN_TIMESTAMP}.
         */
        static BlobFileTimestampResolver fromMap(@Nullable Map<BlobFile, Long> map) {
            if (map == null || map.isEmpty()) {
                return ALL_UNKNOWN;
            }
            return blobFile -> map.getOrDefault(blobFile, SharedBlobCacheService.UNKNOWN_TIMESTAMP);
        }
    }
}
