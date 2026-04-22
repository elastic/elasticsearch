/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

public final class CacheFileReaderTestUtils {

    private CacheFileReaderTestUtils() {}

    public static SharedBlobCacheService<FileCacheKey>.CacheFile getCacheFile(CacheFileReader reader) {
        return reader.getCacheFile();
    }
}
