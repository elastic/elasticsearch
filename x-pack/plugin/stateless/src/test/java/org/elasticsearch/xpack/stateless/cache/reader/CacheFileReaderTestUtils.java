/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.apache.lucene.store.IOContext;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

public final class CacheFileReaderTestUtils {

    private CacheFileReaderTestUtils() {}

    public static SharedBlobCacheService<FileCacheKey>.CacheFile getCacheFile(CacheFileReader reader) {
        return reader.getCacheFile();
    }

    public static int getDesiredAdvice(CacheFileReader reader) {
        return reader.getDesiredMAdvice();
    }

    public static long getExclusiveStart(CacheFileReader reader) {
        return reader.getExclusiveStart();
    }

    public static long getExclusiveEnd(CacheFileReader reader) {
        return reader.getExclusiveEnd();
    }

    public static int adviceForRange(CacheFileReader reader, ByteRange range) {
        return reader.adviceForRange(range);
    }

    public static boolean isMadviseRandomEnabled() {
        return CacheFileReader.MADVISE_RANDOM_FEATURE_FLAG.isEnabled();
    }

    public static int contextToAdvice(IOContext context, boolean hasSearchRole) {
        return CacheFileReader.contextToAdvice(context, hasSearchRole);
    }

    public static long roundUpToRegion(long offset, int regionSize) {
        return CacheFileReader.roundUpToRegion(offset, regionSize);
    }

    public static long roundDownToRegion(long offset, int regionSize) {
        return CacheFileReader.roundDownToRegion(offset, regionSize);
    }
}
