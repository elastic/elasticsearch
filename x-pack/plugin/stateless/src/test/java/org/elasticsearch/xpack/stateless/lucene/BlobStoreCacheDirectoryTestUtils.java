/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.lucene;

import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.cache.reader.CacheFileReaderTestUtils;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

public class BlobStoreCacheDirectoryTestUtils {

    private BlobStoreCacheDirectoryTestUtils() {}

    public static BlobLocation getBlobLocation(BlobStoreCacheDirectory target, String fileName) {
        return target.getBlobLocation(fileName);
    }

    public static StatelessSharedBlobCacheService.CacheFile getCacheFile(BlobCacheIndexInput target) {
        return CacheFileReaderTestUtils.getCacheFile(target.getCacheFileReader());
    }

    public static StatelessSharedBlobCacheService getCacheService(BlobStoreCacheDirectory target) {
        return target.getCacheService();
    }

    public static void updateLatestUploadedBcc(SearchDirectory target, PrimaryTermAndGeneration latestUploadedBccTermAndGen) {
        target.updateLatestUploadedBcc(latestUploadedBccTermAndGen);
    }

    public static void updateLatestCommitInfo(SearchDirectory target, PrimaryTermAndGeneration ccTermAndGen, String nodeId) {
        target.updateLatestCommitInfo(ccTermAndGen, nodeId);
    }
}
