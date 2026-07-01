/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.lucene;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.cache.reader.CacheFileReaderTestUtils;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
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

    public static int getDesiredAdvice(BlobCacheIndexInput target) {
        return CacheFileReaderTestUtils.getDesiredAdvice(target.getCacheFileReader());
    }

    public static long getExclusiveStart(BlobCacheIndexInput target) {
        return CacheFileReaderTestUtils.getExclusiveStart(target.getCacheFileReader());
    }

    public static long getExclusiveEnd(BlobCacheIndexInput target) {
        return CacheFileReaderTestUtils.getExclusiveEnd(target.getCacheFileReader());
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

    /**
     * Opens an {@link IndexInput} for the given file, stripping any replicated ranges from the associated {@link BlobFileRanges}.
     * This forces reads to go through the original position of the file in the blob rather than using replicated content usually
     * located in the first region of the compound commit.
     */
    public static IndexInput doOpenInputWithoutReplicatedRanges(SearchDirectory target, String fileName, IOContext ioContext) {
        var blobFileRanges = target.getBlobFileRangesForFile(fileName);
        if (blobFileRanges == null) {
            throw new AssertionError("No BlobFileRanges for file [" + fileName + ']');
        }
        if (blobFileRanges.hasReplicatedRanges()) {
            blobFileRanges = new BlobFileRanges(blobFileRanges.blobLocation(), blobFileRanges.timestampRange());
            assert blobFileRanges.hasReplicatedRanges() == false;
        }
        return target.doOpenInput(fileName, ioContext, blobFileRanges);
    }
}
