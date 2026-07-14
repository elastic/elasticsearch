/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.UNKNOWN_TIMESTAMP;

/**
 * Helper class to accumulate the information needed to backfill the cache-region timestamps after a BCC/CC metadata read of a single blob.
 * Instances are not thread-safe; a single metadata read of one blob uses one instance from one thread.
 */
public final class MetadataReadTimestampBackfill {

    private final StatelessSharedBlobCacheService cacheService;
    private final FileCacheKey cacheKey;
    private final boolean indexHasTimestampField;

    private long mostRecentTimestamp = UNKNOWN_TIMESTAMP;

    private long lastCommitOffset = -1L;
    private long lastCommitHeaderSize;
    private long lastCommitSizeInBytes;

    public MetadataReadTimestampBackfill(
        StatelessSharedBlobCacheService cacheService,
        FileCacheKey cacheKey,
        boolean indexHasTimestampField
    ) {
        this.cacheService = cacheService;
        this.cacheKey = cacheKey;
        this.indexHasTimestampField = indexHasTimestampField;
    }

    public void mergeCc(BatchedCompoundCommit bcc) {
        long commitOffset = 0L;
        for (StatelessCompoundCommit compoundCommit : bcc.compoundCommits()) {
            mergeCc(commitOffset, compoundCommit);
            commitOffset += BlobCacheUtils.toPageAlignedSize(compoundCommit.sizeInBytes());
        }
    }

    public void mergeCc(long commitOffset, StatelessCompoundCommit compoundCommit) {
        mostRecentTimestamp = BlobFileRanges.mostRecentKnownTimestamp(
            mostRecentTimestamp,
            BlobFileRanges.midpointMillisOrUnknownForCache(compoundCommit.getTimestampFieldValueRange())
        );
        if (commitOffset >= lastCommitOffset) {
            lastCommitOffset = commitOffset;
            lastCommitHeaderSize = compoundCommit.headerSizeInBytes();
            lastCommitSizeInBytes = compoundCommit.sizeInBytes();
        }
    }

    public void finalizeAndBackfill() {
        if (lastCommitOffset < 0L) {
            return;
        }
        final long resolved;
        if (mostRecentTimestamp != UNKNOWN_TIMESTAMP) {
            resolved = mostRecentTimestamp;
        } else if (indexHasTimestampField) {
            resolved = 1L;
        } else {
            return;
        }
        int lastRegion = cacheService.getEndingRegion(lastCommitOffset + lastCommitHeaderSize);
        if (Assertions.ENABLED) {
            /// to cover for assertion-only reads in [BatchedCompoundCommit#assertPaddingComposedOfZeros]
            lastRegion = Math.max(lastRegion, cacheService.getEndingRegion(lastCommitOffset + lastCommitSizeInBytes));
        }
        cacheService.backfillRegionTimestamps(cacheKey, 0, lastRegion, resolved);
    }

    // visible for testing
    long mostRecentTimestamp() {
        return mostRecentTimestamp;
    }

    // visible for testing
    long lastCommitOffset() {
        return lastCommitOffset;
    }
}
