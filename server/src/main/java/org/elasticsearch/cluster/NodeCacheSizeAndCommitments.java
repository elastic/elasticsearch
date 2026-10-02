/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.RatioValue;

import java.io.IOException;

/**
 * Current cache size and boosted/unboosted cache commitments for a node.
 */
public record NodeCacheSizeAndCommitments(long cacheSizeInBytes, long boostedCacheCommitmentInBytes, long unboostedCacheCommitmentInBytes)
    implements
        Writeable {

    public NodeCacheSizeAndCommitments {
        assert cacheSizeInBytes >= 0 : "cacheSizeInBytes must be non-negative: " + cacheSizeInBytes;
        assert boostedCacheCommitmentInBytes >= 0 : "boostedCacheCommitmentInBytes must be non-negative: " + boostedCacheCommitmentInBytes;
        assert unboostedCacheCommitmentInBytes >= 0
            : "unboostedCacheCommitmentInBytes must be non-negative: " + unboostedCacheCommitmentInBytes;
    }

    public NodeCacheSizeAndCommitments(StreamInput in) throws IOException {
        this(in.readLong(), in.readLong(), in.readLong());
    }

    public long totalCacheCommitmentInBytes() {
        return Math.addExact(boostedCacheCommitmentInBytes, unboostedCacheCommitmentInBytes);
    }

    /**
     * The caller resolves {@code commitmentBytes} first, since which commitment value to compare is a policy decision outside
     * this record's concern.
     */
    public boolean exceedsWatermark(long commitmentBytes, RatioValue watermark) {
        return commitmentBytes > (long) (cacheSizeInBytes * watermark.getAsRatio());
    }

    /**
     * The bytes of spare capacity below the given watermark threshold, clamped to zero if the commitment already meets or exceeds it.
     * Uses the same threshold formula as {@link #exceedsWatermark}, so the two methods are consistent.
     */
    public long spareCapacityBytes(long commitmentBytes, RatioValue watermark) {
        return Math.max(0L, (long) (cacheSizeInBytes * watermark.getAsRatio()) - commitmentBytes);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(cacheSizeInBytes);
        out.writeLong(boostedCacheCommitmentInBytes);
        out.writeLong(unboostedCacheCommitmentInBytes);
    }
}
