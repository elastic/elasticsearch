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

import java.io.IOException;

/**
 * Cache commitments for a shard, split by boosted and unboosted cache accounting.
 */
public record BoostedAndUnboostedCacheCommitments(long boostedCacheCommitmentInBytes, long unboostedCacheCommitmentInBytes)
    implements
        Writeable {
    /// Sentinel indicating that no cache commitment for boosted/unboosted data applies.
    public static final long NO_BOOSTED_OR_UNBOOSTED_CACHE_COMMITMENT = -1L;

    public BoostedAndUnboostedCacheCommitments {
        assert boostedCacheCommitmentInBytes >= 0 || boostedCacheCommitmentInBytes == NO_BOOSTED_OR_UNBOOSTED_CACHE_COMMITMENT
            : "boostedCacheCommitmentInBytes must be non-negative or NO_BOOSTED_OR_UNBOOSTED_CACHE_COMMITMENT (-1): "
                + boostedCacheCommitmentInBytes;
        assert unboostedCacheCommitmentInBytes >= 0 || unboostedCacheCommitmentInBytes == NO_BOOSTED_OR_UNBOOSTED_CACHE_COMMITMENT
            : "unboostedCacheCommitmentInBytes must be non-negative or NO_BOOSTED_OR_UNBOOSTED_CACHE_COMMITMENT (-1): "
                + unboostedCacheCommitmentInBytes;
    }

    public BoostedAndUnboostedCacheCommitments(StreamInput in) throws IOException {
        this(in.readLong(), in.readLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(boostedCacheCommitmentInBytes);
        out.writeLong(unboostedCacheCommitmentInBytes);
    }
}
