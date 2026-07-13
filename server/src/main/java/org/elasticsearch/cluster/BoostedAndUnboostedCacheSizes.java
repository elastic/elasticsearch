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
 * Cache size requirements for a shard, split by boosted and unboosted cache accounting.
 */
public record BoostedAndUnboostedCacheSizes(long boostedCacheSizeInBytes, long unboostedCacheSizeInBytes) implements Writeable {
    /// Sentinel indicating that no cache size for boosted/unboosted data applies.
    public static final long NO_BOOSTED_OR_UNBOOSTED_CACHE_SIZE = -1L;

    public BoostedAndUnboostedCacheSizes {
        assert boostedCacheSizeInBytes >= 0 || boostedCacheSizeInBytes == NO_BOOSTED_OR_UNBOOSTED_CACHE_SIZE
            : "boostedCacheSizeInBytes must be non-negative or NO_BOOSTED_OR_UNBOOSTED_CACHE_SIZE (-1): " + boostedCacheSizeInBytes;
        assert unboostedCacheSizeInBytes >= 0 || unboostedCacheSizeInBytes == NO_BOOSTED_OR_UNBOOSTED_CACHE_SIZE
            : "unboostedCacheSizeInBytes must be non-negative or NO_BOOSTED_OR_UNBOOSTED_CACHE_SIZE (-1): " + unboostedCacheSizeInBytes;
    }

    public BoostedAndUnboostedCacheSizes(StreamInput in) throws IOException {
        this(in.readLong(), in.readLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(boostedCacheSizeInBytes);
        out.writeLong(unboostedCacheSizeInBytes);
    }
}
