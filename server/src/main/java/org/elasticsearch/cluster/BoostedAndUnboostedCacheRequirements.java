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
 * Cache requirements for a shard, split by boosted and unboosted cache accounting.
 */
public record BoostedAndUnboostedCacheRequirements(long boostedCacheRequirementInBytes, long unboostedCacheRequirementInBytes)
    implements
        Writeable {
    /// Sentinel indicating that no cache requirement for boosted/unboosted data applies.
    public static final long NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT = -1L;

    public BoostedAndUnboostedCacheRequirements {
        assert boostedCacheRequirementInBytes >= 0 || boostedCacheRequirementInBytes == NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT
            : "boostedCacheRequirementInBytes must be non-negative or NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT (-1): "
                + boostedCacheRequirementInBytes;
        assert unboostedCacheRequirementInBytes >= 0 || unboostedCacheRequirementInBytes == NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT
            : "unboostedCacheRequirementInBytes must be non-negative or NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT (-1): "
                + unboostedCacheRequirementInBytes;
    }

    public BoostedAndUnboostedCacheRequirements(StreamInput in) throws IOException {
        this(in.readLong(), in.readLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(boostedCacheRequirementInBytes);
        out.writeLong(unboostedCacheRequirementInBytes);
    }
}
