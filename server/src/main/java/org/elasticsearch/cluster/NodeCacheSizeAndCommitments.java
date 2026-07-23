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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(cacheSizeInBytes);
        out.writeLong(boostedCacheCommitmentInBytes);
        out.writeLong(unboostedCacheCommitmentInBytes);
    }
}
