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
 * Current shared cache capacity and committed usage for a node.
 */
public record CurrentCacheUsage(long cacheSizeInBytes, long currentCacheCommitmentInBytes) implements Writeable {

    public CurrentCacheUsage {
        assert cacheSizeInBytes >= 0 : "cacheSizeInBytes must be non-negative: " + cacheSizeInBytes;
        assert currentCacheCommitmentInBytes >= 0 : "currentCacheCommitmentInBytes must be non-negative: " + currentCacheCommitmentInBytes;
    }

    public CurrentCacheUsage(StreamInput in) throws IOException {
        this(in.readLong(), in.readLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(cacheSizeInBytes);
        out.writeLong(currentCacheCommitmentInBytes);
    }
}
