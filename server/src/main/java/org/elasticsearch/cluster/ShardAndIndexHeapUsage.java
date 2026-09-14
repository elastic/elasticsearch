/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Tracks a shard's heap usage, as well as any index-level heap usage overhead that should be deduplicated per node.
 */
public record ShardAndIndexHeapUsage(long shardHeapUsageBytes, long indexHeapUsageBytes, long postingsHeapUsageBytes) implements Writeable {

    public static final TransportVersion INCLUDE_POSTINGS_IN_SHARD_AND_INDEX_HEAP = TransportVersion.fromName(
        "include_postings_in_shard_and_index_heap"
    );

    /** Used when no collector-specific default is available. */
    public static final ShardAndIndexHeapUsage ZERO = new ShardAndIndexHeapUsage(0, 0, 0);

    public ShardAndIndexHeapUsage {
        assert shardHeapUsageBytes >= 0;
        assert indexHeapUsageBytes >= 0;
        assert postingsHeapUsageBytes >= 0;
    }

    public ShardAndIndexHeapUsage(StreamInput in) throws IOException {
        this(in.readLong(), in.readLong(), in.getTransportVersion().supports(INCLUDE_POSTINGS_IN_SHARD_AND_INDEX_HEAP) ? in.readLong() : 0);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(this.shardHeapUsageBytes);
        out.writeLong(this.indexHeapUsageBytes);
        if (out.getTransportVersion().supports(INCLUDE_POSTINGS_IN_SHARD_AND_INDEX_HEAP)) {
            out.writeLong(this.postingsHeapUsageBytes);
        }
    }

    public long shardHeapUsageBytesExcludingPostings() {
        return shardHeapUsageBytes - postingsHeapUsageBytes;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "{shardHeapUsageBytes="
            + shardHeapUsageBytes
            + ", indexHeapUsageBytes="
            + indexHeapUsageBytes
            + ", postingsHeapUsageBytes="
            + postingsHeapUsageBytes
            + "}";
    }
}
