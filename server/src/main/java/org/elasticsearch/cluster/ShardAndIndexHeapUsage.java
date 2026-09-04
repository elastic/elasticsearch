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
 * Tracks the heap usage inputs for a shard when deriving node-level heap estimates.
 *
 * @param shardHeapUsageBytes heap usage attributed directly to the shard, excluding postings heap that must be handled separately
 * @param indexHeapUsageBytes heap usage attributed to the shard's index; counted once per index on each node that hosts a shard of the index
 * @param shardPostingsHeapUsageBytes postings heap usage attributed to the shard; tracked separately because node totals use the maximum
 *                                    hosted postings heap across the estimated nodes, while hosted-shards usage uses the node-local value
 */
public record ShardAndIndexHeapUsage(long shardHeapUsageBytes, long indexHeapUsageBytes, long shardPostingsHeapUsageBytes)
    implements
        Writeable {

    public static final TransportVersion EXPLICIT_HEAP_ESTIMATE_COMPONENTS = NodeHeapEstimates.EXPLICIT_HEAP_ESTIMATE_COMPONENTS;

    /** Used when no collector-specific default is available. */
    public static final ShardAndIndexHeapUsage ZERO = new ShardAndIndexHeapUsage(0, 0, 0);

    public ShardAndIndexHeapUsage(long shardHeapUsageBytes, long indexHeapUsageBytes) {
        this(shardHeapUsageBytes, indexHeapUsageBytes, 0);
    }

    public ShardAndIndexHeapUsage {
        assert shardHeapUsageBytes >= 0;
        assert indexHeapUsageBytes >= 0;
        assert shardPostingsHeapUsageBytes >= 0;
    }

    public ShardAndIndexHeapUsage(StreamInput in) throws IOException {
        this(in.readLong(), in.readLong(), in.getTransportVersion().supports(EXPLICIT_HEAP_ESTIMATE_COMPONENTS) ? in.readLong() : 0L);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(EXPLICIT_HEAP_ESTIMATE_COMPONENTS)) {
            out.writeLong(this.shardHeapUsageBytes);
        } else {
            // Legacy readers do not have a separate postings field, so keep their effective shard heap unchanged.
            out.writeLong(Math.addExact(this.shardHeapUsageBytes, this.shardPostingsHeapUsageBytes));
        }
        out.writeLong(this.indexHeapUsageBytes);
        if (out.getTransportVersion().supports(EXPLICIT_HEAP_ESTIMATE_COMPONENTS)) {
            out.writeLong(this.shardPostingsHeapUsageBytes);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "{shardHeapUsageBytes="
            + shardHeapUsageBytes
            + ", indexHeapUsageBytes="
            + indexHeapUsageBytes
            + ", shardPostingsHeapUsageBytes="
            + shardPostingsHeapUsageBytes
            + "}";
    }
}
