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
 * Record representing an estimate of the heap used by allocated shards and ongoing merges on a particular node
 */
public record ShardHeapUsage(String nodeId, String nodeName, long totalBytes, long estimatedUsageBytes) implements Writeable {

    public ShardHeapUsage {
        assert estimatedUsageBytes <= totalBytes;
    }

    public ShardHeapUsage(StreamInput in) throws IOException {
        this(in.readString(), in.readString(), in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.nodeId);
        out.writeString(this.nodeName);
        out.writeVLong(this.totalBytes);
        out.writeVLong(this.estimatedUsageBytes);
    }

    public long estimatedFreeBytes() {
        return totalBytes - estimatedUsageBytes;
    }
}
