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
 * Record representing an estimate of a node's write load. The estimation is based on the usage of the node's write thread pool.
 *
 * @param nodeId Node ID.
 * @param totalWriteThreadPoolThreads Total number of threads in the write thread pool.
 * @param percentWriteThreadPoolUtilization Percent of write thread pool threads that are in use, averaged over some period of time.
 * @param maxTaskTimeInWriteQueueMillis How long the oldest task (next to be run) in the write thread pool queue has been queued. Zero if
 *                                      there is no write thread pool queue.
 */
public record NodeWriteLoad(
    String nodeId,
    int totalWriteThreadPoolThreads,
    int percentWriteThreadPoolUtilization,
    long maxTaskTimeInWriteQueueMillis
) implements Writeable {

    public NodeWriteLoad(StreamInput in) throws IOException {
        this(in.readString(), in.readVInt(), in.readVInt(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.nodeId);
        out.writeVInt(this.totalWriteThreadPoolThreads);
        out.writeVInt(this.percentWriteThreadPoolUtilization);
        out.writeVLong(this.maxTaskTimeInWriteQueueMillis);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeWriteLoad other = (NodeWriteLoad) o;
        return nodeId.equals(other.nodeId)
            && totalWriteThreadPoolThreads == other.totalWriteThreadPoolThreads
            && percentWriteThreadPoolUtilization == other.percentWriteThreadPoolUtilization
            && maxTaskTimeInWriteQueueMillis == other.maxTaskTimeInWriteQueueMillis;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "{nodeId=["
            + nodeId
            + "], totalWriteThreadPoolThreads=["
            + totalWriteThreadPoolThreads
            + "], percentWriteThreadPoolUtilization=["
            + percentWriteThreadPoolUtilization
            + "], maxTaskTimeInWriteQueueMillis=["
            + maxTaskTimeInWriteQueueMillis
            + "]}";
    }

}
