/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

public class ReplicaShardRefreshRequest extends ReplicationRequest<ReplicaShardRefreshRequest> {

    @Nullable
    private final Long segmentGeneration;

    public ReplicaShardRefreshRequest(ShardId shardId, TaskId parentTaskId, @Nullable Long segmentGeneration) {
        super(shardId);
        setParentTask(parentTaskId);
        this.segmentGeneration = segmentGeneration;
    }

    public ReplicaShardRefreshRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
            this.segmentGeneration = in.readOptionalVLong();
        } else {
            this.segmentGeneration = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
            out.writeOptionalVLong(segmentGeneration);
        }
    }

    @Nullable
    public Long getSegmentGeneration() {
        return segmentGeneration;
    }

    @Override
    public String toString() {
        return "ReplicaShardRefreshRequest{" + shardId + '}';
    }
}
