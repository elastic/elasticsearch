/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * A replication request that has no more information than ReplicationRequest.
 * Unfortunately ReplicationRequest can't be declared as a type parameter
 * because it has a self referential type parameter of its own. So use this
 * instead.
 */
public class BasicReplicationRequest extends ReplicationRequest<BasicReplicationRequest> implements ReshardSplitAwareReplicationRequest {

    private final SplitShardCountSummary splitShardCountSummary;

    /**
     * Creates a new request with resolved shard id and SplitShardCountSummary (used
     * to determine if the request needs to be executed on a split shard not yet seen by the
     * coordinator that sent the request)
     */
    public BasicReplicationRequest(ShardId shardId, SplitShardCountSummary splitShardCountSummary) {
        super(shardId);
        this.splitShardCountSummary = splitShardCountSummary;
    }

    public BasicReplicationRequest(StreamInput in) throws IOException {
        super(in);
        this.splitShardCountSummary = readReshardSplitAwareSummary(in, legacySplitShardCountSummary);
    }

    @Override
    public SplitShardCountSummary splitShardCountSummary() {
        return splitShardCountSummary;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeReshardSplitAwareSummary(out, splitShardCountSummary);
    }

    @Override
    public String toString() {
        return "BasicReplicationRequest{" + shardId + "}";
    }
}
