/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.replication.ReshardSplitAwareReplicationRequest;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class ShardFlushRequest extends ReplicationRequest<ShardFlushRequest> implements ReshardSplitAwareReplicationRequest {

    private final FlushRequest request;
    private final SplitShardCountSummary splitShardCountSummary;

    /**
     * Creates a request for a resolved shard id and SplitShardCountSummary (used
     * to determine if the request needs to be executed on a split shard not yet seen by the
     * coordinator that sent the request)
     */
    public ShardFlushRequest(FlushRequest request, ShardId shardId, SplitShardCountSummary splitShardCountSummary) {
        super(shardId);
        this.splitShardCountSummary = splitShardCountSummary;
        this.request = request;
        this.waitForActiveShards = ActiveShardCount.NONE; // don't wait for any active shards before proceeding, by default
    }

    public ShardFlushRequest(StreamInput in) throws IOException {
        super(in);
        request = new FlushRequest(in);
        this.splitShardCountSummary = readReshardSplitAwareSummary(in, legacySplitShardCountSummary);
    }

    FlushRequest getRequest() {
        return request;
    }

    @Override
    public SplitShardCountSummary splitShardCountSummary() {
        return splitShardCountSummary;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
        writeReshardSplitAwareSummary(out, splitShardCountSummary);
    }

    @Override
    public String toString() {
        return "flush {" + shardId + "}";
    }
}
