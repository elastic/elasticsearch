/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.truncate;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.replication.ReshardSplitAwareReplicationRequest;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * Truncates one shard copy. The request that reaches the primary has no sequence number; the primary assigns one when it truncates,
 * and the request it replicates to the other copies carries it so that every copy places the truncate at the same point in history.
 */
public class ShardTruncateRequest extends ReplicationRequest<ShardTruncateRequest> implements ReshardSplitAwareReplicationRequest {

    private final SplitShardCountSummary splitShardCountSummary;
    private final long seqNo;

    /**
     * Creates the request that is sent to the primary.
     */
    public ShardTruncateRequest(ShardId shardId, SplitShardCountSummary splitShardCountSummary) {
        this(shardId, splitShardCountSummary, SequenceNumbers.UNASSIGNED_SEQ_NO);
    }

    private ShardTruncateRequest(ShardId shardId, SplitShardCountSummary splitShardCountSummary, long seqNo) {
        super(shardId);
        this.splitShardCountSummary = splitShardCountSummary;
        this.seqNo = seqNo;
        this.waitForActiveShards = ActiveShardCount.NONE; // don't wait for any active shards before proceeding, by default
    }

    public ShardTruncateRequest(StreamInput in) throws IOException {
        super(in);
        this.splitShardCountSummary = readReshardSplitAwareSummary(in, legacySplitShardCountSummary);
        this.seqNo = in.readZLong();
    }

    /**
     * Returns the request to replicate once the primary truncated and was assigned {@code assignedSeqNo}.
     */
    ShardTruncateRequest forReplicas(long assignedSeqNo) {
        final ShardTruncateRequest replicaRequest = new ShardTruncateRequest(shardId, splitShardCountSummary, assignedSeqNo);
        replicaRequest.timeout(timeout());
        // the replication layer requires the request sent to replicas to belong to the same task tree as the primary's request
        replicaRequest.setParentTask(getParentTask());
        return replicaRequest;
    }

    /**
     * The sequence number the primary assigned to the truncate, or {@link SequenceNumbers#UNASSIGNED_SEQ_NO} on the way to the primary.
     */
    long seqNo() {
        return seqNo;
    }

    @Override
    public SplitShardCountSummary splitShardCountSummary() {
        return splitShardCountSummary;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeReshardSplitAwareSummary(out, splitShardCountSummary);
        out.writeZLong(seqNo);
    }

    @Override
    public String toString() {
        return "truncate {" + shardId + ", seqNo=" + seqNo + "}";
    }
}
