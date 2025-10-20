/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.reshard;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class SplitStateRequest extends MasterNodeRequest<SplitStateRequest> {

    private final ShardId shardId;
    private final long sourcePrimaryTerm;
    private final long targetPrimaryTerm;
    private final IndexReshardingState.Split.TargetShardState newTargetShardState;

    public SplitStateRequest(
        ShardId shardId,
        IndexReshardingState.Split.TargetShardState newTargetShardState,
        long sourcePrimaryTerm,
        long targetPrimaryTerm
    ) {
        super(INFINITE_MASTER_NODE_TIMEOUT);
        this.shardId = shardId;
        this.newTargetShardState = newTargetShardState;
        this.sourcePrimaryTerm = sourcePrimaryTerm;
        this.targetPrimaryTerm = targetPrimaryTerm;
    }

    public SplitStateRequest(StreamInput in) throws IOException {
        super(in);
        shardId = new ShardId(in);
        newTargetShardState = IndexReshardingState.Split.TargetShardState.readFrom(in);
        sourcePrimaryTerm = in.readVLong();
        targetPrimaryTerm = in.readVLong();
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public ShardId getShardId() {
        return shardId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        newTargetShardState.writeTo(out);
        out.writeVLong(sourcePrimaryTerm);
        out.writeVLong(targetPrimaryTerm);
    }

    public long getSourcePrimaryTerm() {
        return sourcePrimaryTerm;
    }

    public long getTargetPrimaryTerm() {
        return targetPrimaryTerm;
    }

    public IndexReshardingState.Split.TargetShardState getNewTargetShardState() {
        return newTargetShardState;
    }
}
