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

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.Snapshot;

import java.io.IOException;
import java.util.Objects;

public class GetShardSnapshotCommitInfoRequest extends ActionRequest {

    private final Snapshot snapshot;
    private final ShardId shardId;

    public GetShardSnapshotCommitInfoRequest(ShardId shardId, Snapshot snapshot) {
        this.shardId = Objects.requireNonNull(shardId);
        this.snapshot = Objects.requireNonNull(snapshot);
    }

    public GetShardSnapshotCommitInfoRequest(StreamInput in) throws IOException {
        super(in);
        this.shardId = new ShardId(in);
        this.snapshot = new Snapshot(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        snapshot.writeTo(out);
    }

    public ShardId shardId() {
        return shardId;
    }

    public Snapshot snapshot() {
        return snapshot;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
