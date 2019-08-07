/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class PutCcrRestoreSessionRequest extends SingleShardRequest<PutCcrRestoreSessionRequest> {

    private final String sessionUUID;
    private final ShardId shardId;

    PutCcrRestoreSessionRequest(StreamInput in) throws IOException {
        super(in);
        sessionUUID = in.readString();
        shardId = new ShardId(in);
    }

    public PutCcrRestoreSessionRequest(String sessionUUID, ShardId shardId) {
        super(shardId.getIndexName());
        this.sessionUUID = sessionUUID;
        this.shardId = shardId;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sessionUUID);
        shardId.writeTo(out);
    }

    String getSessionUUID() {
        return sessionUUID;
    }

    ShardId getShardId() {
        return shardId;
    }
}
