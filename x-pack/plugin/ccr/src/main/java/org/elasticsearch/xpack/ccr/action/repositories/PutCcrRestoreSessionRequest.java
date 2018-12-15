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
import org.elasticsearch.index.store.Store;

import java.io.IOException;

public class PutCcrRestoreSessionRequest extends SingleShardRequest<PutCcrRestoreSessionRequest> {

    private String sessionUUID;
    private ShardId shardId;
    private Store.MetadataSnapshot metaData;

    PutCcrRestoreSessionRequest() {
    }

    public PutCcrRestoreSessionRequest(String sessionUUID, ShardId shardId, Store.MetadataSnapshot metaData) {
        super(shardId.getIndexName());
        this.sessionUUID = sessionUUID;
        this.shardId = shardId;
        this.metaData = metaData;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        sessionUUID = in.readString();
        shardId = ShardId.readShardId(in);
        metaData = new Store.MetadataSnapshot(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sessionUUID);
        shardId.writeTo(out);
        metaData.writeTo(out);
    }

    public String getSessionUUID() {
        return sessionUUID;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public Store.MetadataSnapshot getMetaData() {
        return metaData;
    }
}
