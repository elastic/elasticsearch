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

    public PutCcrRestoreSessionRequest() {
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
    public void readFrom(StreamInput streamInput) throws IOException {
        super.readFrom(streamInput);
        sessionUUID = streamInput.readString();
        shardId = ShardId.readShardId(streamInput);
        metaData = new Store.MetadataSnapshot(streamInput);
    }

    @Override
    public void writeTo(StreamOutput streamOutput) throws IOException {
        super.writeTo(streamOutput);
        streamOutput.writeString(sessionUUID);
        shardId.writeTo(streamOutput);
        metaData.writeTo(streamOutput);
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
