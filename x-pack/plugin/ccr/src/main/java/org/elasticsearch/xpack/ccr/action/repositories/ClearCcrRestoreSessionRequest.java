/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class ClearCcrRestoreSessionRequest extends BaseNodesRequest<ClearCcrRestoreSessionRequest> {

    private Request request;

    ClearCcrRestoreSessionRequest() {
    }

    public ClearCcrRestoreSessionRequest(String nodeId, Request request) {
        super(nodeId);
        this.request = request;
    }

    @Override
    public void readFrom(StreamInput streamInput) throws IOException {
        super.readFrom(streamInput);
        request = new Request();
        request.readFrom(streamInput);
    }

    @Override
    public void writeTo(StreamOutput streamOutput) throws IOException {
        super.writeTo(streamOutput);
        request.writeTo(streamOutput);
    }

    public Request getRequest() {
        return request;
    }

    public static class Request extends BaseNodeRequest {

        private String sessionUUID;
        private ShardId shardId;


        Request() {
        }

        public Request(String nodeId, String sessionUUID, ShardId shardId) {
            super(nodeId);
            this.sessionUUID = sessionUUID;
            this.shardId = shardId;
        }

        @Override
        public void readFrom(StreamInput streamInput) throws IOException {
            super.readFrom(streamInput);
            sessionUUID = streamInput.readString();
            shardId = ShardId.readShardId(streamInput);
        }

        @Override
        public void writeTo(StreamOutput streamOutput) throws IOException {
            super.writeTo(streamOutput);
            streamOutput.writeString(sessionUUID);
            shardId.writeTo(streamOutput);
        }

        public String getSessionUUID() {
            return sessionUUID;
        }

        public ShardId getShardId() {
            return shardId;
        }
    }
}
