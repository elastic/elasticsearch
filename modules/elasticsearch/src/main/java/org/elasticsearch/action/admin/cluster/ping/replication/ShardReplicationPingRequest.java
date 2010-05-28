/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.ping.replication;

import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class ShardReplicationPingRequest extends ShardReplicationOperationRequest {

    private int shardId;

    public ShardReplicationPingRequest(IndexReplicationPingRequest request, int shardId) {
        this(request.index(), shardId);
        timeout = request.timeout();
        replicationType(request.replicationType());
    }

    public ShardReplicationPingRequest(String index, int shardId) {
        this.index = index;
        this.shardId = shardId;
    }

    ShardReplicationPingRequest() {
    }

    public int shardId() {
        return this.shardId;
    }

    @Override public ShardReplicationPingRequest listenerThreaded(boolean threadedListener) {
        super.listenerThreaded(threadedListener);
        return this;
    }

    @Override public ShardReplicationPingRequest operationThreaded(boolean threadedOperation) {
        super.operationThreaded(threadedOperation);
        return this;
    }

    @Override public ShardReplicationPingRequest replicationType(ReplicationType replicationType) {
        super.replicationType(replicationType);
        return this;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardId = in.readVInt();
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(shardId);
    }

    @Override public String toString() {
        return "[" + index + "][" + shardId + "]";
    }
}