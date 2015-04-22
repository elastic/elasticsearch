/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.action.admin.indices.synccommit;

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Arrays;

/**
 */
public class PreSyncedFlushRequest extends BroadcastOperationRequest<PreSyncedFlushRequest> {
    private ShardId shardId;
    private boolean force = true;
    private boolean waitIfOngoing = true;


    PreSyncedFlushRequest() {
    }

    public PreSyncedFlushRequest(ShardId shardId) {
        super(Arrays.asList(shardId.getIndex()).toArray(new String[0]));
        this.shardId = shardId;
    }

    /**
     * Returns <tt>true</tt> iff a flush should block
     * if a another flush operation is already running. Otherwise <tt>false</tt>
     */
    public boolean waitIfOngoing() {
        return this.waitIfOngoing;
    }

    /**
     * if set to <tt>true</tt> the flush will block
     * if a another flush operation is already running until the flush can be performed.
     */
    public PreSyncedFlushRequest waitIfOngoing(boolean waitIfOngoing) {
        this.waitIfOngoing = waitIfOngoing;
        return this;
    }

    /**
     * Force flushing, even if one is possibly not needed.
     */
    public boolean force() {
        return force;
    }

    /**
     * Force flushing, even if one is possibly not needed.
     */
    public PreSyncedFlushRequest force(boolean force) {
        this.force = force;
        return this;
    }

    @Override
    public String toString() {
        return "SyncCommit{" +
                "waitIfOngoing=" + waitIfOngoing() +
                ", force=" + force() + "}";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(force);
        out.writeBoolean(waitIfOngoing);
        shardId.writeTo(out);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        force = in.readBoolean();
        waitIfOngoing = in.readBoolean();
        this.shardId = ShardId.readShardId(in);
    }

    public ShardId shardId() {
        return shardId;
    }

    public void shardId(ShardId shardId) {
        this.shardId = shardId;
    }
}
