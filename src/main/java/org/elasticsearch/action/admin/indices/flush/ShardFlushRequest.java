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

package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 *
 */
class ShardFlushRequest extends BroadcastShardOperationRequest {

    private boolean full;
    private boolean force;
    private boolean waitIfOngoing;

    ShardFlushRequest() {
    }

    ShardFlushRequest(ShardId shardId, FlushRequest request) {
        super(shardId, request);
        this.full = request.full();
        this.force = request.force();
        this.waitIfOngoing = request.waitIfOngoing();
    }

    public boolean full() {
        return this.full;
    }

    public boolean force() {
        return this.force;
    }

    public boolean waitIfOngoing() {
        return waitIfOngoing;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        full = in.readBoolean();
        force = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            waitIfOngoing = in.readBoolean();
        } else {
            waitIfOngoing = false;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(full);
        out.writeBoolean(force);
        if (out.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            out.writeBoolean(waitIfOngoing);
        }
    }
}
