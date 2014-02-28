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

package org.elasticsearch.action.admin.indices.optimize;


import org.elasticsearch.Version;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 *
 */
class ShardOptimizeRequest extends BroadcastShardOperationRequest {

    private boolean waitForMerge = OptimizeRequest.Defaults.WAIT_FOR_MERGE;
    private int maxNumSegments = OptimizeRequest.Defaults.MAX_NUM_SEGMENTS;
    private boolean onlyExpungeDeletes = OptimizeRequest.Defaults.ONLY_EXPUNGE_DELETES;
    private boolean flush = OptimizeRequest.Defaults.FLUSH;
    private boolean force = OptimizeRequest.Defaults.FORCE;

    ShardOptimizeRequest() {
    }

    public ShardOptimizeRequest(String index, int shardId, OptimizeRequest request) {
        super(index, shardId, request);
        waitForMerge = request.waitForMerge();
        maxNumSegments = request.maxNumSegments();
        onlyExpungeDeletes = request.onlyExpungeDeletes();
        flush = request.flush();
    }

    boolean waitForMerge() {
        return waitForMerge;
    }

    int maxNumSegments() {
        return maxNumSegments;
    }

    public boolean onlyExpungeDeletes() {
        return onlyExpungeDeletes;
    }

    public boolean flush() {
        return flush;
    }

    public boolean force() {
        return force;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        waitForMerge = in.readBoolean();
        maxNumSegments = in.readInt();
        onlyExpungeDeletes = in.readBoolean();
        flush = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_1_1_0)) {
            force = in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(waitForMerge);
        out.writeInt(maxNumSegments);
        out.writeBoolean(onlyExpungeDeletes);
        out.writeBoolean(flush);
        if (out.getVersion().onOrAfter(Version.V_1_1_0)) {
            out.writeBoolean(force);
        }
    }
}
