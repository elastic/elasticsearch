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

package org.elasticsearch.action.support.broadcast;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

/**
 *
 */
public abstract class BroadcastShardOperationRequest extends TransportRequest {

    private String index;
    private int shardId;

    protected BroadcastShardOperationRequest() {
    }

    protected BroadcastShardOperationRequest(String index, int shardId, BroadcastOperationRequest request) {
        super(request);
        this.index = index;
        this.shardId = shardId;
    }

    public BroadcastShardOperationRequest(String index, int shardId) {
        this.index = index;
        this.shardId = shardId;
    }

    public String index() {
        return this.index;
    }

    public int shardId() {
        return this.shardId;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readString();
        shardId = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeVInt(shardId);
    }
}
