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


package org.elasticsearch.indices.syncedflush;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

public class SyncedFlushReplicaResponse extends TransportResponse {
    boolean succeeded = true;
    private String index;
    private int shardId;
    private String nodeId;
    private String reason;

    void setResult(boolean succeeded, String index, int shardId, String nodeId, String reason) {
        this.succeeded = succeeded;
        this.index = index;
        this.shardId = shardId;
        this.nodeId = nodeId;
        this.reason = reason;
    }

    public String getIndex() {
        return index;
    }

    public int getShardId() {
        return shardId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getReason() {
        return reason;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        succeeded = in.readBoolean();
        this.index = in.readString();
        this.shardId = in.readInt();
        this.nodeId = in.readString();
        this.reason = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(succeeded);
        out.writeString(index);
        out.writeInt(shardId);
        out.writeString(nodeId);
        out.writeString(reason);
    }

}
