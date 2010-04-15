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

package org.elasticsearch.action.admin.indices.gateway.snapshot;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;
import org.elasticsearch.util.io.stream.Streamable;

import java.io.IOException;

/**
 * An index level gateway snapshot response.
 *
 * @author kimchy (shay.banon)
 */
public class IndexGatewaySnapshotResponse implements ActionResponse, Streamable {

    private String index;

    private int successfulShards;

    private int failedShards;

    IndexGatewaySnapshotResponse(String index, int successfulShards, int failedShards) {
        this.index = index;
        this.successfulShards = successfulShards;
        this.failedShards = failedShards;
    }

    IndexGatewaySnapshotResponse() {

    }

    /**
     * The index the gateway snapshot has performed on.
     */
    public String index() {
        return index;
    }

    /**
     * The index the gateway snapshot has performed on.
     */
    public String getIndex() {
        return index();
    }

    /**
     * The number of successful shards the gateway snapshot operation was performed on.
     */
    public int successfulShards() {
        return successfulShards;
    }

    /**
     * The number of successful shards the gateway snapshot operation was performed on.
     */
    public int getSuccessfulShards() {
        return successfulShards();
    }

    /**
     * The number of failed shards the gateway snapshot operation was performed on.
     */
    public int failedShards() {
        return failedShards;
    }

    /**
     * The number of failed shards the gateway snapshot operation was performed on.
     */
    public int getFailedShards() {
        return failedShards();
    }

    /**
     * The number of total shards the gateway snapshot operation was performed on.
     */
    public int totalShards() {
        return successfulShards + failedShards;
    }

    /**
     * The number of total shards the gateway snapshot operation was performed on.
     */
    public int getTotalShards() {
        return totalShards();
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        index = in.readUTF();
        successfulShards = in.readVInt();
        failedShards = in.readVInt();
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(index);
        out.writeVInt(successfulShards);
        out.writeVInt(failedShards);
    }
}