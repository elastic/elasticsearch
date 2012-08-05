/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.action.updatebyquery;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Update by query response from a single shard.
 */
public class ShardUpdateByQueryResponse implements ActionResponse {

    private int shardId;
    private int totalHits;
    private int updated;
    private BulkItemResponse[] bulkResponses = new BulkItemResponse[0];
    private String failedShardExceptionMessage;

    ShardUpdateByQueryResponse() {
    }

    public ShardUpdateByQueryResponse(int shardId) {
        this(shardId, 0, 0, new BulkItemResponse[0]);
    }

    public ShardUpdateByQueryResponse(int shardId, int totalHits, int updated, BulkItemResponse[] bulkResponses) {
        this.shardId = shardId;
        this.totalHits = totalHits;
        this.updated = updated;
        this.bulkResponses = bulkResponses;
    }

    public ShardUpdateByQueryResponse(int shardId, String failure) {
        this.shardId = shardId;
        this.failedShardExceptionMessage = failure;
    }

    public int shardId() {
        return shardId;
    }

    public void shardId(int shardId) {
        this.shardId = shardId;
    }

    public int updated() {
        return updated;
    }

    public String failedShardExceptionMessage() {
        return failedShardExceptionMessage;
    }

    public ShardUpdateByQueryResponse failedShardExceptionMessage(String failedShardExceptionMessage) {
        this.failedShardExceptionMessage = failedShardExceptionMessage;
        return this;
    }

    public BulkItemResponse[] bulkResponses() {
        return bulkResponses;
    }

    public ShardUpdateByQueryResponse responses(BulkItemResponse[] responses) {
        this.bulkResponses = responses;
        return this;
    }

    public int totalHits() {
        return totalHits;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(shardId);
        out.writeVInt(totalHits);
        out.writeVInt(updated);
        out.writeVInt(bulkResponses.length);
        for (BulkItemResponse response : bulkResponses) {
            response.writeTo(out);
        }
        out.writeOptionalString(failedShardExceptionMessage);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        shardId = in.readVInt();
        totalHits = in.readVInt();
        updated = in.readVInt();
        bulkResponses = new BulkItemResponse[in.readVInt()];
        for (int i = 0; i < bulkResponses.length; i++) {
            bulkResponses[i] = BulkItemResponse.readBulkItem(in);
        }
        failedShardExceptionMessage = in.readOptionalString();
    }
}
