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

import com.google.common.collect.Maps;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Map;

/**
 * Encapsulates the result of an update by query request by bundling all bulk item responses.
 * Each bulk item response holds the result of an individual update.
 */
public class IndexUpdateByQueryResponse implements ActionResponse {

    private String index;
    private long totalHits;
    private long updated;
    private Map<Integer, BulkItemResponse[]> responsesByShard = Maps.newHashMap();
    private Map<Integer, String> failuresByShard = Maps.newHashMap();

    IndexUpdateByQueryResponse() {
    }

    public IndexUpdateByQueryResponse(String index, ShardUpdateByQueryResponse[] shardResponses) {
        this.index = index;
        shardResponses(shardResponses);
    }

    public String index() {
        return index;
    }

    public String getIndex() {
        return index();
    }

    public IndexUpdateByQueryResponse index(String index) {
        this.index = index;
        return this;
    }

    public IndexUpdateByQueryResponse shardResponses(ShardUpdateByQueryResponse[] responses) {
        for (ShardUpdateByQueryResponse response : responses) {
            totalHits += response.totalHits();
            updated += response.updated();
            if (response.failedShardExceptionMessage() != null) {
                failuresByShard.put(response.shardId(), response.failedShardExceptionMessage());
            }
            if (response.bulkResponses().length != 0) {
                responsesByShard.put(response.shardId(), response.bulkResponses());
            }
        }
        return this;
    }

    public Map<Integer, BulkItemResponse[]> responsesByShard() {
        return responsesByShard;
    }

    public Map<Integer, String> failuresByShard() {
        return failuresByShard;
    }

    public long totalHits() {
        return totalHits;
    }

    public long updated() {
        return updated;
    }

    public long countShardResponses() {
        long count = 0;
        for (BulkItemResponse[] bulkItemResponses : responsesByShard.values()) {
            count += bulkItemResponses.length;
        }
        return count;
    }

    public boolean hasFailures() {
        return !failuresByShard.isEmpty();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        index = in.readString();
        totalHits = in.readVLong();
        updated = in.readVLong();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            int shardId = in.readVInt();
            BulkItemResponse[] responses = new BulkItemResponse[in.readVInt()];
            for (int j = 0; j < responses.length; j++) {
                responses[i] = BulkItemResponse.readBulkItem(in);
            }
            responsesByShard.put(shardId, responses);
        }

        size = in.readVInt();
        for (int i = 0; i < size; i++) {
            int shardId = in.readVInt();
            failuresByShard.put(shardId, in.readString());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeVLong(totalHits);
        out.writeVLong(updated);
        out.writeVInt(responsesByShard.size());
        for (Map.Entry<Integer, BulkItemResponse[]> entry : responsesByShard.entrySet()) {
            out.writeVInt(entry.getKey());
            out.writeVInt(entry.getValue().length);
            for (BulkItemResponse bulkItemResponse : entry.getValue()) {
                bulkItemResponse.writeTo(out);
            }
        }
        out.writeVInt(failuresByShard.size());
        for (Map.Entry<Integer, String> entry : failuresByShard.entrySet()) {
            out.writeVInt(entry.getKey());
            out.writeString(entry.getValue());
        }
    }

    public static IndexUpdateByQueryResponse readResponseItem(StreamInput in) throws IOException {
        IndexUpdateByQueryResponse response = new IndexUpdateByQueryResponse();
        response.readFrom(in);
        return response;
    }
}
