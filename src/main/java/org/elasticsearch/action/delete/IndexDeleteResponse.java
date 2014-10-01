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

package org.elasticsearch.action.delete;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Delete by query response executed on a specific index.
 */
public class IndexDeleteResponse extends ActionResponse {

    private String index;
    private int successfulShards;
    private int failedShards;
    private ShardDeleteResponse[] deleteResponses;

    IndexDeleteResponse(String index, int failedShards, ShardDeleteResponse[] deleteResponses) {
        this.index = index;
        this.successfulShards = deleteResponses.length;
        this.failedShards = failedShards;
        this.deleteResponses = deleteResponses;
    }

    /**
     * The index the delete by query operation was executed against.
     */
    public String getIndex() {
        return this.index;
    }

    /**
     * The total number of shards the delete by query was executed on.
     */
    public int getTotalShards() {
        return failedShards + successfulShards;
    }

    /**
     * The successful number of shards the delete by query was executed on.
     */
    public int getSuccessfulShards() {
        return successfulShards;
    }

    /**
     * The failed number of shards the delete by query was executed on.
     */
    public int getFailedShards() {
        return failedShards;
    }

    public ShardDeleteResponse[] getResponses() {
        return this.deleteResponses;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("IndexDeleteResponse is not supposed to be sent over the transport");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("IndexDeleteResponse is not supposed to be sent over the transport");
    }
}