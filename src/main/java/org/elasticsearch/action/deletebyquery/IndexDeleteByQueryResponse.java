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

package org.elasticsearch.action.deletebyquery;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * Delete by query response executed on a specific index.
 */
public class IndexDeleteByQueryResponse extends ActionResponse {

    private String index;
    private int successfulShards;
    private int failedShards;
    private ShardOperationFailedException[] failures;

    IndexDeleteByQueryResponse(String index, int successfulShards, int failedShards, List<ShardOperationFailedException> failures) {
        this.index = index;
        this.successfulShards = successfulShards;
        this.failedShards = failedShards;
        if (failures == null || failures.isEmpty()) {
            this.failures = new DefaultShardOperationFailedException[0];
        } else {
            this.failures = failures.toArray(new ShardOperationFailedException[failures.size()]);
        }
    }

    IndexDeleteByQueryResponse() {

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

    public ShardOperationFailedException[] getFailures() {
        return failures;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readString();
        successfulShards = in.readVInt();
        failedShards = in.readVInt();
        int size = in.readVInt();
        failures = new ShardOperationFailedException[size];
        for (int i = 0; i < size; i++) {
            failures[i] = DefaultShardOperationFailedException.readShardOperationFailed(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeVInt(successfulShards);
        out.writeVInt(failedShards);
        out.writeVInt(failures.length);
        for (ShardOperationFailedException failure : failures) {
            failure.writeTo(out);
        }
    }
}