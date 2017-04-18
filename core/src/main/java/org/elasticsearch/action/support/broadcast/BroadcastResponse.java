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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.index.shard.ShardNotFoundException;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.action.support.DefaultShardOperationFailedException.readShardOperationFailed;

/**
 * Base class for all broadcast operation based responses.
 */
public class BroadcastResponse extends ActionResponse {
    private static final ShardOperationFailedException[] EMPTY = new ShardOperationFailedException[0];
    private int totalShards;
    private int successfulShards;
    private int failedShards;
    private ShardOperationFailedException[] shardFailures = EMPTY;

    public BroadcastResponse() {
    }

    public BroadcastResponse(int totalShards, int successfulShards, int failedShards,
                             List<? extends ShardOperationFailedException> shardFailures) {
        assertNoShardNotAvailableFailures(shardFailures);
        this.totalShards = totalShards;
        this.successfulShards = successfulShards;
        this.failedShards = failedShards;
        this.shardFailures = shardFailures == null ? EMPTY :
                shardFailures.toArray(new ShardOperationFailedException[shardFailures.size()]);
    }

    private void assertNoShardNotAvailableFailures(List<? extends ShardOperationFailedException> shardFailures) {
        if (shardFailures != null) {
            for (Object e : shardFailures) {
                assert (e instanceof ShardNotFoundException) == false : "expected no ShardNotFoundException failures, but got " + e;
            }
        }
    }

    /**
     * The total shards this request ran against.
     */
    public int getTotalShards() {
        return totalShards;
    }

    /**
     * The successful shards this request was executed on.
     */
    public int getSuccessfulShards() {
        return successfulShards;
    }

    /**
     * The failed shards this request was executed on.
     */
    public int getFailedShards() {
        return failedShards;
    }

    /**
     * The REST status that should be used for the response
     */
    public RestStatus getStatus() {
        if (failedShards > 0) {
            return shardFailures[0].status();
        } else {
            return RestStatus.OK;
        }
    }

    /**
     * The list of shard failures exception.
     */
    public ShardOperationFailedException[] getShardFailures() {
        return shardFailures;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        totalShards = in.readVInt();
        successfulShards = in.readVInt();
        failedShards = in.readVInt();
        int size = in.readVInt();
        if (size > 0) {
            shardFailures = new ShardOperationFailedException[size];
            for (int i = 0; i < size; i++) {
                shardFailures[i] = readShardOperationFailed(in);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(totalShards);
        out.writeVInt(successfulShards);
        out.writeVInt(failedShards);
        out.writeVInt(shardFailures.length);
        for (ShardOperationFailedException exp : shardFailures) {
            exp.writeTo(out);
        }
    }
}
