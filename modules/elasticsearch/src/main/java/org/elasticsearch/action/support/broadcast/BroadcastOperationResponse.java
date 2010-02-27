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

package org.elasticsearch.action.support.broadcast;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for all broadcast operation based responses.
 *
 * @author kimchy (shay.banon)
 */
public abstract class BroadcastOperationResponse implements ActionResponse {

    private int successfulShards;

    private int failedShards;

    private List<ShardOperationFailedException> shardFailures = ImmutableList.of();

    protected BroadcastOperationResponse() {
    }

    protected BroadcastOperationResponse(int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures) {
        this.successfulShards = successfulShards;
        this.failedShards = failedShards;
        this.shardFailures = shardFailures;
        if (shardFailures == null) {
            this.shardFailures = ImmutableList.of();
        }
    }

    /**
     * The total shards this request ran against.
     */
    public int totalShards() {
        return successfulShards + failedShards;
    }

    /**
     * The successful shards this request was executed on.
     */
    public int successfulShards() {
        return successfulShards;
    }

    /**
     * The failed shards this request was executed on.
     */
    public int failedShards() {
        return failedShards;
    }

    /**
     * The list of shard failures exception.
     */
    public List<? extends ShardOperationFailedException> shardFailures() {
        if (shardFailures == null) {
            return ImmutableList.of();
        }
        return shardFailures;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        successfulShards = in.readInt();
        failedShards = in.readInt();
        int size = in.readInt();
        if (size > 0) {
            shardFailures = new ArrayList<ShardOperationFailedException>(size);
            for (int i = 0; i < size; i++) {
                shardFailures.add(DefaultShardOperationFailedException.readShardOperationFailed(in));
            }
        }
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeInt(successfulShards);
        out.writeInt(failedShards);
        out.writeInt(shardFailures.size());
        for (ShardOperationFailedException exp : shardFailures) {
            exp.writeTo(out);
        }
    }
}
