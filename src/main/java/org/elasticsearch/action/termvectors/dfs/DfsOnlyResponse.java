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

package org.elasticsearch.action.termvectors.dfs;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.dfs.AggregatedDfs;

import java.io.IOException;
import java.util.List;

/**
 * A response of a dfs only request.
 */
public class DfsOnlyResponse extends BroadcastOperationResponse {

    private AggregatedDfs dfs;
    private long tookInMillis;

    DfsOnlyResponse(AggregatedDfs dfs, int totalShards, int successfulShards, int failedShards,
                    List<ShardOperationFailedException> shardFailures, long tookInMillis) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.dfs = dfs;
        this.tookInMillis = tookInMillis;
    }

    public AggregatedDfs getDfs() {
        return dfs;
    }

    public TimeValue getTook() {
        return new TimeValue(tookInMillis);
    }

    public long getTookInMillis() {
        return tookInMillis;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        AggregatedDfs.readAggregatedDfs(in);
        tookInMillis = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        dfs.writeTo(out);
        out.writeVLong(tookInMillis);
    }

}
