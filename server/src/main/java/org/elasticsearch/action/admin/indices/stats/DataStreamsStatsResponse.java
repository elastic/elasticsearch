/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.util.List;

public class DataStreamsStatsResponse extends BroadcastResponse {

    private final int streams;
    private final int backingIndices;
    private final ByteSizeValue totalStoreSize;
    private final DataStreamStats[] dataStreamStats;

    public DataStreamsStatsResponse(int totalShards, int successfulShards, int failedShards,
                                    List<DefaultShardOperationFailedException> shardFailures, int streams, int backingIndices,
                                    ByteSizeValue totalStoreSize, DataStreamStats[] dataStreamStats) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.streams = streams;
        this.backingIndices = backingIndices;
        this.totalStoreSize = totalStoreSize;
        this.dataStreamStats = dataStreamStats;
    }

    public DataStreamsStatsResponse(StreamInput in) throws IOException {
        super(in);
        this.streams = in.readVInt();
        this.backingIndices = in.readVInt();
        this.totalStoreSize = new ByteSizeValue(in);
        this.dataStreamStats = in.readArray(DataStreamStats::new, DataStreamStats[]::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(streams);
        out.writeVInt(backingIndices);
        totalStoreSize.writeTo(out);
        out.writeArray(dataStreamStats);
    }

    public int getStreams() {
        return streams;
    }

    public int getBackingIndices() {
        return backingIndices;
    }

    public ByteSizeValue getTotalStoreSize() {
        return totalStoreSize;
    }

    public DataStreamStats[] getDataStreamStats() {
        return dataStreamStats;
    }
}
