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

package org.elasticsearch.indices.recovery;

import com.google.common.collect.Lists;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStreams;

import java.io.IOException;
import java.util.List;

/**
 *
 */
class RecoveryTranslogOperationsRequest implements Streamable {

    private ShardId shardId;
    private List<Translog.Operation> operations;

    RecoveryTranslogOperationsRequest() {
    }

    RecoveryTranslogOperationsRequest(ShardId shardId, List<Translog.Operation> operations) {
        this.shardId = shardId;
        this.operations = operations;
    }

    public ShardId shardId() {
        return shardId;
    }

    public List<Translog.Operation> operations() {
        return operations;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        shardId = ShardId.readShardId(in);
        int size = in.readVInt();
        operations = Lists.newArrayListWithExpectedSize(size);
        for (int i = 0; i < size; i++) {
            operations.add(TranslogStreams.readTranslogOperation(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeVInt(operations.size());
        for (Translog.Operation operation : operations) {
            TranslogStreams.writeTranslogOperation(out, operation);
        }
    }
}
