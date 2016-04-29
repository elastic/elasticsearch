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

package org.elasticsearch.index.shard;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 *
 */
public class IllegalIndexShardStateException extends ElasticsearchException {

    private final IndexShardState currentState;

    public IllegalIndexShardStateException(ShardId shardId, IndexShardState currentState, String msg, Object... args) {
        this(shardId, currentState, msg, null, args);
    }

    public IllegalIndexShardStateException(ShardId shardId, IndexShardState currentState, String msg, Throwable ex, Object... args) {
        super("CurrentState[" + currentState + "] " + msg, ex, args);
        setShard(shardId);
        this.currentState = currentState;
    }

    public IndexShardState currentState() {
        return currentState;
    }

    public IllegalIndexShardStateException(StreamInput in) throws IOException{
        super(in);
        currentState = IndexShardState.fromId(in.readByte());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByte(currentState.id());
    }

    @Override
    public RestStatus status() {
        return RestStatus.NOT_FOUND;
    }
}
