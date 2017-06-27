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

import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class IndexShardClosedException extends IllegalIndexShardStateException {
    public IndexShardClosedException(ShardId shardId) {
        super(shardId, IndexShardState.CLOSED, "Closed");
    }

    public IndexShardClosedException(ShardId shardId, Throwable t) {
        super(shardId, IndexShardState.CLOSED, "Closed", t);
    }

    public IndexShardClosedException(ShardId shardId, String message) {
        super(shardId, IndexShardState.CLOSED, message);
    }

    public IndexShardClosedException(StreamInput in) throws IOException{
        super(in);
    }
}
