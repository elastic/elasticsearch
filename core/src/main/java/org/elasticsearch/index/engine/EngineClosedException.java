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

package org.elasticsearch.index.engine;

import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;

/**
 * An engine is already closed.
 * <p/>
 * <p>Note, the relationship between shard and engine indicates that engine closed is shard closed, and
 * we might get something slipping through the the shard and into the engine while the shard is closing.
 *
 *
 */
public class EngineClosedException extends IndexShardClosedException {

    public EngineClosedException(ShardId shardId) {
        super(shardId);
    }

    public EngineClosedException(ShardId shardId, Throwable t) {
        super(shardId, t);
    }
}
