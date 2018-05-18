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

package org.elasticsearch.env;

import org.elasticsearch.index.shard.ShardId;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A shard lock guarantees exclusive access to a shards data
 * directory. Internal processes should acquire a lock on a shard
 * before executing any write operations on the shards data directory.
 *
 * @see org.elasticsearch.env.NodeEnvironment
 */
public abstract class ShardLock implements Closeable {

    private final ShardId shardId;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ShardLock(ShardId id) {
        this.shardId = id;
    }

    /**
     * Returns the locks shards Id.
     */
    public final ShardId getShardId() {
        return shardId;
    }

    @Override
    public final void close() {
        if (this.closed.compareAndSet(false, true)) {
           closeInternal();
        }
    }

    protected  abstract void closeInternal();

    /**
     * Returns true if this lock is still open ie. has not been closed yet.
     */
    public final boolean isOpen() {
        return closed.get() == false;
    }

    @Override
    public String toString() {
        return "ShardLock{" +
                "shardId=" + shardId +
                '}';
    }

}
