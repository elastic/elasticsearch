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

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.store.Store;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

public abstract class AbstractIndexShard extends AbstractIndexShardComponent {

    private final Store store;
    private final ShardPath shardPath;

    private volatile ShardRouting shardRouting;
    private volatile IndexShardState state;

    protected final Object mutex = new Object();

    AbstractIndexShard(final ShardId shardId,
                       final IndexSettings indexSettings,
                       final ShardPath shardPath,
                       final Store store,
                       final ShardRouting shardRouting,
                       final IndexShardState state) {
        super(shardId, indexSettings);
        this.store = Objects.requireNonNull(store);
        this.shardPath = Objects.requireNonNull(shardPath);
        this.shardRouting = Objects.requireNonNull(shardRouting);
        this.state = Objects.requireNonNull(state);
    }

    public ShardPath shardPath() {
        return shardPath;
    }

    public Store store() {
        return store;
    }

    /**
     * @return the latest routing entry updated for this shard.
     */
    public ShardRouting routingEntry() {
        return shardRouting;
    }

    /**
     * @return the latest state updated for this shard.
     */
    public IndexShardState state() {
        return state;
    }

    /**
     * Updates the shard's routing with the given {@link ShardRouting} and returns
     * the previous routing value. This method must always be executed while the
     * current thread holds the lock on the {@code mutex} object.
     *
     * @param newShardRouting the new shard routing value
     * @return the previous shard routing
     */
    protected final ShardRouting changeShardRouting(final ShardRouting newShardRouting) {
        assert Thread.holdsLock(mutex);
        final ShardRouting previousShardRouting = shardRouting;
        logger.debug("shard routing: [{}]->[{}]", previousShardRouting, newShardRouting);
        shardRouting = newShardRouting;
        return previousShardRouting;
    }

    /**
     * Updates the state of the current shard with the given {@link IndexShardState} and returns
     * the previous shard state. This method must always be executed while the
     * current thread holds the lock on the {@code mutex} object.
     *
     * @param newState the new shard state
     * @param reason   the reason for the state change
     * @return the previous shard state
     */
    protected final IndexShardState changeState(final IndexShardState newState, final String reason) {
        assert Thread.holdsLock(mutex);
        final IndexShardState previousState = state;
        logger.debug("state: [{}]->[{}], reason [{}]", previousState, newState, reason);
        state = newState;
        afterShardStateChange(previousState, newState, reason);
        return previousState;
    }

    protected void afterShardStateChange(final IndexShardState previous, final IndexShardState current, final String reason) {}

    protected final void close(final String reason, final Closeable onClose) throws IOException {
        synchronized (mutex) {
            try {
                changeState(IndexShardState.CLOSED, reason);
            } finally {
                if (onClose != null) {
                    onClose.close();
                }
            }
        }
    }
}
