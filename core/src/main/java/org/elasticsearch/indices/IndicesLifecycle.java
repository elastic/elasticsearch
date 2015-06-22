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

package org.elasticsearch.indices;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.settings.IndexSettings;

/**
 * A global component allowing to register for lifecycle of an index (create/closed) and
 * an index shard (created/closed).
 */
public interface IndicesLifecycle {

    /**
     * Add a listener.
     */
    void addListener(Listener listener);

    /**
     * Remove a listener.
     */
    void removeListener(Listener listener);

    /**
     * A listener for index and index shard lifecycle events (create/closed).
     */
    public abstract static class Listener {

        /**
         * Called when the shard routing has changed state.
         *
         * @param indexShard The index shard
         * @param oldRouting The old routing state (can be null)
         * @param newRouting The new routing state
         */
        public void shardRoutingChanged(IndexShard indexShard, @Nullable ShardRouting oldRouting, ShardRouting newRouting) {

        }

        /**
         * Called on the Master node only before the index is created
         */
        public void beforeIndexAddedToCluster(Index index, @IndexSettings Settings indexSettings) {

        }

        /**
         * Called before the index gets created. Note that this is also called
         * when the index is created on data nodes
         */
        public void beforeIndexCreated(Index index, @IndexSettings Settings indexSettings) {

        }

        /**
         * Called after the index has been created.
         */
        public void afterIndexCreated(IndexService indexService) {

        }

        /**
         * Called before the index shard gets created.
         */
        public void beforeIndexShardCreated(ShardId shardId, @IndexSettings Settings indexSettings) {

        }

        /**
         * Called after the index shard has been created.
         */
        public void afterIndexShardCreated(IndexShard indexShard) {

        }

        /**
         * Called right after the shard is moved into POST_RECOVERY mode
         */
        public void afterIndexShardPostRecovery(IndexShard indexShard) {}

        /**
         * Called right before the shard is moved into POST_RECOVERY mode.
         * The shard is ready to be used but not yet marked as POST_RECOVERY.
         */
        public void beforeIndexShardPostRecovery(IndexShard indexShard) {}

        /**
         * Called after the index shard has been started.
         */
        public void afterIndexShardStarted(IndexShard indexShard) {

        }

        /**
         * Called before the index get closed.
         *
         * @param indexService The index service
         */
        public void beforeIndexClosed(IndexService indexService) {

        }

        /**
         * Called after the index has been closed.
         *
         * @param index The index
         */
        public void afterIndexClosed(Index index, @IndexSettings Settings indexSettings) {

        }

        /**
         * Called before the index shard gets closed.
         *
         * @param indexShard The index shard
         */
        public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard,
                                           @IndexSettings Settings indexSettings) {

        }

        /**
         * Called after the index shard has been closed.
         *
         * @param shardId The shard id
         */
        public void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard,
                                          @IndexSettings Settings indexSettings) {

        }

        /**
         * Called before the index shard gets deleted from disk
         * Note: this method is only executed on the first attempt of deleting the shard. Retries are will not invoke
         * this method.
         * @param shardId The shard id
         * @param indexSettings the shards index settings
         */
        public void beforeIndexShardDeleted(ShardId shardId, @IndexSettings Settings indexSettings) {
        }

        /**
         * Called after the index shard has been deleted from disk.
         *
         * Note: this method is only called if the deletion of the shard did finish without an exception
         *
         * @param shardId The shard id
         * @param indexSettings the shards index settings
         */
        public void afterIndexShardDeleted(ShardId shardId, @IndexSettings Settings indexSettings) {
        }

        /**
         * Called after a shard's {@link org.elasticsearch.index.shard.IndexShardState} changes.
         * The order of concurrent events is preserved. The execution must be lightweight.
         *
         * @param indexShard the shard the new state was applied to
         * @param previousState the previous index shard state if there was one, null otherwise
         * @param currentState the new shard state
         * @param reason the reason for the state change if there is one, null otherwise
         */
        public void indexShardStateChanged(IndexShard indexShard, @Nullable IndexShardState previousState, IndexShardState currentState, @Nullable String reason) {

        }

        /**
         * Called after the index has been deleted.
         * This listener method is invoked after {@link #afterIndexClosed(org.elasticsearch.index.Index, org.elasticsearch.common.settings.Settings)}
         * when an index is deleted
         *
         * @param index The index
         */
        public void afterIndexDeleted(Index index, @IndexSettings Settings indexSettings) {

        }

        /**
         * Called before the index gets deleted.
         * This listener method is invoked after
         * {@link #beforeIndexClosed(org.elasticsearch.index.IndexService)} when an index is deleted
         *
         * @param indexService The index service
         */
        public void beforeIndexDeleted(IndexService indexService) {

        }

        /**
         * Called when a shard is marked as inactive
         *
         * @param indexShard The shard that was marked inactive
         */
        public void onShardInactive(IndexShard indexShard) {

        }
    }

}
