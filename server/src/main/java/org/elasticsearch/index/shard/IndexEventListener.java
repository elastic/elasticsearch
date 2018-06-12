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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;

/**
 * An index event listener is the primary extension point for plugins and build-in services
 * to react / listen to per-index and per-shard events. These listeners are registered per-index
 * via {@link org.elasticsearch.index.IndexModule#addIndexEventListener(IndexEventListener)}. All listeners have the same
 * lifecycle as the {@link IndexService} they are created for.
 * <p>
 * An IndexEventListener can be used across multiple indices and shards since all callback methods receive sufficient
 * local state via their arguments. Yet, if an instance is shared across indices they might be called concurrently and should not
 * modify local state without sufficient synchronization.
 * </p>
 */
public interface IndexEventListener {

    /**
     * Called when the shard routing has changed state.
     *
     * @param indexShard The index shard
     * @param oldRouting The old routing state (can be null)
     * @param newRouting The new routing state
     */
    default void shardRoutingChanged(IndexShard indexShard, @Nullable ShardRouting oldRouting, ShardRouting newRouting) {}

    /**
     * Called after the index shard has been created.
     */
    default void afterIndexShardCreated(IndexShard indexShard) {}

    /**
     * Called after the index shard has been started.
     */
    default void afterIndexShardStarted(IndexShard indexShard) {}

    /**
     * Called before the index shard gets closed.
     *
     * @param indexShard The index shard
     */
    default void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {}

    /**
     * Called after the index shard has been closed.
     *
     * @param shardId The shard id
     */
    default void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {}

    /**
     * Called after a shard's {@link org.elasticsearch.index.shard.IndexShardState} changes.
     * The order of concurrent events is preserved. The execution must be lightweight.
     *
     * @param indexShard the shard the new state was applied to
     * @param previousState the previous index shard state if there was one, null otherwise
     * @param currentState the new shard state
     * @param reason the reason for the state change if there is one, null otherwise
     */
    default void indexShardStateChanged(IndexShard indexShard, @Nullable IndexShardState previousState, IndexShardState currentState, @Nullable String reason) {}

    /**
     * Called when a shard is marked as inactive
     *
     * @param indexShard The shard that was marked inactive
     */
    default void onShardInactive(IndexShard indexShard) {}

    /**
     * Called before the index gets created. Note that this is also called
     * when the index is created on data nodes
     */
    default void beforeIndexCreated(Index index, Settings indexSettings) {

    }

    /**
     * Called after the index has been created.
     */
    default void afterIndexCreated(IndexService indexService) {

    }

    /**
     * Called before the index get closed.
     *
     * @param indexService The index service
     * @param reason       the reason for index removal
     */
    default void beforeIndexRemoved(IndexService indexService, IndexRemovalReason reason) {

    }

    /**
     * Called after the index has been removed.
     *
     * @param index The index
     * @param reason       the reason for index removal
     */
    default void afterIndexRemoved(Index index, IndexSettings indexSettings, IndexRemovalReason reason) {

    }

    /**
     * Called before the index shard gets created.
     */
    default void beforeIndexShardCreated(ShardId shardId, Settings indexSettings) {
    }

    /**
     * Called before the index shard gets deleted from disk
     * Note: this method is only executed on the first attempt of deleting the shard. Retries are will not invoke
     * this method.
     * @param shardId The shard id
     * @param indexSettings the shards index settings
     */
    default void beforeIndexShardDeleted(ShardId shardId, Settings indexSettings) {
    }

    /**
     * Called after the index shard has been deleted from disk.
     *
     * Note: this method is only called if the deletion of the shard did finish without an exception
     *
     * @param shardId The shard id
     * @param indexSettings the shards index settings
     */
    default void afterIndexShardDeleted(ShardId shardId, Settings indexSettings) {
    }

    /**
     * Called on the Master node only before the {@link IndexService} instances is created to simulate an index creation.
     * This happens right before the index and it's metadata is registered in the cluster state
     */
    default void beforeIndexAddedToCluster(Index index, Settings indexSettings) {
    }

    /**
     * Called when the given shards store is closed. The store is closed once all resource have been released on the store.
     * This implies that all index readers are closed and no recoveries are running.
     *
     * @param shardId the shard ID the store belongs to
     */
    default void onStoreClosed(ShardId shardId) {}
}
