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

package org.elasticsearch.indices;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;

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
         * Called before the index gets created.
         */
        public void beforeIndexCreated(Index index) {

        }

        /**
         * Called after the index has been created.
         */
        public void afterIndexCreated(IndexService indexService) {

        }

        /**
         * Called before the index shard gets created.
         */
        public void beforeIndexShardCreated(ShardId shardId) {

        }

        /**
         * Called after the index shard has been created.
         */
        public void afterIndexShardCreated(IndexShard indexShard) {

        }

        /**
         * Called after the index shard has been started.
         */
        public void afterIndexShardStarted(IndexShard indexShard) {

        }

        /**
         * Called before the index get closed.
         *
         * @param indexService The index service
         * @param delete       Does the index gets closed because of a delete command, or because the node is shutting down
         */
        public void beforeIndexClosed(IndexService indexService, boolean delete) {

        }

        /**
         * Called after the index has been closed.
         *
         * @param index  The index
         * @param delete Does the index gets closed because of a delete command, or because the node is shutting down
         */
        public void afterIndexClosed(Index index, boolean delete) {

        }

        /**
         * Called before the index shard gets closed.
         *
         * @param indexShard The index shard
         * @param delete     Does the index shard gets closed because of a delete command, or because the node is shutting down
         */
        public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, boolean delete) {

        }

        /**
         * Called after the index shard has been closed.
         *
         * @param shardId The shard id
         * @param delete  Does the index shard gets closed because of a delete command, or because the node is shutting down
         */
        public void afterIndexShardClosed(ShardId shardId, boolean delete) {

        }
    }

}
