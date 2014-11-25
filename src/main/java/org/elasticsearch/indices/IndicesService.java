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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;

import java.util.List;

/**
 *
 */
public interface IndicesService extends Iterable<IndexService>, LifecycleComponent<IndicesService> {

    /**
     * Returns <tt>true</tt> if changes (adding / removing) indices, shards and so on are allowed.
     */
    public boolean changesAllowed();

    /**
     * Returns the node stats indices stats. The <tt>includePrevious</tt> flag controls
     * if old shards stats will be aggregated as well (only for relevant stats, such as
     * refresh and indexing, not for docs/store).
     */
    NodeIndicesStats stats(boolean includePrevious);

    NodeIndicesStats stats(boolean includePrevious, CommonStatsFlags flags);

    boolean hasIndex(String index);

    IndicesLifecycle indicesLifecycle();

    /**
     * Returns a snapshot of the started indices and the associated {@link IndexService} instances.
     *
     * The map being returned is not a live view and subsequent calls can return a different view.
     */
    ImmutableMap<String, IndexService> indices();

    /**
     * Returns an IndexService for the specified index if exists otherwise returns <code>null</code>.
     *
     * Even if the index name appeared in {@link #indices()} <code>null</code> can still be returned as an
     * index maybe removed in the meantime, so preferable use the associated {@link IndexService} in order to prevent NPE.
     */
    @Nullable
    IndexService indexService(String index);

    /**
     * Returns an IndexService for the specified index if exists otherwise a {@link IndexMissingException} is thrown.
     */
    IndexService indexServiceSafe(String index) throws IndexMissingException;

    IndexService createIndex(String index, Settings settings, String localNodeId) throws ElasticsearchException;

    /**
     * Removes the given index from this service and releases all associated resources. Persistent parts of the index
     * like the shards files, state and transaction logs are kept around in the case of a disaster recovery.
     * @param index the index to remove
     * @param reason  the high level reason causing this removal
     */
    void removeIndex(String index, String reason) throws ElasticsearchException;

    /**
     * Deletes the given index. Persistent parts of the index
     * like the shards files, state and transaction logs are removed once all resources are released.
     *
     * Equivalent to {@link #removeIndex(String, String)} but fires
     * different lifecycle events to ensure pending resources of this index are immediately removed.
     * @param index the index to delete
     * @param reason the high level reason causing this delete
     */
    void deleteIndex(String index, String reason) throws ElasticsearchException;

    /**
     * A listener interface that can be used to get notification once a shard or all shards
     * of an certain index that are allocated on a node are actually closed. The listener methods
     * are invoked once the actual low level instance modifying or reading a shard are closed in contrast to
     * removal methods that might return earlier.
     */
    public static interface IndexCloseListener {

        /**
         * Invoked once all shards are closed or their closing failed.
         * @param index the index that got closed
         * @param failures the recorded shard closing failures
         */
        public void onAllShardsClosed(Index index, List<Throwable> failures);

        /**
         * Invoked once the last resource using the given shard ID is released.
         * Yet, this method is called while still holding the shards lock such that
         * operations on the shards data can safely be executed in this callback.
         */
        public void onShardClosed(ShardId shardId);

        /**
         * Invoked if closing the given shard failed.
         */
        public void onShardCloseFailed(ShardId shardId, Throwable t);

    }
}
