/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Extends the lifetime of shard state beyond the lifetime of an IndexShard.
 */
public class ClosedShardService {
    private static final Logger logger = LogManager.getLogger(ClosedShardService.class);

    // Active shard reader information, per shard. Readers may continue running after a shard is technically closed.
    private final Map<ShardId, Set<PrimaryTermAndGeneration>> openReadersByShardId = new ConcurrentHashMap<>();

    /**
     * Registers information about what serverless shard commits are in still in active use by search operations.
     *
     * Expected to be called whenever an {@link org.elasticsearch.index.shard.IndexShard} closes with active readers.
     */
    public void onShardClose(ShardId shardId, Set<PrimaryTermAndGeneration> openReaders) {
        if (openReaders.isEmpty()) {
            return;
        }
        openReadersByShardId.put(shardId, openReaders);
    }

    /**
     * Clears any information about serverless shard commits in use. All active search operation have finished: the Store cannot be closed
     * until all active search operations have exited, releasing the storage state.
     *
     * Expected to be called whenever an index shard {@link org.elasticsearch.index.store.Store} closes.
     */
    public void onStoreClose(ShardId shardId) {
        openReadersByShardId.remove(shardId);
    }

    /**
     * Fetches what serverless commits were still in use when the shard was closed, if any.
     *
     * @param shardId
     * @return A set of serverless commit identifiers. Can be empty.
     */
    public Set<PrimaryTermAndGeneration> getPrimaryTermAndGenerations(ShardId shardId) {
        return openReadersByShardId.getOrDefault(shardId, Set.of());
    }
}
