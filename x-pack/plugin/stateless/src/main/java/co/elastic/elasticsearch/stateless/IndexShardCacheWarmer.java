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

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService;
import co.elastic.elasticsearch.stateless.commits.BatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectory;
import co.elastic.elasticsearch.stateless.lucene.IndexDirectory;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardNotRecoveringException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.threadpool.ThreadPool;

public class IndexShardCacheWarmer {

    private final ObjectStoreService objectStoreService;
    private final SharedBlobCacheWarmingService warmingService;
    private final ThreadPool threadPool;

    public IndexShardCacheWarmer(
        ObjectStoreService objectStoreService,
        SharedBlobCacheWarmingService warmingService,
        ThreadPool threadPool
    ) {
        this.objectStoreService = objectStoreService;
        this.warmingService = warmingService;
        this.threadPool = threadPool;
    }

    /**
     * Schedule the pre-warming of a stateless index shard
     *
     * @param description A description of the warming being performed, will appear in log messages
     * @param indexShard The shard to warm
     * @param listener Completed with true when warming was started, false when no commit was discovered or the shard store was closed
     */
    public void preWarmIndexShardCache(String description, IndexShard indexShard, ActionListener<Boolean> listener) {
        final IndexShardState currentState = indexShard.state(); // single volatile read
        if (currentState == IndexShardState.CLOSED) {
            throw new IndexShardNotRecoveringException(indexShard.shardId(), currentState);
        }
        assert currentState == IndexShardState.RECOVERING
            : "expected a recovering shard " + indexShard.shardId() + " but got " + currentState;
        assert indexShard.routingEntry().isSearchable() == false && indexShard.routingEntry().isPromotableToPrimary()
            : "only stateless ingestion shards are supported";
        assert indexShard.recoveryState().getRecoverySource().getType() == RecoverySource.Type.PEER : "Only peer recoveries are supported";
        threadPool.generic().execute(() -> doPreWarmIndexShardCache(description, indexShard, listener));
    }

    private void doPreWarmIndexShardCache(String description, IndexShard indexShard, ActionListener<Boolean> listener) {
        final Store store = indexShard.store();
        if (store.tryIncRef()) {
            ActionListener.completeWith(ActionListener.runBefore(listener, store::decRef), () -> {
                final var blobStore = objectStoreService.blobStore();
                final ShardId shardId = indexShard.shardId();
                final var shardBasePath = objectStoreService.shardBasePath(shardId);
                BlobStoreCacheDirectory.unwrapDirectory(store.directory())
                    .setBlobContainer(primaryTerm -> blobStore.blobContainer(shardBasePath.add(String.valueOf(primaryTerm))));
                final BlobContainer existingBlobContainer = blobStore.blobContainer(shardBasePath);
                final BatchedCompoundCommit batchedCompoundCommit = ObjectStoreService.readIndexingShardState(
                    existingBlobContainer,
                    indexShard.getOperationPrimaryTerm()
                ).latestCommit();
                if (batchedCompoundCommit != null) {
                    assert indexShard.routingEntry().isPromotableToPrimary();
                    StatelessCompoundCommit last = batchedCompoundCommit.last();
                    // We read from the directory as part of warming, so we need to update
                    // the cache directory commit
                    IndexDirectory.unwrapDirectory(store.directory()).updateMetadataForPreWarming(last);
                    warmingService.warmCacheForShardRecovery(description, indexShard, last);
                    return true;
                }
                return false;
            });
        } else {
            listener.onResponse(false);
        }
    }
}
