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
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.lucene.IndexDirectory;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardNotRecoveringException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.FileNotFoundException;
import java.nio.file.NoSuchFileException;

import static co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService.Type.INDEXING_EARLY;

public class IndexShardCacheWarmer {

    private static final Logger logger = LogManager.getLogger(IndexShardCacheWarmer.class);

    private final ObjectStoreService objectStoreService;
    private final SharedBlobCacheWarmingService warmingService;
    private final ThreadPool threadPool;
    private final boolean useReplicatedRanges;

    public IndexShardCacheWarmer(
        ObjectStoreService objectStoreService,
        SharedBlobCacheWarmingService warmingService,
        ThreadPool threadPool,
        boolean useReplicatedRanges
    ) {
        this.objectStoreService = objectStoreService;
        this.warmingService = warmingService;
        this.threadPool = threadPool;
        this.useReplicatedRanges = useReplicatedRanges;
    }

    /**
     * Schedule the pre-warming of a stateless index shard
     *
     * @param indexShard The shard to warm
     */
    public void preWarmIndexShardCache(IndexShard indexShard) {
        final IndexShardState currentState = indexShard.state(); // single volatile read
        if (currentState == IndexShardState.CLOSED) {
            throw new IndexShardNotRecoveringException(indexShard.shardId(), currentState);
        }
        assert currentState == IndexShardState.RECOVERING
            : "expected a recovering shard " + indexShard.shardId() + " but got " + currentState;
        assert indexShard.routingEntry().isSearchable() == false && indexShard.routingEntry().isPromotableToPrimary()
            : "only stateless ingestion shards are supported";
        assert indexShard.recoveryState().getRecoverySource().getType() == RecoverySource.Type.PEER : "Only peer recoveries are supported";
        threadPool.generic().execute(() -> doPreWarmIndexShardCache(indexShard));
    }

    private void doPreWarmIndexShardCache(IndexShard indexShard) {
        assert indexShard.routingEntry().isPromotableToPrimary();
        final Store store = indexShard.store();
        if (store.tryIncRef()) {
            boolean success = false;
            try {
                final var blobStore = objectStoreService.blobStore();
                final var shardBasePath = objectStoreService.shardBasePath(indexShard.shardId());
                var indexDirectory = IndexDirectory.unwrapDirectory(store.directory());
                // Recovery hasn't even started yet, so we need to set the blob container here in a copied prewarming instance. This
                // instance will also be copied when reading the last BCC and other referenced blobs, so it is OK to use it for warming
                // purpose once the last BCC is known.
                var prewarmingDirectory = indexDirectory.createNewInstance();
                prewarmingDirectory.setBlobContainer(
                    primaryTerm -> blobStore.blobContainer(shardBasePath.add(String.valueOf(primaryTerm)))
                );
                ObjectStoreService.readIndexingShardState(
                    prewarmingDirectory,
                    blobStore.blobContainer(shardBasePath),
                    indexShard.getOperationPrimaryTerm(),
                    threadPool,
                    useReplicatedRanges,
                    ActionListener.releaseAfter(ActionListener.wrap(state -> {
                        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);

                        var batchedCompoundCommit = state.latestCommit();
                        if (batchedCompoundCommit != null) {
                            StatelessCompoundCommit last = batchedCompoundCommit.lastCompoundCommit();
                            // We do not want to update the internal directory metadata this early as this gets dispatched
                            // into the GENERIC thread pool and, it can make the directory to go backwards if the recovery
                            // makes progress before this task gets executed, for that reason we reuse the copied directory
                            // instance that will be used _only_ during pre-warming.
                            prewarmingDirectory.updateMetadata(state.blobFileRanges(), last.getAllFilesSizeInBytes());
                            warmingService.warmCacheForShardRecovery(INDEXING_EARLY, indexShard, last, prewarmingDirectory);
                        }
                    }, e -> logException(indexShard.shardId(), e)), store::decRef)
                );
                success = true;
            } catch (Exception e) {
                logException(indexShard.shardId(), e);
            } finally {
                if (success == false) {
                    store.decRef();
                }
            }
        }
    }

    private static void logException(ShardId shardId, Exception e) {
        logger.log(
            e instanceof FileNotFoundException || e instanceof NoSuchFileException ? Level.DEBUG : Level.INFO,
            () -> Strings.format("%s early indexing cache prewarming failed", shardId),
            e
        );
    }
}
