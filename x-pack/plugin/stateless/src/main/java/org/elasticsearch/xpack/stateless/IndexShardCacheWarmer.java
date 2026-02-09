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

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.ExceptionsHelper;
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
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.lucene.BlobCacheIndexInput;
import org.elasticsearch.xpack.stateless.lucene.IndexDirectory;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.io.FileNotFoundException;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.Executor;

import static org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService.Type.INDEXING_EARLY;

public class IndexShardCacheWarmer {

    private static final Logger logger = LogManager.getLogger(IndexShardCacheWarmer.class);

    private final ObjectStoreService objectStoreService;
    private final SharedBlobCacheWarmingService warmingService;
    private final ThreadPool threadPool;
    private final boolean useReplicatedRanges;
    private final Executor bccHeaderReadExecutor;

    public IndexShardCacheWarmer(
        ObjectStoreService objectStoreService,
        SharedBlobCacheWarmingService warmingService,
        ThreadPool threadPool,
        boolean useReplicatedRanges,
        Executor bccHeaderReadExecutor
    ) {
        this.objectStoreService = objectStoreService;
        this.warmingService = warmingService;
        this.threadPool = threadPool;
        this.useReplicatedRanges = useReplicatedRanges;
        this.bccHeaderReadExecutor = bccHeaderReadExecutor;
    }

    /**
     * Schedule the pre-warming of a peer recovering stateless index shard
     */
    public void preWarmIndexShardCache(IndexShard indexShard) {
        preWarmIndexShardCache(indexShard, INDEXING_EARLY);
    }

    /**
     * Schedule the pre-warming of a stateless index shard
     *
     * @param indexShard The shard to warm
     */
    public void preWarmIndexShardCache(IndexShard indexShard, SharedBlobCacheWarmingService.Type warmingType) {
        final IndexShardState currentState = indexShard.state(); // single volatile read
        if (currentState == IndexShardState.CLOSED) {
            throw new IndexShardNotRecoveringException(indexShard.shardId(), currentState);
        }
        assert warmingType != INDEXING_EARLY || currentState == IndexShardState.RECOVERING
            : "expected a recovering shard " + indexShard.shardId() + " but got " + currentState;
        assert indexShard.routingEntry().isSearchable() == false && indexShard.routingEntry().isPromotableToPrimary()
            : "only stateless ingestion shards are supported";
        assert warmingType != INDEXING_EARLY || indexShard.recoveryState().getRecoverySource().getType() == RecoverySource.Type.PEER
            : "Only peer recoveries are supported";
        threadPool.generic().execute(() -> doPreWarmIndexShardCache(indexShard, warmingType));
    }

    private void doPreWarmIndexShardCache(IndexShard indexShard, SharedBlobCacheWarmingService.Type warmingType) {
        assert indexShard.routingEntry().isPromotableToPrimary();
        final Store store = indexShard.store();
        if (store.tryIncRef()) {
            boolean success = false;
            try {
                final var blobStore = objectStoreService.getProjectBlobStore(indexShard.shardId());
                final var shardBasePath = objectStoreService.shardBasePath(indexShard.shardId());
                var indexDirectory = IndexDirectory.unwrapDirectory(store.directory());
                // Recovery hasn't even started yet, so we need to set the blob container here in a copied prewarming instance. This
                // instance will also be copied when reading the last BCC and other referenced blobs, so it is OK to use it for warming
                // purpose once the last BCC is known.
                var prewarmingDirectory = indexDirectory.getBlobStoreCacheDirectory().createNewBlobStoreCacheDirectoryForWarming();
                prewarmingDirectory.setBlobContainer(
                    primaryTerm -> blobStore.blobContainer(shardBasePath.add(String.valueOf(primaryTerm)))
                );
                // During unhollowing, we want to read referenced blobs with replicated ranges as well
                boolean readSingleBlobIfHollow = warmingType != SharedBlobCacheWarmingService.Type.UNHOLLOWING;
                ObjectStoreService.readIndexingShardState(
                    prewarmingDirectory,
                    BlobCacheIndexInput.WARMING,
                    blobStore.blobContainer(shardBasePath),
                    indexShard.getOperationPrimaryTerm(),
                    threadPool,
                    useReplicatedRanges,
                    bccHeaderReadExecutor,
                    readSingleBlobIfHollow,
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
                            if (last.hollow() == false || readSingleBlobIfHollow == false) {
                                warmingService.warmCacheForShardRecovery(warmingType, indexShard, last, prewarmingDirectory, null);
                            }
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
            ExceptionsHelper.unwrap(e, FileNotFoundException.class, NoSuchFileException.class) != null ? Level.DEBUG : Level.INFO,
            () -> Strings.format("%s early indexing cache prewarming failed", shardId),
            e
        );
    }
}
