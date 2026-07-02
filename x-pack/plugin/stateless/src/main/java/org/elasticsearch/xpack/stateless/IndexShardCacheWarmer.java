/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.core.Nullable;
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
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService.Type;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.lucene.BlobCacheIndexInput;
import org.elasticsearch.xpack.stateless.lucene.IndexBlobStoreCacheDirectory;
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

    public void preWarmIndexShardCacheForPeerRecovery(
        IndexShard indexShard,
        StatelessCommitService.SourceBlobsInfo sourceBlobsInfo,
        boolean preWarmForIdLookup
    ) {
        preWarmIndexShardCache(indexShard, INDEXING_EARLY, sourceBlobsInfo, preWarmForIdLookup);
    }

    public void preWarmIndexShardCache(IndexShard indexShard, SharedBlobCacheWarmingService.Type warmingType) {
        preWarmIndexShardCache(indexShard, warmingType, null, false);
    }

    public void preWarmIndexShardCache(
        IndexShard indexShard,
        SharedBlobCacheWarmingService.Type warmingType,
        @Nullable StatelessCommitService.SourceBlobsInfo sourceBlobsInfo,
        boolean preWarmForIdLookup
    ) {
        final IndexShardState currentState = indexShard.state(); // single volatile read
        if (currentState == IndexShardState.CLOSED) {
            throw new IndexShardNotRecoveringException(indexShard.shardId(), currentState);
        }
        if (warmingType == INDEXING_EARLY && currentState != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(indexShard.shardId(), currentState);
        }
        assert warmingType != INDEXING_EARLY || currentState == IndexShardState.RECOVERING
            : "expected a recovering shard " + indexShard.shardId() + " but got " + currentState;
        assert indexShard.routingEntry().isSearchable() == false && indexShard.routingEntry().isPromotableToPrimary()
            : "only stateless ingestion shards are supported";
        assert warmingType != INDEXING_EARLY || indexShard.recoveryState().getRecoverySource().getType() == RecoverySource.Type.PEER
            : "Only peer recoveries are supported";
        assert sourceBlobsInfo == null || warmingType == INDEXING_EARLY;
        threadPool.generic().execute(() -> doPreWarmIndexShardCache(indexShard, warmingType, sourceBlobsInfo, preWarmForIdLookup));
    }

    private void doPreWarmIndexShardCache(
        IndexShard indexShard,
        SharedBlobCacheWarmingService.Type warmingType,
        @Nullable StatelessCommitService.SourceBlobsInfo sourceBlobsInfo,
        boolean preWarmForIdLookup
    ) {
        assert indexShard.routingEntry().isPromotableToPrimary();
        final Store store = indexShard.store();
        if (store.tryIncRef()) {
            boolean success = false;
            try {
                final var blobStore = objectStoreService.getProjectBlobStore(indexShard.shardId());
                final var shardBasePath = objectStoreService.shardBasePath(indexShard.shardId());
                // Recovery hasn't even started yet, so we need to set the blob container here in a copied prewarming instance. This
                // instance will also be copied when reading the last BCC and other referenced blobs, so it is OK to use it for warming
                // purpose once the last BCC is known.
                var prewarmingDirectory = createNewBlobStoreCacheDirectoryForWarming(store, blobStore, shardBasePath);
                assert (warmingType != Type.UNHOLLOWING);
                ObjectStoreService.readIndexingShardState(
                    prewarmingDirectory,
                    BlobCacheIndexInput.WARMING,
                    blobStore.blobContainer(shardBasePath),
                    indexShard.getOperationPrimaryTerm(),
                    threadPool,
                    useReplicatedRanges,
                    bccHeaderReadExecutor,
                    true,
                    sourceBlobsInfo,
                    ActionListener.releaseAfter(ActionListener.wrap(state -> {
                        updateMetadataAndWarmCache(indexShard, warmingType, state, prewarmingDirectory, true, preWarmForIdLookup);
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

    public void preWarmIndexShardCacheForUnhollowing(Store store, IndexShard indexShard, ObjectStoreService.IndexingShardState state) {
        try {
            final var blobStore = objectStoreService.getProjectBlobStore(indexShard.shardId());
            final var shardBasePath = objectStoreService.shardBasePath(indexShard.shardId());
            var prewarmingDirectory = createNewBlobStoreCacheDirectoryForWarming(store, blobStore, shardBasePath);
            updateMetadataAndWarmCache(indexShard, Type.UNHOLLOWING, state, prewarmingDirectory, false, false);
        } catch (Exception e) {
            logException(indexShard.shardId(), e);
        }
    }

    private static IndexBlobStoreCacheDirectory createNewBlobStoreCacheDirectoryForWarming(
        Store store,
        BlobStore blobStore,
        BlobPath shardBasePath
    ) {
        var indexDirectory = IndexDirectory.unwrapDirectory(store.directory());
        var prewarmingDirectory = indexDirectory.getBlobStoreCacheDirectory().createNewBlobStoreCacheDirectoryForWarming();
        prewarmingDirectory.setBlobContainer(primaryTerm -> blobStore.blobContainer(shardBasePath.add(String.valueOf(primaryTerm))));
        return prewarmingDirectory;
    }

    private void updateMetadataAndWarmCache(
        IndexShard indexShard,
        SharedBlobCacheWarmingService.Type warmingType,
        ObjectStoreService.IndexingShardState state,
        IndexBlobStoreCacheDirectory prewarmingDirectory,
        boolean readSingleBlobIfHollow,
        boolean preWarmForIdLookup
    ) {
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
                warmingService.warmCacheForShardRecoveryOrUnhollowing(
                    warmingType,
                    indexShard,
                    last,
                    prewarmingDirectory,
                    null,
                    preWarmForIdLookup,
                    ActionListener.noop()
                );

            }
        }
    }

    private static void logException(ShardId shardId, Exception e) {
        // FileNotFoundException and NoSuchFileException are expected for the early indexing as files might be replaced
        // during relocation flushes.
        logger.log(
            ExceptionsHelper.unwrap(e, FileNotFoundException.class, NoSuchFileException.class) != null ? Level.DEBUG : Level.INFO,
            () -> Strings.format("%s early indexing cache prewarming failed", shardId),
            e
        );
    }
}
