/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.NoOpEngine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.engine.HollowIndexEngine;
import org.elasticsearch.xpack.stateless.engine.IndexEngine;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator;
import org.elasticsearch.xpack.stateless.lucene.IndexBlobStoreCacheDirectory;
import org.elasticsearch.xpack.stateless.lucene.IndexDirectory;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.recovery.metering.StatelessPrimaryRelocationMetricsCollector;
import org.elasticsearch.xpack.stateless.reshard.SplitSourceService;
import org.elasticsearch.xpack.stateless.reshard.SplitTargetService;
import org.elasticsearch.xpack.stateless.snapshots.SnapshotsCommitService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static org.elasticsearch.index.shard.StoreRecovery.bootstrap;
import static org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService.Type.INDEXING;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit.HOLLOW_TRANSLOG_RECOVERY_START_FILE;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit.TRANSLOG_RECOVERY_START_FILE;
import static org.elasticsearch.xpack.stateless.engine.IndexEngine.TRANSLOG_RELEASE_END_FILE;

/**
 * {@link IndexEventListener} that drives shard recovery from the object store on stateless index nodes.
 */
public class StatelessIndexNodeRecoveryListener extends AbstractStatelessRecoveryListener implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(StatelessIndexNodeRecoveryListener.class);

    private final ThreadPool threadPool;
    private final StatelessCommitService statelessCommitService;
    private final TranslogReplicator translogReplicator;
    private final SharedBlobCacheWarmingService warmingService;
    private final StatelessSharedBlobCacheService cacheService;
    private final HollowShardsService hollowShardsService;
    private final SplitTargetService splitTargetService;
    private final SplitSourceService splitSourceService;
    private final Executor bccHeaderReadExecutor;
    private final SnapshotsCommitService snapshotsCommitService;
    private final StatelessPrimaryRelocationMetricsCollector relocationMetricsCollector;

    public StatelessIndexNodeRecoveryListener(
        ThreadPool threadPool,
        StatelessCommitService statelessCommitService,
        ObjectStoreService objectStoreService,
        TranslogReplicator translogReplicator,
        SharedBlobCacheWarmingService warmingService,
        HollowShardsService hollowShardsService,
        SplitTargetService splitTargetService,
        SplitSourceService splitSourceService,
        ProjectResolver projectResolver,
        Executor bccHeaderReadExecutor,
        StatelessSharedBlobCacheService cacheService,
        SnapshotsCommitService snapshotsCommitService,
        StatelessPrimaryRelocationMetricsCollector relocationMetricsCollector
    ) {
        super(objectStoreService, projectResolver);
        this.threadPool = threadPool;
        this.statelessCommitService = statelessCommitService;
        this.translogReplicator = translogReplicator;
        this.warmingService = warmingService;
        this.hollowShardsService = hollowShardsService;
        this.splitTargetService = splitTargetService;
        this.splitSourceService = splitSourceService;
        this.bccHeaderReadExecutor = bccHeaderReadExecutor;
        this.cacheService = cacheService;
        this.snapshotsCommitService = snapshotsCommitService;
        this.relocationMetricsCollector = relocationMetricsCollector;
    }

    @Override
    public void afterFilesRestoredFromRepository(IndexShard indexShard) {
        final var store = indexShard.store();
        store.incRef();
        try {
            final var userData = store.readLastCommittedSegmentsInfo().getUserData();
            final String startFile = userData.get(TRANSLOG_RECOVERY_START_FILE);
            if (startFile == null) {
                return;
            }
            final var startFileValue = Long.parseLong(startFile);
            final var currentNodeStartFileForNextCommit = translogReplicator.getMaxUploadedFile() + 1;
            if (startFileValue == HOLLOW_TRANSLOG_RECOVERY_START_FILE) {
                logger.debug("restoring {} from a hollow commit, updating hollow commit markers", indexShard.shardId());
                final var updatedUserData = new HashMap<>(userData);
                updatedUserData.put(TRANSLOG_RECOVERY_START_FILE, Long.toString(currentNodeStartFileForNextCommit));
                final var removed = updatedUserData.remove(TRANSLOG_RELEASE_END_FILE);
                assert removed != null : "TRANSLOG_RELEASE_END_FILE should be present in userData for hollow commit";
                store.associateIndexWithNewUserData(updatedUserData);
                return;
            }
            if (startFileValue != currentNodeStartFileForNextCommit) {
                store.associateIndexWithNewUserKeyValueData(TRANSLOG_RECOVERY_START_FILE, Long.toString(currentNodeStartFileForNextCommit));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            store.decRef();
        }
    }

    @Override
    public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
        final Store store = indexShard.store();
        try {
            store.incRef();
            boolean success = false;
            try {
                final var existingBlobContainer = initializeBlobContainer(indexShard, store);
                assert indexShard.routingEntry().isSearchable() == false;
                var releaseAfterListener = ActionListener.releaseAfter(listener, store::decRef);
                if (IndexReshardingMetadata.isSplitTarget(indexShard.shardId(), indexSettings.getIndexMetadata().getReshardingMetadata())) {
                    beforeRecoveryOnSplitTarget(indexShard, existingBlobContainer, releaseAfterListener, indexSettings);
                } else {
                    beforeRecoveryOnIndexingShard(indexShard, existingBlobContainer, releaseAfterListener);
                }
                success = true;
            } finally {
                if (success == false) {
                    store.decRef();
                }
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void beforeRecoveryOnSplitTarget(
        final IndexShard indexShard,
        final BlobContainer existingBlobContainer,
        final ActionListener<Void> releaseAfterListener,
        final IndexSettings indexSettings
    ) {
        splitTargetService.startSplitTargetShardRecovery(
            indexShard,
            indexSettings.getIndexMetadata(),
            new ThreadedActionListener<>(
                threadPool.generic(),
                releaseAfterListener.delegateFailureAndWrap(
                    (listener1, unused) -> beforeRecoveryOnIndexingShard(indexShard, existingBlobContainer, releaseAfterListener)
                )
            )
        );
    }

    private void beforeRecoveryOnIndexingShard(IndexShard indexShard, BlobContainer shardContainer, ActionListener<Void> listener) {
        assert indexShard.store().refCount() > 0 : indexShard.shardId();
        assert indexShard.routingEntry().isPromotableToPrimary();
        final var recoveryInfoFromSource = statelessCommitService.getRecoveryInfoFromSourceEntry(indexShard.shardId());
        final var sourceBlobsInfo = recoveryInfoFromSource == null ? null : recoveryInfoFromSource.sourceBlobsInfo();
        final var lastCommitBlobs = recoveryInfoFromSource == null ? null : recoveryInfoFromSource.lastCommitBlobs();
        final var lastCommitIsHollow = recoveryInfoFromSource != null && recoveryInfoFromSource.lastCommitIsHollow();
        final var hasRecentIdLookup = recoveryInfoFromSource != null && recoveryInfoFromSource.hasRecentIdLookup();
        final long readIndexingShardStateStartMillis = threadPool.relativeTimeInMillis();
        SubscribableListener.<ObjectStoreService.IndexingShardState>newForked(l -> {
            if (shardContainer == null) {
                ActionListener.completeWith(l, () -> ObjectStoreService.IndexingShardState.EMPTY);
                return;
            }

            final var directory = IndexBlobStoreCacheDirectory.unwrapDirectory(indexShard.store().directory());
            if (lastCommitBlobs != null && lastCommitIsHollow == false) {
                warmingService.warmCacheForBCCHeadersRead(
                    indexShard,
                    directory,
                    lastCommitBlobs,
                    ActionListener.wrap(
                        v -> {},
                        e -> logger.warn("[{}] failed to pre-warm region 0 before BCC header reads", indexShard.shardId(), e)
                    )
                );
            }

            ObjectStoreService.readIndexingShardState(
                directory,
                IOContext.DEFAULT,
                shardContainer,
                indexShard.getOperationPrimaryTerm(),
                threadPool,
                statelessCommitService.useReplicatedRanges(),
                bccHeaderReadExecutor,
                true,
                sourceBlobsInfo,
                l
            );
        }).<Void>andThen((l, state) -> {
            relocationMetricsCollector.recordRelocationTargetReadIndexingShardStateDuration(
                threadPool.relativeTimeInMillis() - readIndexingShardStateStartMillis
            );
            recoverBatchedCompoundCommitOnIndexShard(indexShard, state, hasRecentIdLookup, l);
        }).addListener(listener);
    }

    @Override
    public void afterIndexShardRecovery(IndexShard indexShard, ActionListener<Void> listener) {
        ActionListener.run(listener, l -> indexShard.withEngine(engine -> {
            switch (engine) {
                case IndexEngine indexEngine -> {
                    long currentGeneration = indexEngine.getCurrentGeneration();
                    if (currentGeneration > statelessCommitService.getRecoveredGeneration(indexShard.shardId())) {
                        ShardId shardId = indexShard.shardId();
                        statelessCommitService.addListenerForUploadedGeneration(shardId, indexEngine.getCurrentGeneration(), l);
                    } else {
                        indexEngine.flush(true, true, l.map(f -> null));
                    }
                }
                case HollowIndexEngine ignored -> {
                    hollowShardsService.addHollowShard(indexShard, "recovery");
                    // Evict the recovery BCC blob, since it won't be read again, in order to create space for new cache entries.
                    // Note: we run prewarming asynchronously. It's unlikely but possible to have it race with eviction, and that may mean
                    // the cache entry ultimately stays in the cache. But because it should be rare, we do not optimize further for this.
                    cacheService.forceEvict(indexShard.shardId(), k -> true);
                    l.onResponse(null);
                }
                case NoOpEngine ignored -> l.onResponse(null);
                default -> throw new AssertionError("unexpected engine type: " + engine);
            }
            return null;
        }));
    }

    @Override
    public void afterIndexShardStarted(IndexShard indexShard) {
        assert indexShard.routingEntry().isPromotableToPrimary();
        IndexSettings indexSettings = indexShard.indexSettings();
        IndexReshardingMetadata reshardingMetadata = indexSettings.getIndexMetadata().getReshardingMetadata();
        if (IndexReshardingMetadata.isSplitSource(indexShard.shardId(), reshardingMetadata)) {
            splitSourceService.splitSourceShardStarted(indexShard, reshardingMetadata);
        }
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings indexSettings) {
        // Can be null if there was a problem creating the shard.
        if (indexShard != null) {
            splitTargetService.cancelSplits(indexShard);
        }
    }

    @Override
    public void afterIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null) {
            splitSourceService.cancelSplits(indexShard);
        }
    }

    private void recoverBatchedCompoundCommitOnIndexShard(
        IndexShard indexShard,
        ObjectStoreService.IndexingShardState indexingShardState,
        boolean hasRecentIdLookup,
        ActionListener<Void> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);

            var store = indexShard.store();
            var indexDirectory = IndexDirectory.unwrapDirectory(store.directory());
            var batchedCompoundCommit = indexingShardState.latestCommit();
            logBootstrappingFromObjectStore(logger, indexShard, batchedCompoundCommit);

            if (batchedCompoundCommit != null) {
                var recoveryCommit = batchedCompoundCommit.lastCompoundCommit();
                var blobFileRanges = indexingShardState.blobFileRanges();
                assert blobFileRanges.keySet().containsAll(recoveryCommit.commitFiles().keySet())
                    || statelessCommitService.useReplicatedRanges() == false;

                indexDirectory.updateRecoveryCommit(
                    recoveryCommit.generation(),
                    recoveryCommit.nodeEphemeralId(),
                    recoveryCommit.translogRecoveryStartFile(),
                    recoveryCommit.getAllFilesSizeInBytes(),
                    blobFileRanges
                );
                if (recoveryCommit.hollow() == false) {
                    // We must use a copied instance for warming as the index directory will move forward with new commits
                    var warmingDirectory = indexDirectory.getBlobStoreCacheDirectory().createNewBlobStoreCacheDirectoryForWarming();
                    warmingDirectory.updateMetadata(blobFileRanges, recoveryCommit.getAllFilesSizeInBytes());

                    warmingService.warmCacheForShardRecoveryOrUnhollowing(
                        INDEXING,
                        indexShard,
                        recoveryCommit,
                        warmingDirectory,
                        hasRecentIdLookup,
                        ActionListener.noop()
                    );
                }
            }
            final var segmentInfos = SegmentInfos.readLatestCommit(indexDirectory);
            final var translogUUID = segmentInfos.userData.get(Translog.TRANSLOG_UUID_KEY);
            final var checkPoint = segmentInfos.userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY);
            if (translogUUID != null) {
                Translog.createEmptyTranslog(
                    indexShard.shardPath().resolveTranslog(),
                    indexShard.shardId(),
                    checkPoint == null ? SequenceNumbers.UNASSIGNED_SEQ_NO : Long.parseLong(checkPoint),
                    indexShard.getPendingPrimaryTerm(),
                    translogUUID,
                    null
                );
            } else {
                bootstrap(indexShard, store);
            }

            if (batchedCompoundCommit != null) {
                assert batchedCompoundCommit.shardId().equals(indexShard.shardId())
                    || indexShard.routingEntry()
                        .recoverySource() instanceof RecoverySource.ReshardSplitRecoverySource reshardSplitRecoverySource
                        && reshardSplitRecoverySource.getSourceShardId().equals(batchedCompoundCommit.shardId())
                    : batchedCompoundCommit.shardId() + " vs " + indexShard.shardId();

                statelessCommitService.markRecoveredBcc(
                    indexShard.shardId(),
                    batchedCompoundCommit,
                    indexingShardState.otherBlobs(),
                    snapshotsCommitService.getExtraReferenceConsumers(indexShard.shardId())
                );
            }
            statelessCommitService.addConsumerForNewUploadedBcc(indexShard.shardId(), info -> {
                Set<String> uploadedFiles = info.uploadedBcc()
                    .compoundCommits()
                    .stream()
                    .flatMap(f -> f.commitFiles().keySet().stream())
                    .collect(Collectors.toSet());
                indexDirectory.updateCommit(
                    info.uploadedBcc().lastCompoundCommit().generation(),
                    info.uploadedBcc().lastCompoundCommit().getAllFilesSizeInBytes(),
                    uploadedFiles,
                    info.blobFileRanges()
                );
            });

            statelessCommitService.addConsumerForNewUploadedBcc(
                indexShard.shardId(),
                info -> translogReplicator.markShardCommitUploaded(indexShard.shardId(), info.translogReleaseEndFile())
            );
            return null;
        });
    }
}
