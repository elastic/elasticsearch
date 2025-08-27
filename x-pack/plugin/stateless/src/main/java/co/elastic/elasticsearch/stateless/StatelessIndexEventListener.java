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
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService;
import co.elastic.elasticsearch.stateless.commits.BatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.HollowShardsService;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.HollowIndexEngine;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.engine.SearchEngine;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;
import co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectory;
import co.elastic.elasticsearch.stateless.lucene.IndexBlobStoreCacheDirectory;
import co.elastic.elasticsearch.stateless.lucene.IndexDirectory;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.recovery.RecoveryCommitRegistrationHandler;
import co.elastic.elasticsearch.stateless.reshard.SplitSourceService;
import co.elastic.elasticsearch.stateless.reshard.SplitTargetService;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.NoOpEngine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService.Type.INDEXING;
import static co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService.Type.SEARCH;
import static org.elasticsearch.index.shard.StoreRecovery.bootstrap;

class StatelessIndexEventListener implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(StatelessIndexEventListener.class);

    private final ThreadPool threadPool;
    private final StatelessCommitService statelessCommitService;
    private final ObjectStoreService objectStoreService;
    private final TranslogReplicator translogReplicator;
    private final RecoveryCommitRegistrationHandler recoveryCommitRegistrationHandler;
    private final SharedBlobCacheWarmingService warmingService;
    private final HollowShardsService hollowShardsService;
    private final SplitTargetService splitTargetService;
    private final SplitSourceService splitSourceService;
    private final ProjectResolver projectResolver;
    private final Executor bccHeaderReadExecutor;

    StatelessIndexEventListener(
        ThreadPool threadPool,
        StatelessCommitService statelessCommitService,
        ObjectStoreService objectStoreService,
        TranslogReplicator translogReplicator,
        RecoveryCommitRegistrationHandler recoveryCommitRegistrationHandler,
        SharedBlobCacheWarmingService warmingService,
        HollowShardsService hollowShardsService,
        SplitTargetService splitTargetService,
        SplitSourceService splitSourceService,
        ProjectResolver projectResolver,
        Executor bccHeaderReadExecutor
    ) {
        this.threadPool = threadPool;
        this.statelessCommitService = statelessCommitService;
        this.objectStoreService = objectStoreService;
        this.translogReplicator = translogReplicator;
        this.recoveryCommitRegistrationHandler = recoveryCommitRegistrationHandler;
        this.warmingService = warmingService;
        this.hollowShardsService = hollowShardsService;
        this.splitTargetService = splitTargetService;
        this.splitSourceService = splitSourceService;
        this.projectResolver = projectResolver;
        this.bccHeaderReadExecutor = bccHeaderReadExecutor;
    }

    @Override
    public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
        final Store store = indexShard.store();
        try {
            store.incRef();
            boolean success = false;
            try {
                final var projectId = projectResolver.getProjectId();
                final var shardId = indexShard.shardId();
                assert objectStoreService.assertProjectIdAndShardIdConsistency(projectId, shardId);

                final BlobStore blobStore = objectStoreService.getProjectBlobStore(projectId);
                final BlobPath shardBasePath = objectStoreService.shardBasePath(projectId, shardId);
                final BlobContainer existingBlobContainer = hasNoExistingBlobContainer(indexShard.recoveryState().getRecoverySource())
                    ? null
                    : blobStore.blobContainer(shardBasePath);

                BlobStoreCacheDirectory.unwrapDirectory(store.directory())
                    .setBlobContainer(primaryTerm -> blobStore.blobContainer(shardBasePath.add(String.valueOf(primaryTerm))));

                var releaseAfterListener = ActionListener.releaseAfter(listener, store::decRef);
                if (indexShard.routingEntry().isSearchable()) {
                    beforeRecoveryOnSearchShard(indexShard, existingBlobContainer, releaseAfterListener);
                } else {
                    if (IndexReshardingMetadata.isSplitTarget(shardId, indexSettings.getIndexMetadata().getReshardingMetadata())) {
                        splitTargetService.startSplitRecovery(
                            indexShard,
                            indexSettings.getIndexMetadata(),
                            new ThreadedActionListener<>(
                                threadPool.generic(),
                                releaseAfterListener.delegateFailureAndWrap(
                                    (listener1, unused) -> beforeRecoveryOnIndexingShard(
                                        indexShard,
                                        existingBlobContainer,
                                        releaseAfterListener
                                    )
                                )
                            )
                        );
                    } else {
                        beforeRecoveryOnIndexingShard(indexShard, existingBlobContainer, releaseAfterListener);
                    }
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

    private static boolean hasNoExistingBlobContainer(RecoverySource recoverySource) {
        return recoverySource == RecoverySource.EmptyStoreRecoverySource.INSTANCE
            || recoverySource instanceof RecoverySource.SnapshotRecoverySource;
    }

    private static void logBootstrapping(IndexShard indexShard, BatchedCompoundCommit latestCommit) {
        logger.info(
            "{} with UUID [{}] bootstrapping [{}] shard on primary term [{}] with {} from object store ({})",
            indexShard.shardId(),
            indexShard.shardId().getIndex().getUUID(),
            indexShard.routingEntry().role(),
            indexShard.getOperationPrimaryTerm(),
            latestCommit != null ? latestCommit.lastCompoundCommit().toShortDescription() : "empty commit",
            describe(indexShard.recoveryState())
        );
    }

    private static void logBootstrapping(
        IndexShard indexShard,
        StatelessCompoundCommit latestCommit,
        PrimaryTermAndGeneration latestUploaded
    ) {
        assert indexShard.routingEntry().isPromotableToPrimary() == false;
        assert latestCommit != null;
        logger.info(
            "{} with UUID [{}] bootstrapping [{}] shard on primary term [{}] with {} and latest uploaded {} from indexing shard ({})",
            indexShard.shardId(),
            indexShard.shardId().getIndex().getUUID(),
            indexShard.routingEntry().role(),
            indexShard.getOperationPrimaryTerm(),
            latestCommit.toShortDescription(),
            latestUploaded,
            describe(indexShard.recoveryState())
        );
    }

    private static String describe(RecoveryState recoveryState) {
        return recoveryState.getRecoverySource() == RecoverySource.PeerRecoverySource.INSTANCE
            ? recoveryState.getRecoverySource() + " from " + recoveryState.getSourceNode().getName()
            : recoveryState.getRecoverySource().toString();
    }

    private void beforeRecoveryOnIndexingShard(IndexShard indexShard, BlobContainer shardContainer, ActionListener<Void> listener) {
        assert indexShard.store().refCount() > 0 : indexShard.shardId();
        assert indexShard.routingEntry().isPromotableToPrimary();
        SubscribableListener.<ObjectStoreService.IndexingShardState>newForked(l -> {
            if (shardContainer == null) {
                ActionListener.completeWith(l, () -> ObjectStoreService.IndexingShardState.EMPTY);
                return;
            }
            ObjectStoreService.readIndexingShardState(
                IndexBlobStoreCacheDirectory.unwrapDirectory(indexShard.store().directory()),
                IOContext.DEFAULT,
                shardContainer,
                indexShard.getOperationPrimaryTerm(),
                threadPool,
                statelessCommitService.useReplicatedRanges(),
                bccHeaderReadExecutor,
                l
            );
        }).<Void>andThen((l, state) -> recoverBatchedCompoundCommitOnIndexShard(indexShard, state, l)).addListener(listener);
    }

    private void recoverBatchedCompoundCommitOnIndexShard(
        IndexShard indexShard,
        ObjectStoreService.IndexingShardState indexingShardState,
        ActionListener<Void> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);

            var store = indexShard.store();
            var indexDirectory = IndexDirectory.unwrapDirectory(store.directory());
            var batchedCompoundCommit = indexingShardState.latestCommit();
            logBootstrapping(indexShard, batchedCompoundCommit);

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
                // We must use a copied instance for warming as the index directory will move forward with new commits
                var warmingDirectory = indexDirectory.createNewInstance();
                warmingDirectory.updateMetadata(blobFileRanges, recoveryCommit.getAllFilesSizeInBytes());
                warmingService.warmCacheForShardRecovery(INDEXING, indexShard, recoveryCommit, warmingDirectory);
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

                statelessCommitService.markRecoveredBcc(indexShard.shardId(), batchedCompoundCommit, indexingShardState.otherBlobs());
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

    private void beforeRecoveryOnSearchShard(IndexShard indexShard, BlobContainer blobContainer, ActionListener<Void> listener)
        throws IOException {
        assert indexShard.store().refCount() > 0 : indexShard.shardId();
        assert blobContainer != null : indexShard.routingEntry();

        final var batchedCompoundCommit = ObjectStoreService.readSearchShardState(blobContainer, indexShard.getOperationPrimaryTerm());
        assert batchedCompoundCommit == null || batchedCompoundCommit.shardId().equals(indexShard.shardId())
            : batchedCompoundCommit.shardId() + " != " + indexShard.shardId();

        recoveryCommitRegistrationHandler.register(
            batchedCompoundCommit != null ? batchedCompoundCommit.primaryTermAndGeneration() : PrimaryTermAndGeneration.ZERO,
            batchedCompoundCommit != null
                ? batchedCompoundCommit.lastCompoundCommit().primaryTermAndGeneration()
                : PrimaryTermAndGeneration.ZERO,
            indexShard.shardId(),
            listener.delegateFailure((l, response) -> ActionListener.completeWith(listener, () -> {
                var searchDirectory = SearchDirectory.unwrapDirectory(indexShard.store().directory());
                var lastUploaded = response.getLatestUploadedBatchedCompoundCommitTermAndGen();
                var nodeId = response.getNodeId();
                assert nodeId != null : response;

                var compoundCommit = response.getCompoundCommit();
                if (compoundCommit == null) {
                    // If the indexing shard provided no compound commit to recover from, then the last uploaded BCC term/gen returned
                    // should be equal to zero indicated the indexing shard's engine is null or is a NoOpEngine
                    assert PrimaryTermAndGeneration.ZERO.equals(lastUploaded) : lastUploaded;

                    logBootstrapping(indexShard, batchedCompoundCommit);
                    // If there is no batched compound commit found in the object store, then recover from an empty commit
                    if (batchedCompoundCommit == null) {
                        return null;
                    }

                    // Otherwise recover from the compound commit found in the object store
                    // TODO Should we revisit this? the indexing shard does not know about the commits used by this search shard
                    // until the next new commit notification.
                    compoundCommit = batchedCompoundCommit.lastCompoundCommit();
                    lastUploaded = batchedCompoundCommit.primaryTermAndGeneration();

                } else {
                    logBootstrapping(indexShard, compoundCommit, lastUploaded);
                }

                assert batchedCompoundCommit == null
                    || batchedCompoundCommit.lastCompoundCommit()
                        .primaryTermAndGeneration()
                        .onOrBefore(compoundCommit.primaryTermAndGeneration());
                assert compoundCommit != null;

                searchDirectory.updateLatestUploadedBcc(lastUploaded);
                searchDirectory.updateLatestCommitInfo(compoundCommit.primaryTermAndGeneration(), nodeId);
                searchDirectory.updateCommit(compoundCommit);
                // warming up the latest commit upon recovery will fetch a few regions of every active
                // segment (the first region of every segment is always fetched)
                warmingService.warmCacheForShardRecovery(SEARCH, indexShard, compoundCommit, searchDirectory);
                assert indexShard.store().refCount() > 0 : indexShard.shardId();
                return null;
            }))
        );
    }

    @Override
    public void afterIndexShardRecovery(IndexShard indexShard, ActionListener<Void> listener) {
        ActionListener.run(listener, l -> {
            if (indexShard.routingEntry().isPromotableToPrimary()) {
                Engine engineOrNull = indexShard.getEngineOrNull();

                IndexSettings indexSettings = indexShard.indexSettings();
                IndexReshardingMetadata reshardingMetadata = indexSettings.getIndexMetadata().getReshardingMetadata();
                // TODO with this implementation we will sometimes run below calls on stateless_upload_prewarm thread pool
                // due to how statelessCommitService.addListenerForUploadedGeneration works.
                if (IndexReshardingMetadata.isSplitTarget(indexShard.shardId(), reshardingMetadata)) {
                    // Should already have advanced past CLONE
                    assert reshardingMetadata.getSplit()
                        .targetStateAtLeast(indexShard.shardId().id(), IndexReshardingState.Split.TargetShardState.HANDOFF);
                    l = l.delegateFailure(
                        (toWrap, unused) -> splitTargetService.afterSplitTargetIndexShardRecovery(indexShard, reshardingMetadata, toWrap)
                    );
                } else if (IndexReshardingMetadata.isSplitSource(indexShard.shardId(), reshardingMetadata)) {
                    l = l.delegateFailure(
                        (toWrap, unused) -> splitSourceService.afterSplitSourceIndexShardRecovery(indexShard, reshardingMetadata, toWrap)
                    );
                }
                if (engineOrNull instanceof IndexEngine engine) {
                    long currentGeneration = engine.getCurrentGeneration();
                    if (currentGeneration > statelessCommitService.getRecoveredGeneration(indexShard.shardId())) {
                        ShardId shardId = indexShard.shardId();
                        statelessCommitService.addListenerForUploadedGeneration(shardId, engine.getCurrentGeneration(), l);
                    } else {
                        engine.flush(true, true, l.map(f -> null));
                    }
                } else if (engineOrNull instanceof HollowIndexEngine) {
                    hollowShardsService.addHollowShard(indexShard, "recovery");
                    l.onResponse(null);
                } else if (engineOrNull == null) {
                    throw new AlreadyClosedException("engine is closed");
                } else {
                    assert engineOrNull instanceof NoOpEngine;
                    l.onResponse(null);
                }
            } else {
                final Engine engineOrNull = indexShard.getEngineOrNull();
                if (engineOrNull instanceof SearchEngine searchEngine) {
                    assert indexShard.state() == IndexShardState.POST_RECOVERY
                        : "expected post recovery index shard state but is: " + indexShard.state();
                    assert indexShard.routingEntry().state() == ShardRoutingState.INITIALIZING
                        : "expected initializing shard routing state but is: " + indexShard.state();
                    indexShard.updateGlobalCheckpointOnReplica(searchEngine.getLastSyncedGlobalCheckpoint(), "search shard recovery");
                    searchEngine.afterRecovery();
                    l.onResponse(null);
                } else if (engineOrNull == null) {
                    throw new AlreadyClosedException("engine is closed");
                } else {
                    assert engineOrNull instanceof NoOpEngine;
                    l.onResponse(null);
                }
            }
        });
    }

    @Override
    public void beforeIndexShardMutableOperation(IndexShard indexShard, boolean permitAcquired, ActionListener<Void> listener) {
        hollowShardsService.onMutableOperation(indexShard, permitAcquired, listener);
    }
}
