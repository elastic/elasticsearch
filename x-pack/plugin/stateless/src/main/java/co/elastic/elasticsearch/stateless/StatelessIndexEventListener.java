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
import co.elastic.elasticsearch.stateless.commits.BlobFile;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;
import co.elastic.elasticsearch.stateless.lucene.IndexDirectory;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.recovery.RecoveryCommitRegistrationHandler;

import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.NoOpEngine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.index.shard.StoreRecovery.bootstrap;

class StatelessIndexEventListener implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(StatelessIndexEventListener.class);

    private final StatelessCommitService statelessCommitService;
    private final Supplier<ObjectStoreService> objectStoreService;
    private final Supplier<TranslogReplicator> translogReplicator;
    private final Supplier<RecoveryCommitRegistrationHandler> recoveryCommitRegistrationHandler;
    private final Supplier<SharedBlobCacheWarmingService> sharedBlobCacheWarmingService;

    StatelessIndexEventListener(
        StatelessCommitService statelessCommitService,
        Supplier<ObjectStoreService> objectStoreService,
        Supplier<TranslogReplicator> translogReplicator,
        Supplier<RecoveryCommitRegistrationHandler> recoveryCommitRegistrationHandler,
        Supplier<SharedBlobCacheWarmingService> sharedBlobCacheWarmingService
    ) {
        this.statelessCommitService = statelessCommitService;
        this.objectStoreService = objectStoreService;
        this.translogReplicator = translogReplicator;
        this.recoveryCommitRegistrationHandler = recoveryCommitRegistrationHandler;
        this.sharedBlobCacheWarmingService = sharedBlobCacheWarmingService;
    }

    @Override
    public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
        final Store store = indexShard.store();
        try {
            store.incRef();
            final var service = objectStoreService.get();
            final var blobStore = service.blobStore();
            final ShardId shardId = indexShard.shardId();
            final var shardBasePath = service.shardBasePath(shardId);
            SearchDirectory.unwrapDirectory(store.directory())
                .setBlobContainer(primaryTerm -> blobStore.blobContainer(shardBasePath.add(String.valueOf(primaryTerm))));
            final BlobContainer existingBlobContainer;
            if (indexShard.recoveryState().getRecoverySource() == RecoverySource.EmptyStoreRecoverySource.INSTANCE) {
                logger.info(
                    "{} bootstrapping [{}] shard on primary term [{}] with empty commit ({})",
                    shardId,
                    indexShard.routingEntry().role(),
                    indexShard.getOperationPrimaryTerm(),
                    RecoverySource.EmptyStoreRecoverySource.INSTANCE
                );
                existingBlobContainer = null;
            } else if (indexShard.recoveryState().getRecoverySource() instanceof RecoverySource.SnapshotRecoverySource) {
                logger.info(
                    "{} bootstrapping [{}] shard restore on primary term [{}] with snapshot recovery source ({})",
                    shardId,
                    indexShard.routingEntry().role(),
                    indexShard.getOperationPrimaryTerm(),
                    indexShard.recoveryState().getRecoverySource()
                );
                existingBlobContainer = null;

            } else {
                existingBlobContainer = blobStore.blobContainer(shardBasePath);
            }
            if (indexShard.routingEntry().isSearchable()) {
                beforeRecoveryOnSearchShard(indexShard, existingBlobContainer, listener);
            } else {
                beforeRecoveryIndexShard(indexShard, existingBlobContainer, listener);
            }
        } catch (IOException e) {
            listener.onFailure(e);
        } finally {
            store.decRef();
        }
    }

    private static void logBootstrapping(IndexShard indexShard, StatelessCompoundCommit latestCommit) {
        if (latestCommit != null) {
            logger.info(
                "{} bootstrapping [{}] shard on primary term [{}] with {} from object store ({})",
                indexShard.shardId(),
                indexShard.routingEntry().role(),
                indexShard.getOperationPrimaryTerm(),
                latestCommit.toShortDescription(),
                indexShard.recoveryState().getRecoverySource()
            );
        } else {
            logger.info(
                "{} bootstrapping [{}] shard on primary term [{}] with empty commit from object store ({})",
                indexShard.shardId(),
                indexShard.routingEntry().role(),
                indexShard.getOperationPrimaryTerm(),
                indexShard.recoveryState().getRecoverySource()
            );
        }
    }

    private void beforeRecoveryIndexShard(IndexShard indexShard, BlobContainer existingBlobContainer, ActionListener<Void> listener)
        throws IOException {
        final Set<BlobFile> unreferencedFiles;
        final StatelessCompoundCommit commit;
        if (existingBlobContainer != null) {
            var state = ObjectStoreService.readIndexingShardState(existingBlobContainer, indexShard.getOperationPrimaryTerm());
            commit = state.v1();
            unreferencedFiles = state.v2();
            logBootstrapping(indexShard, commit);
        } else {
            commit = null;
            unreferencedFiles = null;
        }
        assert indexShard.routingEntry().isPromotableToPrimary();
        ActionListener.completeWith(listener, () -> {
            final Store store = indexShard.store();
            final var indexDirectory = IndexDirectory.unwrapDirectory(store.directory());
            if (commit != null) {
                indexDirectory.updateCommit(commit, null);
                var warmingService = sharedBlobCacheWarmingService.get();
                warmingService.warmCacheForIndexingShardRecovery(indexShard, commit);
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

            if (commit != null) {
                statelessCommitService.markRecoveredCommit(indexShard.shardId(), commit, unreferencedFiles);
            }
            statelessCommitService.addConsumerForNewUploadedCommit(
                indexShard.shardId(),
                info -> indexDirectory.updateCommit(info.commit(), info.filesToRetain())
            );

            final TranslogReplicator localTranslogReplicator = translogReplicator.get();
            statelessCommitService.addConsumerForNewUploadedCommit(
                indexShard.shardId(),
                info -> localTranslogReplicator.markShardCommitUploaded(indexShard.shardId(), info.commit().translogRecoveryStartFile())
            );
            return null;
        });
    }

    private void beforeRecoveryOnSearchShard(IndexShard indexShard, BlobContainer existingBlobContainer, ActionListener<Void> listener)
        throws IOException {
        final StatelessCompoundCommit commit;
        if (existingBlobContainer != null) {
            commit = ObjectStoreService.readSearchShardState(existingBlobContainer, indexShard.getOperationPrimaryTerm());
            logBootstrapping(indexShard, commit);
        } else {
            commit = null;
        }
        if (commit == null) {
            // TODO: can this happen? Do we need this?
            listener.onResponse(null);
            return;
        }
        final Store store = indexShard.store();
        // On a search shard. First register, then read the new or confirmed commit.
        store.incRef();
        var commitToRegister = commit.primaryTermAndGeneration();
        recoveryCommitRegistrationHandler.get().register(commitToRegister, commit.shardId(), new ActionListener<>() {
            @Override
            public void onResponse(PrimaryTermAndGeneration commitToUse) {
                ActionListener.completeWith(listener, () -> {
                    try {
                        logger.debug(
                            "{} indexing shard's response to registering commit {} for recovery is {}",
                            commit.shardId(),
                            commitToRegister,
                            commitToUse
                        );
                        var searchDirectory = SearchDirectory.unwrapDirectory(store.directory());
                        var compoundCommit = commit;
                        if (commitToRegister.equals(commitToUse) == false) {
                            compoundCommit = ObjectStoreService.readStatelessCompoundCommit(
                                searchDirectory.getBlobContainer(commitToUse.primaryTerm()),
                                commitToUse.generation()
                            );
                        }
                        assert compoundCommit != null;
                        searchDirectory.updateCommit(compoundCommit);
                        return null;
                    } finally {
                        store.decRef();
                    }
                });
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug(() -> format("%s error while registering commit %s for recovery", commit.shardId(), commitToRegister), e);
                store.decRef();
                listener.onFailure(e);
            }
        });
    }

    @Override
    public void afterIndexShardRecovery(IndexShard indexShard, ActionListener<Void> listener) {
        ActionListener.run(listener, l -> {
            if (indexShard.routingEntry().isPromotableToPrimary()) {
                Engine engineOrNull = indexShard.getEngineOrNull();
                if (engineOrNull instanceof IndexEngine engine) {
                    long currentGeneration = engine.getCurrentGeneration();
                    if (currentGeneration > statelessCommitService.getRecoveredGeneration(indexShard.shardId())) {
                        ShardId shardId = indexShard.shardId();
                        statelessCommitService.addListenerForUploadedGeneration(shardId, engine.getCurrentGeneration(), l);
                    } else {
                        engine.flush(true, true, l.map(f -> null));
                    }
                } else if (engineOrNull == null) {
                    throw new IllegalStateException("Engine is null");
                } else {
                    assert engineOrNull instanceof NoOpEngine;
                    l.onResponse(null);
                }
            } else {
                l.onResponse(null);
            }
        });
    }
}
