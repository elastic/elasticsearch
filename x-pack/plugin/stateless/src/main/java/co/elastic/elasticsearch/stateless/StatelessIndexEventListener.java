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
import org.elasticsearch.core.Releasables;
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

import static org.elasticsearch.index.shard.StoreRecovery.bootstrap;

class StatelessIndexEventListener implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(StatelessIndexEventListener.class);

    private final StatelessCommitService statelessCommitService;
    private final ObjectStoreService objectStoreService;
    private final TranslogReplicator translogReplicator;
    private final RecoveryCommitRegistrationHandler recoveryCommitRegistrationHandler;
    private final SharedBlobCacheWarmingService warmingService;

    StatelessIndexEventListener(
        StatelessCommitService statelessCommitService,
        ObjectStoreService objectStoreService,
        TranslogReplicator translogReplicator,
        RecoveryCommitRegistrationHandler recoveryCommitRegistrationHandler,
        SharedBlobCacheWarmingService warmingService
    ) {
        this.statelessCommitService = statelessCommitService;
        this.objectStoreService = objectStoreService;
        this.translogReplicator = translogReplicator;
        this.recoveryCommitRegistrationHandler = recoveryCommitRegistrationHandler;
        this.warmingService = warmingService;
    }

    @Override
    public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
        final Store store = indexShard.store();
        try {
            store.incRef();
            final var blobStore = objectStoreService.blobStore();
            final ShardId shardId = indexShard.shardId();
            final var shardBasePath = objectStoreService.shardBasePath(shardId);
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

    private static void logBootstrapping(IndexShard indexShard, BatchedCompoundCommit latestCommit) {
        if (latestCommit != null) {
            logger.info(
                "{} bootstrapping [{}] shard on primary term [{}] with {} from object store ({})",
                indexShard.shardId(),
                indexShard.routingEntry().role(),
                indexShard.getOperationPrimaryTerm(),
                latestCommit.last().toShortDescription(),
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

    private static void logBootstrapping(
        IndexShard indexShard,
        StatelessCompoundCommit latestCommit,
        PrimaryTermAndGeneration latestUploaded
    ) {
        assert indexShard.routingEntry().isPromotableToPrimary() == false;
        assert latestCommit != null;
        logger.info(
            "{} bootstrapping [{}] shard on primary term [{}] with {} and latest uploaded {} from indexing shard ({})",
            indexShard.shardId(),
            indexShard.routingEntry().role(),
            indexShard.getOperationPrimaryTerm(),
            latestCommit.toShortDescription(),
            latestUploaded,
            indexShard.recoveryState().getRecoverySource()
        );
    }

    private void beforeRecoveryIndexShard(IndexShard indexShard, BlobContainer existingBlobContainer, ActionListener<Void> listener)
        throws IOException {
        final Set<BlobFile> unreferencedFiles;
        final BatchedCompoundCommit batchedCompoundCommit;
        if (existingBlobContainer != null) {
            var state = ObjectStoreService.readIndexingShardState(existingBlobContainer, indexShard.getOperationPrimaryTerm());
            batchedCompoundCommit = state.v1();
            unreferencedFiles = state.v2();
            logBootstrapping(indexShard, batchedCompoundCommit);
        } else {
            batchedCompoundCommit = null;
            unreferencedFiles = null;
        }
        assert indexShard.routingEntry().isPromotableToPrimary();
        ActionListener.completeWith(listener, () -> {
            final Store store = indexShard.store();
            final var indexDirectory = IndexDirectory.unwrapDirectory(store.directory());
            if (batchedCompoundCommit != null) {
                indexDirectory.updateCommit(batchedCompoundCommit.last(), null);
                warmingService.warmCacheForShardRecovery(indexShard, batchedCompoundCommit.last());
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
                statelessCommitService.markRecoveredBcc(indexShard.shardId(), batchedCompoundCommit, unreferencedFiles);
            }
            statelessCommitService.addConsumerForNewUploadedBcc(indexShard.shardId(), info -> {
                for (var compoundCommit : info.uploadedBcc().compoundCommits()) {
                    // TODO: Avoid repeatedly update info.filesToRetain() for each CC. Or can we update only for the last CC?
                    indexDirectory.updateCommit(compoundCommit, info.filesToRetain());
                }
            });

            statelessCommitService.addConsumerForNewUploadedBcc(
                indexShard.shardId(),
                info -> translogReplicator.markShardCommitUploaded(
                    indexShard.shardId(),
                    // Use the largest translog start file from all CCs to release translog files
                    info.uploadedBcc().last().translogRecoveryStartFile()
                )
            );
            return null;
        });
    }

    private void beforeRecoveryOnSearchShard(IndexShard indexShard, BlobContainer blobContainer, ActionListener<Void> listener)
        throws IOException {
        assert blobContainer != null : indexShard.routingEntry();

        final var batchedCompoundCommit = ObjectStoreService.readSearchShardState(blobContainer, indexShard.getOperationPrimaryTerm());
        assert batchedCompoundCommit == null || batchedCompoundCommit.shardId().equals(indexShard.shardId())
            : batchedCompoundCommit.shardId() + " != " + indexShard.shardId();

        final var store = indexShard.store();
        store.incRef();
        final var storeDecRef = Releasables.assertOnce(store::decRef);
        boolean registering = false;
        try {
            recoveryCommitRegistrationHandler.register(
                batchedCompoundCommit != null ? batchedCompoundCommit.primaryTermAndGeneration() : PrimaryTermAndGeneration.ZERO,
                batchedCompoundCommit != null ? batchedCompoundCommit.last().primaryTermAndGeneration() : PrimaryTermAndGeneration.ZERO,
                indexShard.shardId(),
                ActionListener.releaseAfter(listener.delegateFailure((l, response) -> ActionListener.completeWith(listener, () -> {
                    var searchDirectory = SearchDirectory.unwrapDirectory(store.directory());
                    var lastUploaded = response.getLatestUploadedBatchedCompoundCommitTermAndGen();

                    var compoundCommit = response.getCompoundCommit();
                    if (compoundCommit == null) {
                        // The indexing shard provided no compound commit to recover from: it might be on a node with an old version, or it
                        // might be closing its engine.
                        if (PrimaryTermAndGeneration.ZERO.equals(lastUploaded)) {
                            // If there are no compound commit nor last uploaded term/generation to use, recover from an empty commit
                            if (batchedCompoundCommit == null) {
                                logBootstrapping(indexShard, null);
                                return null;
                            }

                            // Otherwise recover from the compound commit found in the object store
                            logBootstrapping(indexShard, batchedCompoundCommit);
                            // TODO Should we revisit this? the indexing shard does not know about the commits used by this search shard
                            // until the next new commit notification.
                            compoundCommit = batchedCompoundCommit.last();
                            lastUploaded = compoundCommit.primaryTermAndGeneration();

                            // TODO removes this branch once indexing shards are upgraded to a version that provides a real or EMPTY commit
                        } else {
                            // If the indexing shard is on an old version: the shard still uploads single compound commits so use the last
                            // uploaded one, which should contain a single compound commit, and for which the indexing shard registered
                            // this search shard.
                            if (batchedCompoundCommit == null || batchedCompoundCommit.primaryTermAndGeneration().before(lastUploaded)) {
                                var latestBatchedCompoundCommit = ObjectStoreService.readBatchedCompoundCommit(
                                    searchDirectory.getBlobContainer(lastUploaded.primaryTerm()),
                                    lastUploaded.generation()
                                );
                                logBootstrapping(indexShard, latestBatchedCompoundCommit);
                                compoundCommit = latestBatchedCompoundCommit.last();
                            } else {
                                logBootstrapping(indexShard, batchedCompoundCommit);
                                compoundCommit = batchedCompoundCommit.last();
                            }
                        }
                    } else {
                        logBootstrapping(indexShard, compoundCommit, lastUploaded);
                    }

                    assert batchedCompoundCommit == null
                        || batchedCompoundCommit.last().primaryTermAndGeneration().onOrBefore(compoundCommit.primaryTermAndGeneration());
                    assert compoundCommit != null;

                    searchDirectory.updateLatestUploadedTermAndGen(lastUploaded);
                    searchDirectory.updateCommit(compoundCommit);
                    warmingService.warmCacheForShardRecovery(indexShard, compoundCommit);
                    return null;
                })), storeDecRef)
            );
            registering = true;
        } finally {
            assert registering : "unexpected exception when calling register()";
            if (registering == false) {
                Releasables.close(storeDecRef);
            }
        }
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
