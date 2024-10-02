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
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;
import co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectory;
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
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

import static co.elastic.elasticsearch.stateless.commits.BlobFileRanges.computeLastCommitBlobFileRanges;
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
            BlobStoreCacheDirectory.unwrapDirectory(store.directory())
                .setBlobContainer(primaryTerm -> blobStore.blobContainer(shardBasePath.add(String.valueOf(primaryTerm))));
            final BlobContainer existingBlobContainer = hasNoExistingBlobContainer(indexShard.recoveryState().getRecoverySource())
                ? null
                : blobStore.blobContainer(shardBasePath);
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

    private void beforeRecoveryIndexShard(IndexShard indexShard, BlobContainer existingBlobContainer, ActionListener<Void> listener)
        throws IOException {
        final ObjectStoreService.IndexingShardState indexingShardState;
        if (existingBlobContainer != null) {
            indexingShardState = ObjectStoreService.readIndexingShardState(existingBlobContainer, indexShard.getOperationPrimaryTerm());
        } else {
            indexingShardState = ObjectStoreService.IndexingShardState.EMPTY;
        }
        logBootstrapping(indexShard, indexingShardState.latestCommit());

        assert indexShard.routingEntry().isPromotableToPrimary();
        ActionListener.completeWith(listener, () -> {
            final Store store = indexShard.store();
            final var indexDirectory = IndexDirectory.unwrapDirectory(store.directory());

            final var batchedCompoundCommit = indexingShardState.latestCommit();
            if (batchedCompoundCommit != null) {
                var recoveryCommit = batchedCompoundCommit.lastCompoundCommit();
                // Build a map of BlobFileRanges that includes replicated ranges (ES-9344)
                var blobFileRanges = computeLastCommitBlobFileRanges(batchedCompoundCommit, statelessCommitService.useReplicatedRanges());
                assert blobFileRanges.keySet().containsAll(recoveryCommit.commitFiles().keySet());

                indexDirectory.updateRecoveryCommit(
                    recoveryCommit.generation(),
                    recoveryCommit.nodeEphemeralId(),
                    recoveryCommit.translogRecoveryStartFile(),
                    recoveryCommit.getAllFilesSizeInBytes(),
                    blobFileRanges
                );
                warmingService.warmCacheForShardRecovery(
                    "indexing",
                    indexShard,
                    recoveryCommit,
                    indexDirectory.getBlobStoreCacheDirectory()
                );
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
                statelessCommitService.markRecoveredBcc(
                    indexShard.shardId(),
                    batchedCompoundCommit,
                    indexingShardState.unreferencedBlobs()
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
                info -> translogReplicator.markShardCommitUploaded(
                    indexShard.shardId(),
                    // Use the largest translog start file from all CCs to release translog files
                    info.uploadedBcc().lastCompoundCommit().translogRecoveryStartFile()
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
                batchedCompoundCommit != null
                    ? batchedCompoundCommit.lastCompoundCommit().primaryTermAndGeneration()
                    : PrimaryTermAndGeneration.ZERO,
                indexShard.shardId(),
                ActionListener.releaseAfter(listener.delegateFailure((l, response) -> ActionListener.completeWith(listener, () -> {
                    var searchDirectory = SearchDirectory.unwrapDirectory(store.directory());
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
                    warmingService.warmCacheForShardRecovery("search", indexShard, compoundCommit, searchDirectory);
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
