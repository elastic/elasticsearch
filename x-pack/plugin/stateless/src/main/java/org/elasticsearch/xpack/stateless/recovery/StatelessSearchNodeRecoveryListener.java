/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.lucene.store.IOContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.NoOpEngine;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.stateless.cache.SearchCommitPrefetcherDynamicSettings;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService.WarmTarget;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.engine.SearchEngine;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;
import org.elasticsearch.xpack.stateless.lucene.SearchDirectory;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;

import static org.elasticsearch.xpack.stateless.commits.BlobFileRanges.computeBlobFileRanges;

/**
 * {@link IndexEventListener} that drives shard recovery from the object store on stateless search nodes.
 */
public class StatelessSearchNodeRecoveryListener extends AbstractStatelessRecoveryListener implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(StatelessSearchNodeRecoveryListener.class);

    private final RecoveryCommitRegistrationHandler recoveryCommitRegistrationHandler;
    private final SharedBlobCacheWarmingService warmingService;
    private final Executor bccHeaderReadExecutor;
    private final boolean useInternalFilesReplicatedContentForSearchShards;
    private final ClusterService clusterService;

    public StatelessSearchNodeRecoveryListener(
        ObjectStoreService objectStoreService,
        RecoveryCommitRegistrationHandler recoveryCommitRegistrationHandler,
        SharedBlobCacheWarmingService warmingService,
        ProjectResolver projectResolver,
        Executor bccHeaderReadExecutor,
        ClusterService clusterService
    ) {
        super(objectStoreService, projectResolver);
        this.recoveryCommitRegistrationHandler = recoveryCommitRegistrationHandler;
        this.warmingService = warmingService;
        this.bccHeaderReadExecutor = bccHeaderReadExecutor;
        this.useInternalFilesReplicatedContentForSearchShards = clusterService.getClusterSettings()
            .get(SearchCommitPrefetcherDynamicSettings.STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT);
        this.clusterService = clusterService;
    }

    @Override
    public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
        assert indexShard.routingEntry().isSearchable();
        beforeSearchShardRecovery(indexShard, listener);
    }

    private void beforeSearchShardRecovery(final IndexShard indexShard, final ActionListener<Void> listener) {
        final Store store = indexShard.store();
        // Store ref is held by IndicesService for the recovery lifetime.
        assert store.hasReferences();
        try {
            final var existingBlobContainer = initializeBlobContainer(indexShard, store);
            beforeRecoveryOnSearchShard(indexShard, existingBlobContainer, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void beforeRecoveryOnSearchShard(IndexShard indexShard, BlobContainer blobContainer, ActionListener<Void> listener)
        throws IOException {
        assert indexShard.store().refCount() > 0 : indexShard.shardId();
        assert blobContainer != null : indexShard.routingEntry();

        final var searchDirectory = SearchDirectory.unwrapDirectory(indexShard.store().directory());
        final boolean timestampBackfillEnabled = useInternalFilesReplicatedContentForSearchShards
            && searchDirectory.timestampBackfillEnabled();
        final var metadataReadDirectory = searchDirectory.createMetadataReadDirectory(timestampBackfillEnabled);
        final var batchedCompoundCommit = objectStoreService.readSearchShardState(
            blobContainer,
            searchDirectory,
            metadataReadDirectory,
            indexShard.getOperationPrimaryTerm()
        );
        assert batchedCompoundCommit == null || batchedCompoundCommit.shardId().equals(indexShard.shardId())
            : batchedCompoundCommit.shardId() + " != " + indexShard.shardId();

        recoveryCommitRegistrationHandler.register(
            batchedCompoundCommit != null ? batchedCompoundCommit.primaryTermAndGeneration() : PrimaryTermAndGeneration.ZERO,
            batchedCompoundCommit != null
                ? batchedCompoundCommit.lastCompoundCommit().primaryTermAndGeneration()
                : PrimaryTermAndGeneration.ZERO,
            indexShard.shardId(),
            listener.<RegisterCommitResponse>delegateFailure((l, response) -> {
                var lastUploaded = response.getLatestUploadedBatchedCompoundCommitTermAndGen();
                var nodeId = response.getNodeId();
                assert nodeId != null : response;

                final StatelessCompoundCommit compoundCommit;
                if (response.getCompoundCommit() == null) {
                    // If the indexing shard provided no compound commit to recover from, then the last uploaded BCC term/gen returned
                    // should be equal to zero indicated the indexing shard's engine is null or is a NoOpEngine
                    assert PrimaryTermAndGeneration.ZERO.equals(lastUploaded) : lastUploaded;

                    logBootstrappingFromObjectStore(logger, indexShard, batchedCompoundCommit);
                    // If there is no batched compound commit found in the object store, then recover from an empty commit
                    if (batchedCompoundCommit == null) {
                        l.onResponse(null);
                        return;
                    }

                    // Otherwise recover from the compound commit found in the object store
                    // TODO Should we revisit this? the indexing shard does not know about the commits used by this search shard
                    // until the next new commit notification.
                    compoundCommit = batchedCompoundCommit.lastCompoundCommit();
                    lastUploaded = batchedCompoundCommit.primaryTermAndGeneration();
                } else {
                    compoundCommit = response.getCompoundCommit();
                    logBootstrappingFromIndexingShard(indexShard, compoundCommit, lastUploaded);
                }

                assert batchedCompoundCommit == null
                    || batchedCompoundCommit.lastCompoundCommit()
                        .primaryTermAndGeneration()
                        .onOrBefore(compoundCommit.primaryTermAndGeneration());

                searchDirectory.updateLatestUploadedBcc(lastUploaded);
                searchDirectory.updateLatestCommitInfo(compoundCommit.primaryTermAndGeneration(), nodeId);

                SubscribableListener.<SearchRecoveryWarmingInputs>newForked(l2 -> {
                    if (useInternalFilesReplicatedContentForSearchShards) {
                        Map<String, BlobFileRanges> blobFileRanges = ConcurrentCollections.newConcurrentMap();
                        Map<BlobFile, WarmTarget> targetsToWarm = ConcurrentCollections.newConcurrentMap();
                        ObjectStoreService.readReferencedCompoundCommitsUsingCache(
                            compoundCommit.commitFiles(),
                            batchedCompoundCommit,
                            metadataReadDirectory,
                            IOContext.DEFAULT,
                            bccHeaderReadExecutor,
                            referencedCompoundCommit -> {
                                blobFileRanges.putAll(
                                    computeBlobFileRanges(
                                        true,
                                        referencedCompoundCommit.statelessCompoundCommitReference().compoundCommit(),
                                        referencedCompoundCommit.statelessCompoundCommitReference().headerOffsetInTheBccBlobFile(),
                                        referencedCompoundCommit.referencedInternalFiles()
                                    )
                                );
                                var bccBlobFile = referencedCompoundCommit.statelessCompoundCommitReference().bccBlobFile();
                                var offset = warmingService.byteRangeToWarmForCC(referencedCompoundCommit).end();
                                // Aggregate a single warm target per BCC blob: the furthest offset to warm, stamped with the most recent
                                // representative timestamp among the referenced CCs sharing that blob.
                                // blobSize is 0 as a sentinel until the bccBlobSizeConsumer fills it in.
                                long ccTimestamp = searchDirectory.resolveRegionTimestampMillis(
                                    referencedCompoundCommit.statelessCompoundCommitReference()
                                        .compoundCommit()
                                        .getTimestampFieldValueRange()
                                );
                                targetsToWarm.merge(bccBlobFile, new WarmTarget(offset, 0L, ccTimestamp), WarmTarget::merge);
                            },
                            (blobFile, bccSize) -> {
                                assert targetsToWarm.containsKey(blobFile);
                                targetsToWarm.merge(blobFile, WarmTarget.withUnknownTimestamp(0L, bccSize), WarmTarget::merge);
                            },
                            l2.map(aVoid -> {
                                assert targetsToWarm.values().stream().allMatch(t -> t.blobSize() > 0);
                                var timestampByCacheKey = Maps.<FileCacheKey, Long>newHashMapWithExpectedSize(targetsToWarm.size());
                                for (var entry : targetsToWarm.entrySet()) {
                                    timestampByCacheKey.put(
                                        new FileCacheKey(searchDirectory.getShardId(), entry.getKey()),
                                        entry.getValue().timestampMillis()
                                    );
                                }
                                if (timestampBackfillEnabled) {
                                    // This backfill also handles the initial BCC read in readSearchShardState.
                                    searchDirectory.backfillMetadataReadTimestamps(Collections.unmodifiableMap(timestampByCacheKey), true);
                                }
                                return new SearchRecoveryWarmingInputs(blobFileRanges, targetsToWarm);
                            })
                        );
                    } else {
                        l2.onResponse(null);
                    }
                }).addListener(l.delegateFailureAndWrap((l3, warmingInputs) -> {
                    final var resumeRecovery = new ActionListener<Void>() {
                        @Override
                        public void onResponse(Void unused) {
                            assert indexShard.store().refCount() > 0 : indexShard.shardId();
                            l3.onResponse(null);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.warn("warming failed: {}", e.getMessage(), e);
                            onResponse(null);
                        }
                    };

                    if (warmingInputs != null) {
                        searchDirectory.updateCommit(compoundCommit, warmingInputs.blobFileRanges());
                    } else {
                        searchDirectory.updateCommit(compoundCommit);
                    }
                    warmingService.warmCacheForSearchShardRecovery(
                        clusterService.state(),
                        indexShard,
                        compoundCommit,
                        searchDirectory,
                        warmingInputs != null ? warmingInputs.targetsToWarm() : null,
                        resumeRecovery
                    );
                }));
            })
        );
    }

    @Override
    public void afterIndexShardRecovery(IndexShard indexShard, ActionListener<Void> listener) {
        assert indexShard.routingEntry().isSearchable();
        afterSearchShardRecovery(indexShard, listener);
    }

    private static void afterSearchShardRecovery(final IndexShard indexShard, final ActionListener<Void> listener) {
        ActionListener.run(listener, l -> indexShard.withEngineException(engine -> {
            switch (engine) {
                case SearchEngine searchEngine -> {
                    /*
                     * The shard can be closed underneath us, so we assert that we're either
                     * recovering or closed at this point
                     */
                    assert indexShard.state() == IndexShardState.RECOVERING || indexShard.state() == IndexShardState.CLOSED
                        : "expected index in recovering shard state but is: " + indexShard.state();
                    assert indexShard.routingEntry().state() == ShardRoutingState.INITIALIZING
                        : "expected initializing shard routing state but is: " + indexShard.state();
                    indexShard.updateGlobalCheckpointOnReplica(searchEngine.getLastSyncedGlobalCheckpoint(), "search shard recovery");
                    searchEngine.afterRecovery();
                    l.onResponse(null);
                }
                case NoOpEngine ignored -> l.onResponse(null);
                default -> throw new AssertionError("unexpected engine type: " + engine);
            }
            return null;
        }));
    }

    private static void logBootstrappingFromIndexingShard(
        IndexShard indexShard,
        StatelessCompoundCommit latestCommit,
        PrimaryTermAndGeneration latestUploaded
    ) {
        assert indexShard.routingEntry().isPromotableToPrimary() == false;
        assert latestCommit != null;
        boolean uploaded = latestCommit.getContainingBccBlobFile().termAndGeneration().onOrBefore(latestUploaded);
        logger.info(
            "{} with UUID [{}] bootstrapping [{}] shard on primary term [{}] with {} ({}) and latest uploaded {} from indexing shard ({})",
            indexShard.shardId(),
            indexShard.shardId().getIndex().getUUID(),
            indexShard.routingEntry().role(),
            indexShard.getOperationPrimaryTerm(),
            latestCommit.toShortDescription(),
            uploaded ? "uploaded" : "pending upload",
            latestUploaded,
            describe(indexShard.recoveryState())
        );
    }

    private record SearchRecoveryWarmingInputs(Map<String, BlobFileRanges> blobFileRanges, Map<BlobFile, WarmTarget> targetsToWarm) {
        public SearchRecoveryWarmingInputs {
            Objects.requireNonNull(blobFileRanges);
            Objects.requireNonNull(targetsToWarm);
        }
    }
}
