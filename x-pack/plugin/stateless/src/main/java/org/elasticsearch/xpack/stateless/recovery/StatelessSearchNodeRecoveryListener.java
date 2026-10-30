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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.NoOpEngine;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.stateless.cache.SearchCommitPrefetcherDynamicSettings;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService.WarmTarget;
import org.elasticsearch.xpack.stateless.cache.WarmingRatioProviderFactory;
import org.elasticsearch.xpack.stateless.cache.reader.VectorFillPolicy;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
import org.elasticsearch.xpack.stateless.commits.CommitPointer;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.NewCommitNotification;
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

import static org.elasticsearch.core.Strings.format;
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
    private final WarmingRatioProviderFactory warmingRatioProviderFactory;

    public StatelessSearchNodeRecoveryListener(
        ObjectStoreService objectStoreService,
        RecoveryCommitRegistrationHandler recoveryCommitRegistrationHandler,
        SharedBlobCacheWarmingService warmingService,
        ProjectResolver projectResolver,
        Executor bccHeaderReadExecutor,
        ClusterService clusterService,
        WarmingRatioProviderFactory warmingRatioProviderFactory
    ) {
        super(objectStoreService, projectResolver);
        this.recoveryCommitRegistrationHandler = recoveryCommitRegistrationHandler;
        this.warmingService = warmingService;
        this.bccHeaderReadExecutor = bccHeaderReadExecutor;
        this.useInternalFilesReplicatedContentForSearchShards = clusterService.getClusterSettings()
            .get(SearchCommitPrefetcherDynamicSettings.STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT);
        this.clusterService = clusterService;
        this.warmingRatioProviderFactory = warmingRatioProviderFactory;
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
            final var clusterSettings = clusterService.getClusterSettings();
            if (clusterSettings.get(FastSearchActivationSettings.FAST_ACTIVATION_ENABLED_SETTING) && existingBlobContainer != null) {
                if (clusterSettings.get(
                    FastSearchActivationSettings.FAST_ACTIVATION_DISCOVERY_SETTING
                ) == FastSearchActivationSettings.Discovery.POINTER) {
                    pointerRecoveryOnSearchShard(indexShard, existingBlobContainer, listener);
                } else {
                    fastRecoveryOnSearchShard(indexShard, existingBlobContainer, listener);
                }
            } else {
                beforeRecoveryOnSearchShard(indexShard, existingBlobContainer, listener);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Fast cold activation (prototype, see {@link FastSearchActivationSettings}): registers with the indexing shard first and recovers
     * from the compound commit it returns, without any object store request for discovery (no LIST, no batched compound commit header
     * read, no referenced compound commit header reads). Then prefetches in parallel only what opening the reader needs and resumes
     * recovery once that is in the cache. Falls back to {@link #beforeRecoveryOnSearchShard} (object store discovery) when the
     * indexing shard returns no commit or when the registration fails.
     */
    private void fastRecoveryOnSearchShard(IndexShard indexShard, BlobContainer blobContainer, ActionListener<Void> listener) {
        assert blobContainer != null : indexShard.routingEntry();
        final var clusterSettings = clusterService.getClusterSettings();
        final long wholeBlobThreshold = clusterSettings.get(FastSearchActivationSettings.FAST_ACTIVATION_WHOLE_BLOB_THRESHOLD_SETTING)
            .getBytes();
        final var fallback = new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {
                try {
                    beforeRecoveryOnSearchShard(indexShard, blobContainer, listener);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        };
        recoveryCommitRegistrationHandler.register(
            PrimaryTermAndGeneration.ZERO,
            PrimaryTermAndGeneration.ZERO,
            indexShard.shardId(),
            new ActionListener<>() {
                @Override
                public void onResponse(RegisterCommitResponse response) {
                    final StatelessCompoundCommit compoundCommit = response.getCompoundCommit();
                    if (compoundCommit == null) {
                        logger.debug(
                            "{} fast activation: indexing shard returned no commit, discovering it in the object store",
                            indexShard
                        );
                        fallback.onResponse(null);
                        return;
                    }
                    ActionListener.run(listener, l -> {
                        logBootstrappingFromIndexingShard(
                            indexShard,
                            compoundCommit,
                            response.getLatestUploadedBatchedCompoundCommitTermAndGen()
                        );
                        final var searchDirectory = SearchDirectory.unwrapDirectory(indexShard.store().directory());
                        applyActivationFillSettings(searchDirectory);
                        searchDirectory.updateLatestUploadedBcc(response.getLatestUploadedBatchedCompoundCommitTermAndGen());
                        searchDirectory.updateLatestCommitInfo(compoundCommit.primaryTermAndGeneration(), response.getNodeId());
                        searchDirectory.updateCommit(compoundCommit);
                        warmingService.warmCacheForFastSearchActivation(
                            indexShard,
                            compoundCommit,
                            searchDirectory,
                            wholeBlobThreshold,
                            ActionListener.wrap(ignored -> l.onResponse(null), e -> {
                                logger.warn(() -> format("%s fast activation warming failed", indexShard.shardId()), e);
                                l.onResponse(null);
                            })
                        );
                    });
                }

                @Override
                public void onFailure(Exception e) {
                    logger.info(
                        () -> format("%s fast activation: registration failed, discovering the commit in the object store", indexShard),
                        e
                    );
                    fallback.onResponse(null);
                }
            }
        );
    }

    /**
     * Installs the activation-time fill sizes on the directory (query fill, header/footer warming fill, vector policy) and schedules
     * their end after {@link FastSearchActivationSettings#FAST_ACTIVATION_FILL_WINDOW_SETTING}.
     */
    private void applyActivationFillSettings(SearchDirectory searchDirectory) {
        final var clusterSettings = clusterService.getClusterSettings();
        searchDirectory.setObjectStoreRangeSizeOverride(
            clusterSettings.get(FastSearchActivationSettings.FAST_ACTIVATION_RANGE_SIZE_SETTING).getBytes()
        );
        searchDirectory.setWarmingRangeSizeOverride(
            clusterSettings.get(FastSearchActivationSettings.FAST_ACTIVATION_HEADER_RANGE_SIZE_SETTING).getBytes()
        );
        searchDirectory.setVectorFillPolicy(
            new VectorFillPolicy(
                clusterSettings.get(FastSearchActivationSettings.FAST_ACTIVATION_VECTOR_RANGE_SIZE_SETTING).getBytes(),
                clusterSettings.get(FastSearchActivationSettings.FAST_ACTIVATION_VECTOR_WHOLE_FILE_MAX_SETTING).getBytes()
            )
        );
        final TimeValue window = clusterSettings.get(FastSearchActivationSettings.FAST_ACTIVATION_FILL_WINDOW_SETTING);
        if (window.millis() > 0) {
            final var threadPool = clusterService.threadPool();
            threadPool.schedule(searchDirectory::clearActivationFillOverrides, window, threadPool.generic());
        }
    }

    /**
     * Pointer-based activation (prototype, step 2b, see {@link FastSearchActivationSettings.Discovery#POINTER}): reads the shard's
     * {@link CommitPointer} for the primary term known from the cluster state with one GET (no LIST, no BCC/CC header read), validates
     * it and opens its commit, using the replicated header/footer ranges it carries for the warming. The indexing shard is not
     * contacted on the activation path: registration runs in the background and only (a) lets the indexing shard retain the commit's
     * blobs for this search shard and run its stale-commit check with the pointer's term/generations, and (b) delivers a newer commit
     * than the pointer's (if any) once the engine is open. Falls back to {@link #beforeRecoveryOnSearchShard} (object store discovery
     * and blocking registration) if the pointer is missing, unreadable or does not match, or if warming its commit fails.
     */
    private void pointerRecoveryOnSearchShard(IndexShard indexShard, BlobContainer blobContainer, ActionListener<Void> listener) {
        final long knownPrimaryTerm = indexShard.getOperationPrimaryTerm();
        final ActionListener<Void> fallback = ActionListener.wrap(ignored -> {
            try {
                beforeRecoveryOnSearchShard(indexShard, blobContainer, listener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure);

        CommitPointer pointer = null;
        String invalid;
        logger.debug("{} fast activation: reading commit pointer of term [{}]", indexShard.shardId(), knownPrimaryTerm);
        try {
            pointer = CommitPointer.read(blobContainer, knownPrimaryTerm);
            invalid = pointer == null
                ? "no commit pointer for term [" + knownPrimaryTerm + "]"
                : pointer.validate(indexShard.shardId(), knownPrimaryTerm);
        } catch (Exception e) {
            invalid = "commit pointer unreadable: " + e;
        }
        if (invalid != null) {
            logger.info("{} fast activation: {}, discovering the commit in the object store", indexShard.shardId(), invalid);
            fallback.onResponse(null);
            return;
        }
        final CommitPointer commitPointer = pointer;
        final StatelessCompoundCommit compoundCommit = commitPointer.compoundCommit();
        logger.debug(
            "{} fast activation: read commit pointer {} in BCC {} with {} replicated file ranges",
            indexShard.shardId(),
            compoundCommit.toShortDescription(),
            commitPointer.batchedCompoundCommitTermAndGeneration(),
            commitPointer.replicatedFileRanges().size()
        );
        ActionListener.run(listener, l -> {
            final var searchDirectory = SearchDirectory.unwrapDirectory(indexShard.store().directory());
            applyActivationFillSettings(searchDirectory);
            searchDirectory.updateLatestUploadedBcc(commitPointer.batchedCompoundCommitTermAndGeneration());
            searchDirectory.updateLatestCommitInfo(compoundCommit.primaryTermAndGeneration(), commitPointer.writerNodeId());
            searchDirectory.updateCommit(compoundCommit, commitPointer.replicatedFileRanges());
            registerInBackground(indexShard, commitPointer);
            final long wholeBlobThreshold = clusterService.getClusterSettings()
                .get(FastSearchActivationSettings.FAST_ACTIVATION_WHOLE_BLOB_THRESHOLD_SETTING)
                .getBytes();
            warmingService.warmCacheForFastSearchActivation(
                indexShard,
                compoundCommit,
                searchDirectory,
                wholeBlobThreshold,
                ActionListener.wrap(ignored -> l.onResponse(null), e -> {
                    // e.g. a blob of the pointed commit is gone (deleted after a newer commit): discover the latest commit instead
                    logger.info(
                        () -> format("%s fast activation from commit pointer failed, discovering the commit", indexShard.shardId()),
                        e
                    );
                    fallback.onResponse(null);
                })
            );
        });
    }

    /**
     * Commits returned by a background registration that are newer than the commit the shard was opened from, delivered to the search
     * engine as a new commit notification once it exists (see {@link #afterIndexShardRecovery}).
     */
    private final Map<IndexShard, NewCommitNotification> pendingRegistrationCommits = ConcurrentCollections.newConcurrentMap();

    private void registerInBackground(IndexShard indexShard, CommitPointer pointer) {
        recoveryCommitRegistrationHandler.register(
            pointer.batchedCompoundCommitTermAndGeneration(),
            pointer.compoundCommit().primaryTermAndGeneration(),
            indexShard.shardId(),
            new ActionListener<>() {
                @Override
                public void onResponse(RegisterCommitResponse response) {
                    final var commit = response.getCompoundCommit();
                    logger.debug(
                        "{} fast activation: background registration done, indexing shard commit {}",
                        indexShard.shardId(),
                        commit == null ? "none" : commit.toShortDescription()
                    );
                    if (commit == null || commit.generation() <= pointer.compoundCommit().generation()) {
                        return;
                    }
                    final var notification = new NewCommitNotification(
                        commit,
                        commit.getContainingBccBlobFile().termAndGeneration().generation(),
                        response.getLatestUploadedBatchedCompoundCommitTermAndGen(),
                        clusterService.state().version(),
                        response.getNodeId()
                    );
                    pendingRegistrationCommits.put(indexShard, notification);
                    if (indexShard.getEngineOrNull() instanceof SearchEngine) {
                        deliverPendingRegistrationCommit(indexShard);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug(() -> format("%s fast activation: background registration failed", indexShard.shardId()), e);
                }
            }
        );
    }

    private void deliverPendingRegistrationCommit(IndexShard indexShard) {
        final var notification = pendingRegistrationCommits.remove(indexShard);
        if (notification != null && indexShard.getEngineOrNull() instanceof SearchEngine searchEngine) {
            searchEngine.onCommitNotification(
                notification,
                ActionListener.wrap(
                    ignored -> {},
                    e -> logger.debug(() -> format("%s failed to apply commit from background registration", indexShard.shardId()), e)
                )
            );
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

        final var warmingRatioProvider = warmingRatioProviderFactory.create(indexShard.shardId());

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
                                // Aggregate a single warm target per BCC blob: the furthest offset to warm, stamped with the most recent
                                // representative timestamp among the referenced CCs sharing that blob.
                                // blobSize is 0 as a sentinel until the bccBlobSizeConsumer fills it in.
                                targetsToWarm.merge(
                                    bccBlobFile,
                                    warmingService.computeWarmTarget(warmingRatioProvider, referencedCompoundCommit, searchDirectory),
                                    WarmTarget::merge
                                );
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
                        clusterService::state,
                        indexShard,
                        compoundCommit,
                        searchDirectory,
                        warmingRatioProvider,
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
        afterSearchShardRecovery(indexShard, listener.delegateFailure((l, v) -> {
            deliverPendingRegistrationCommit(indexShard);
            l.onResponse(v);
        }));
    }

    @Override
    public void afterIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null) {
            pendingRegistrationCommits.remove(indexShard);
        }
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
