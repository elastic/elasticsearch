/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.lucene.store.IOContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterState;
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
import org.elasticsearch.xpack.stateless.allocation.CheapShardParkedAllocationDecider;
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
import java.util.concurrent.atomic.AtomicBoolean;

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
            if (FastSearchActivationSettings.getOrDefault(clusterSettings, FastSearchActivationSettings.FAST_ACTIVATION_ENABLED_SETTING)
                && existingBlobContainer != null) {
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
        final long knownPrimaryTerm = pointerPrimaryTerm(indexShard);
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
            // Stale isolated primary check (phase 3, gap 1): if the pointed commit is of an older term than the current primary's, read
            // the pointers of the older terms (usually 1 GET) in parallel with the warming below, and do not serve the commit if one of
            // them names a newer commit: fall back to discovery + registration with that commit, which fails the stale (hollow) primary
            // exactly like the default recovery does (see StatelessCommitService#registerCommitForUnpromotableRecovery).
            final SubscribableListener<String> predecessorCheck = compoundCommit.primaryTermAndGeneration().primaryTerm() < knownPrimaryTerm
                ? SubscribableListener.newForked(
                    l2 -> bccHeaderReadExecutor.execute(
                        ActionRunnable.supply(
                            l2,
                            () -> CommitPointer.checkPredecessorPointers(
                                blobContainer,
                                indexShard.shardId(),
                                knownPrimaryTerm,
                                compoundCommit
                            )
                        )
                    )
                )
                : SubscribableListener.newSucceeded(null);
            final var searchDirectory = SearchDirectory.unwrapDirectory(indexShard.store().directory());
            applyActivationFillSettings(searchDirectory);
            searchDirectory.updateLatestUploadedBcc(commitPointer.batchedCompoundCommitTermAndGeneration());
            searchDirectory.updateLatestCommitInfo(compoundCommit.primaryTermAndGeneration(), commitPointer.writerNodeId());
            searchDirectory.updateCommit(compoundCommit, commitPointer.replicatedFileRanges());
            // Read failure after activation (phase 3, gap 3): the indexing shard does not protect the pointed commit's blobs until
            // (and, for blobs that only this commit references, not even after) the background registration. If a query later finds
            // one of them missing, fail the shard so that it recovers again (reading the then-current pointer, or discovering the
            // commit) instead of failing every search on the missing range until it is relocated.
            searchDirectory.setMissingBlobListener(e -> failShardOnMissingBlob(indexShard, e));
            registerInBackground(indexShard, commitPointer);
            final long wholeBlobThreshold = clusterService.getClusterSettings()
                .get(FastSearchActivationSettings.FAST_ACTIVATION_WHOLE_BLOB_THRESHOLD_SETTING)
                .getBytes();
            final Runnable afterWarming = () -> predecessorCheck.addListener(ActionListener.wrap(staleReason -> {
                if (staleReason == null) {
                    l.onResponse(null);
                } else {
                    logger.info(
                        "{} fast activation: not using commit pointer {}: {}, discovering the commit in the object store",
                        indexShard.shardId(),
                        compoundCommit.primaryTermAndGeneration(),
                        staleReason
                    );
                    searchDirectory.setMissingBlobListener(null);
                    fallback.onResponse(null);
                }
            }, e -> {
                logger.info(() -> format("%s fast activation: older-term pointer check failed, discovering the commit", indexShard), e);
                searchDirectory.setMissingBlobListener(null);
                fallback.onResponse(null);
            }));
            final ActionListener<Void> warmingListener = ActionListener.wrap(ignored -> afterWarming.run(), e -> {
                // e.g. a blob of the pointed commit is gone (deleted after a newer commit): discover the latest commit instead
                logger.info(() -> format("%s fast activation from commit pointer failed, discovering the commit", indexShard.shardId()), e);
                searchDirectory.setMissingBlobListener(null);
                fallback.onResponse(null);
            });
            // Phase 4: if the pointer carries the bytes that opening the reader reads, write them in the cache (no object store read) and
            // only warm whole small blobs; if writing them fails, warm the headers/footers/metadata from the object store as before.
            if (commitPointer.openRanges().isEmpty() == false
                && clusterService.getClusterSettings().get(FastSearchActivationSettings.FAST_ACTIVATION_USE_POINTER_OPEN_BYTES_SETTING)) {
                logger.debug(
                    "{} fast activation: seeding {} open-time ranges ({} bytes) from the commit pointer",
                    indexShard.shardId(),
                    commitPointer.openRanges().size(),
                    commitPointer.openRangesSizeInBytes()
                );
                warmingService.seedCacheFromCommitPointer(indexShard.shardId(), commitPointer.openRanges(), ActionListener.wrap(ignored -> {
                    logger.debug("{} fast activation: seeded open-time ranges from the commit pointer", indexShard.shardId());
                    searchDirectory.setReadPresentWithoutFill();
                    // Phase 5 (item 3): start the fills of the small files that first queries read with dependent reads; not awaited
                    final long queryRangesMaxSize = clusterService.getClusterSettings()
                        .get(FastSearchActivationSettings.FAST_ACTIVATION_PREFETCH_QUERY_RANGES_MAX_SIZE_SETTING)
                        .getBytes();
                    if (queryRangesMaxSize > 0) {
                        clusterService.threadPool()
                            .generic()
                            .execute(
                                () -> ActivationQueryRangesPrefetcher.prefetch(
                                    searchDirectory,
                                    compoundCommit.commitFiles(),
                                    queryRangesMaxSize
                                )
                            );
                    }
                    // Phase 5 (item 4): the reader needs nothing beyond the seeded bytes, so whole small blobs need not be warmed
                    final boolean skipWholeBlobs = clusterService.getClusterSettings()
                        .get(FastSearchActivationSettings.FAST_ACTIVATION_SKIP_WHOLE_BLOB_WARMING_WITH_OPEN_BYTES_SETTING);
                    warmingService.warmCacheForFastSearchActivation(
                        indexShard,
                        compoundCommit,
                        searchDirectory,
                        skipWholeBlobs ? 0L : wholeBlobThreshold,
                        true,
                        warmingListener
                    );
                }, e -> {
                    logger.info(
                        () -> format("%s fast activation: seeding open-time bytes failed, warming headers", indexShard.shardId()),
                        e
                    );
                    warmingService.warmCacheForFastSearchActivation(
                        indexShard,
                        compoundCommit,
                        searchDirectory,
                        wholeBlobThreshold,
                        false,
                        warmingListener
                    );
                }));
            } else {
                warmingService.warmCacheForFastSearchActivation(
                    indexShard,
                    compoundCommit,
                    searchDirectory,
                    wholeBlobThreshold,
                    warmingListener
                );
            }
        });
    }

    private static void failShardOnMissingBlob(IndexShard indexShard, Exception e) {
        try {
            indexShard.failShard(format("%s blob missing after activation from commit pointer", indexShard.shardId()), e);
        } catch (Exception inner) {
            e.addSuppressed(inner);
            logger.debug(() -> format("%s failed to fail shard on missing blob", indexShard.shardId()), e);
        }
    }

    /**
     * Commits returned by a background registration that are newer than the commit the shard was opened from, delivered to the search
     * engine as a new commit notification once it exists (see {@link #afterIndexShardRecovery}).
     */
    private final Map<IndexShard, NewCommitNotification> pendingRegistrationCommits = ConcurrentCollections.newConcurrentMap();

    /**
     * The primary term whose commit pointer names the shard's current commit: the cluster state's primary term, except for a parked
     * index (phase 3, {@code index.cheap_shard.parked}) whose primary is not active. Parking fails the active primary of term T, which
     * bumps the term to T + 1 (IndexMetadataUpdater#shardFailed) without any primary ever being assigned at T + 1 (the parked decider
     * keeps it unassigned, and assigning it would bump the term again), so the last pointer is the one of term T. Reading it directly
     * avoids a missing-pointer GET and the LIST fallback; the stale-primary check then covers the terms before T as usual.
     */
    private long pointerPrimaryTerm(IndexShard indexShard) {
        final long term = indexShard.getOperationPrimaryTerm();
        if (CheapShardParkedAllocationDecider.PARKED_SETTING.get(indexShard.indexSettings().getSettings())
            && term > 1
            && isPrimaryActive(clusterService.state(), indexShard.shardId()) == false) {
            logger.debug("{} fast activation: index is parked, reading the commit pointer of term [{}]", indexShard.shardId(), term - 1);
            return term - 1;
        }
        return term;
    }

    /**
     * Both tiers to zero (phase 3): background registrations of pointer-activated shards whose indexing shard is not active (parked,
     * {@code index.cheap_shard.parked}). Sending them would only wait 10 s for a primary and fail; instead they are sent once the
     * cluster state shows an active primary (see {@link #registerDeferred}). Nothing can write while there is no primary, so the pointed
     * commit is current until then, and a primary that recovers while this copy is assigned retains every commit it lists for all
     * assigned search nodes (StatelessCommitService#registerVirtualBccForUnpromotableRecovery is not needed for that).
     */
    private final Map<IndexShard, CommitPointer> deferredRegistrations = ConcurrentCollections.newConcurrentMap();
    private final AtomicBoolean deferredRegistrationListenerAdded = new AtomicBoolean();

    private void registerInBackground(IndexShard indexShard, CommitPointer pointer) {
        if (isPrimaryActive(clusterService.state(), indexShard.shardId()) == false) {
            logger.debug("{} fast activation: no active indexing shard, deferring the background registration", indexShard.shardId());
            deferredRegistrations.put(indexShard, pointer);
            if (deferredRegistrationListenerAdded.compareAndSet(false, true)) {
                clusterService.addListener(event -> {
                    if (deferredRegistrations.isEmpty() == false && event.routingTableChanged()) {
                        registerDeferred(event.state());
                    }
                });
            }
            // the primary may have become active between the check above and the put
            registerDeferred(clusterService.state());
            return;
        }
        sendRegistrationInBackground(indexShard, pointer);
    }

    private void registerDeferred(ClusterState state) {
        for (var entry : deferredRegistrations.entrySet()) {
            final IndexShard indexShard = entry.getKey();
            if (indexShard.state() == IndexShardState.CLOSED) {
                deferredRegistrations.remove(indexShard);
            } else if (isPrimaryActive(state, indexShard.shardId()) && deferredRegistrations.remove(indexShard, entry.getValue())) {
                logger.debug("{} fast activation: indexing shard is active, sending the deferred registration", indexShard.shardId());
                // off the cluster applier thread
                clusterService.threadPool().generic().execute(() -> sendRegistrationInBackground(indexShard, entry.getValue()));
            }
        }
    }

    private static boolean isPrimaryActive(ClusterState state, ShardId shardId) {
        final var project = state.metadata().lookupProject(shardId.getIndex());
        if (project.isEmpty()) {
            return false;
        }
        final var indexRoutingTable = state.routingTable(project.get().id()).index(shardId.getIndex());
        return indexRoutingTable != null && indexRoutingTable.shard(shardId.id()).primaryShard().active();
    }

    private void sendRegistrationInBackground(IndexShard indexShard, CommitPointer pointer) {
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
            deferredRegistrations.remove(indexShard);
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
