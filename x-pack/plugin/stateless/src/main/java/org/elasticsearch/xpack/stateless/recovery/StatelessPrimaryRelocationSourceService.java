/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.NoOpEngine;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeaseNotFoundException;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.CompositeRecoverySchedulingListener;
import org.elasticsearch.indices.recovery.RecoveryClusterStateDelay;
import org.elasticsearch.indices.recovery.StatelessPrimaryRelocationAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportClient;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.xpack.stateless.IndexShardCacheWarmer;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.engine.HollowIndexEngine;
import org.elasticsearch.xpack.stateless.engine.HollowShardsMetrics;
import org.elasticsearch.xpack.stateless.engine.IndexEngine;
import org.elasticsearch.xpack.stateless.utils.StatelessCommitServiceProvider;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.elasticsearch.common.Strings.format;
import static org.elasticsearch.xpack.stateless.recovery.TransportStatelessPrimaryRelocationAction.BlobFileWithLength;
import static org.elasticsearch.xpack.stateless.recovery.TransportStatelessPrimaryRelocationAction.ID_LOOKUP_RECENCY_THRESHOLD_SETTING;
import static org.elasticsearch.xpack.stateless.recovery.TransportStatelessPrimaryRelocationAction.SLOW_RELOCATION_THRESHOLD_SETTING;

/// Source-side stateless primary relocation protocol. Mirrors [PeerRecoverySourceService].
/// See [StatelessPrimaryRelocationTargetService] for target side logic.
public class StatelessPrimaryRelocationSourceService {

    private static final Logger logger = LogManager.getLogger(StatelessPrimaryRelocationSourceService.class);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Executor recoveryExecutor;
    private final ThreadContext threadContext;
    private final IndicesService indicesService;
    private final HollowShardsService hollowShardsService;
    private final StatelessCommitServiceProvider statelessCommitServiceProvider;
    private final IndexShardCacheWarmer indexShardCacheWarmer;
    private final HollowShardsMetrics hollowShardsMetrics;
    private final RemoteTransportClient remoteTransportClient;
    private volatile CompositeRecoverySchedulingListener schedulingListeners;

    private volatile TimeValue slowRelocationWarningThreshold;
    private volatile TimeValue idLookupRecencyThreshold;

    public StatelessPrimaryRelocationSourceService(
        ClusterService clusterService,
        ThreadPool threadPool,
        IndicesService indicesService,
        HollowShardsService hollowShardsService,
        StatelessCommitServiceProvider statelessCommitServiceProvider,
        IndexShardCacheWarmer indexShardCacheWarmer,
        HollowShardsMetrics hollowShardsMetrics,
        RemoteTransportClient remoteTransportClient
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.recoveryExecutor = threadPool.generic();
        this.threadContext = threadPool.getThreadContext();
        this.indicesService = indicesService;
        this.hollowShardsService = hollowShardsService;
        this.statelessCommitServiceProvider = statelessCommitServiceProvider;
        this.indexShardCacheWarmer = indexShardCacheWarmer;
        this.hollowShardsMetrics = hollowShardsMetrics;
        this.remoteTransportClient = remoteTransportClient;

        clusterService.getClusterSettings()
            .initializeAndWatch(SLOW_RELOCATION_THRESHOLD_SETTING, value -> this.slowRelocationWarningThreshold = value);
        clusterService.getClusterSettings()
            .initializeAndWatch(ID_LOOKUP_RECENCY_THRESHOLD_SETTING, value -> this.idLookupRecencyThreshold = value);
    }

    /// Registers the shared recovery scheduling listeners (available via Guice when the transport action is constructed).
    public void registerRecoverySchedulingListeners(CompositeRecoverySchedulingListener schedulingListeners) {
        // This service is a Guice singleton constructed before TransportStatelessPrimaryRelocationAction, so this method
        // is called exactly once. The assert is a test-time safety net — assertions are disabled in production JVMs.
        assert this.schedulingListeners == null : "already registered scheduling listeners";
        this.schedulingListeners = schedulingListeners;
    }

    void startRelocation(Task task, StatelessPrimaryRelocationAction.Request request, ActionListener<StartRelocationResponse> listener) {
        initiatePrewarm(task, request);

        RecoveryClusterStateDelay.ensureClusterStateVersion(
            request.clusterStateVersion(),
            clusterService,
            recoveryExecutor,
            threadContext,
            listener.delegateResponse((l, e) -> {
                logger.warn(format("%s recovery [%s]: primary relocation failed", request.shardId(), request.targetAllocationId()), e);
                l.onFailure(e);
            }),
            new Consumer<>() {
                @Override
                public void accept(ActionListener<StartRelocationResponse> l) {
                    startRelocationWithFreshClusterState(task, request, l);
                }

                @Override
                public String toString() {
                    return "recovery [" + request + "]";
                }
            }
        );
    }

    private void initiatePrewarm(Task task, StatelessPrimaryRelocationAction.Request request) {
        try {
            final ShardId shardId = request.shardId();
            final StatelessCommitService statelessCommitService = statelessCommitServiceProvider.get();
            final BatchedCompoundCommit latestBcc = statelessCommitService.getLatestUploadedBcc(shardId);
            if (latestBcc == null) {
                logger.trace("{} no uploaded BCC found, skipping initiate prewarm", shardId);
                return;
            }

            final var indexService = indicesService.indexServiceSafe(shardId.getIndex());
            final IndexShard indexShard = indexService.getShard(shardId.id());
            final var engine = indexShard.getEngineOrNull();
            boolean hasRecentIdLookup = engine != null && engine.hasRecentIdLookup(idLookupRecencyThreshold);

            // If the shard is not about to be hollowed, then send an action to the target node to begin warming the cache immediately.
            // Note that if the shard is already hollow, the target warming will just read a single region.
            if (hollowShardsService.isHollowableIndexShard(indexShard) == false) {
                remoteTransportClient.sendChildRequest(
                    request.targetNode(),
                    TransportStatelessPrimaryRelocationAction.PREWARM_RELOCATION_ACTION_NAME,
                    new TransportStatelessPrimaryRelocationAction.PrewarmRelocationRequest(
                        shardId,
                        new BlobFileWithLength(latestBcc.toBlobFile(), latestBcc.calculateBccBlobLength()),
                        hasRecentIdLookup
                    ),
                    task,
                    TransportRequestOptions.EMPTY,
                    // The response (whether prewarm succeeded or not) does not affect the relocation listener, so we use a noop listener
                    new ActionListenerResponseHandler<>(ActionListener.noop().delegateResponse((l, e) -> {
                        logger.debug(() -> format("%s ignoring prewarm action failure", shardId), e);
                        l.onFailure(e);
                    }), in -> ActionResponse.Empty.INSTANCE, recoveryExecutor)
                );
            }
        } catch (Exception e) {
            logger.trace(format("%s ignoring prewarm message failure", request.shardId()), e);
        }
    }

    private void startRelocationWithFreshClusterState(
        Task task,
        StatelessPrimaryRelocationAction.Request request,
        ActionListener<StartRelocationResponse> listener
    ) {
        logger.debug(
            "[{}]: starting unsearchable primary relocation to [{}] with allocation ID [{}]",
            request.shardId(),
            request.targetNode().descriptionWithoutAttributes(),
            request.targetAllocationId()
        );
        final long beforeRelocation = threadPool.relativeTimeInMillis();

        final IndexShard indexShard;
        final Engine preFlushEngine;
        try {
            final var indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
            indexShard = indexService.getShard(request.shardId().id());
            preFlushEngine = ensureIndexTierAllowedEngine(indexShard.getEngineOrNull(), indexShard.state(), indexShard.routingEntry());
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        indexShard.recoveryStats().sourceRecoveryStarted();
        schedulingListeners.onPeerRecoveryStartedOnSource();

        // Flushing before blocking operations because we expect this to reduce the amount of work done by the flush that happens while
        // operations are blocked. NB the flush has force=false so may do nothing.
        final var preFlushStep = new SubscribableListener<Engine.FlushResult>();

        logShardStats("flushing before acquiring all primary operation permits", indexShard, preFlushEngine);

        final var threadDumpListener = SlowRelocationLogger.slowShardOperationListener(
            indexShard,
            request.targetAllocationId(),
            slowRelocationWarningThreshold,
            "flush and acquire permits",
            indexShard::getActiveOperationsCount
        );

        final long beforeInitialFlush = threadPool.relativeTimeInMillis();
        if (hollowShardsService.isHollowShard(indexShard.shardId())) {
            preFlushStep.onResponse(Engine.FlushResult.FLUSH_REQUEST_PROCESSED_AND_NOT_PERFORMED);
        } else {
            ActionListener.run(preFlushStep, l -> preFlushEngine.flush(false, false, l));
        }
        logger.debug("[{}] completed the flush, waiting to upload", request.shardId());

        final RelocationSourceMetrics.Builder relocationSourceMetricsBuilder = new RelocationSourceMetrics.Builder();
        preFlushStep.addListener(ActionListener.runAfter(listener, () -> {
            indexShard.recoveryStats().sourceRecoveryCompleted();
            schedulingListeners.onPeerRecoveryCompletedOnSource();
        }).delegateFailureAndWrap((listener0, preFlushResult) -> {
            final var initialFlushDuration = getTimeSince(beforeInitialFlush);
            final long beforeAcquiringPermits = threadPool.relativeTimeInMillis();
            indexShard.relocated(request.targetNode().getId(), request.targetAllocationId(), (primaryContext, handoffResultListener) -> {
                threadDumpListener.onResponse(null);
                Engine engine = ensureIndexTierAllowedEngine(indexShard.getEngineOrNull(), indexShard.state(), indexShard.routingEntry());
                logShardStats("obtained primary context", indexShard, engine);
                logger.debug("[{}] obtained primary context: [{}]", request.shardId(), primaryContext);
                final var acquirePermitsDuration = getTimeSince(beforeAcquiringPermits);

                // Do not wait on flush durability as we will wait at the stateless commit service level for the upload
                final long beforeFinalFlush = threadPool.relativeTimeInMillis();

                final var shardId = indexShard.shardId();
                final boolean hasRecentIdLookup = engine.hasRecentIdLookup(idLookupRecencyThreshold);
                if (engine instanceof IndexEngine indexEngine) {
                    if (hollowShardsService.isHollowableIndexShard(indexShard, false)) {
                        // Resetting the IndexEngine hollows the shard and switches to a HollowIndexEngine and blocks ingestion
                        // The block will be removed when the source shard is successfully relocated and closed,
                        // or will remain in place if the relocation fails until the shard is unhollowed.
                        logger.debug(() -> "hollowing index engine for shard " + shardId);

                        final var idxVersion = indexShard.indexSettings().getIndexVersionCreated();
                        if (idxVersion.before(IndexVersions.READ_SI_FILES_FROM_MEMORY_FOR_HOLLOW_COMMITS)) {
                            // The HollowIndexEngine potentially needs to read referenced .si files.
                            // On or after the READ_SI_FILES_FROM_MEMORY_FOR_HOLLOW_COMMITS index version those files wil be read from
                            // memory but before that version, we should prewarm to brings them in the cache in parallel, rather than
                            // letting the engine fetch them on-demand sequentially.
                            indexShardCacheWarmer.preWarmIndexShardCache(indexShard, SharedBlobCacheWarmingService.Type.HOLLOWING);
                        }

                        long startTime = threadPool.relativeTimeInMillisSupplier().getAsLong();
                        try {
                            indexShard.resetEngine(newEngine -> {
                                assert newEngine instanceof HollowIndexEngine : shardId + " has non-hollow engine " + newEngine;
                                assert newEngine.getEngineConfig().getEngineResetLock().isWriteLockedByCurrentThread() : shardId;
                                newEngine.refresh("hollowing"); // warms up reader managers
                            });
                        } catch (Exception e) {
                            indexShard.failShard("failed to reset index engine for shard " + shardId, e);
                            throw e;
                        }
                        hollowShardsService.addHollowShard(indexShard, "hollowing");
                        hollowShardsMetrics.hollowSuccessCounter().increment();
                        hollowShardsMetrics.hollowTimeMs().record(threadPool.relativeTimeInMillisSupplier().getAsLong() - startTime);
                        engine = indexShard.getEngineOrNull();
                        assert engine == null || engine instanceof HollowIndexEngine : engine;
                    } else {
                        indexEngine.flush(false, true, ActionListener.noop());
                    }
                } else if (engine instanceof HollowIndexEngine) {
                    hollowShardsService.ensureHollowShard(
                        indexShard.shardId(),
                        true,
                        "hollow shard " + shardId + " should have an ingestion blocker"
                    );
                }
                logShardStats("flush after acquiring primary context completed", indexShard, engine);
                final boolean relocatedAsHollow = engine instanceof HollowIndexEngine;
                long lastFlushedGeneration = engine.getLastCommittedSegmentInfos().getGeneration();

                final var localCheckpoints = new HashMap<>(primaryContext.getCheckpointStates());
                final var sourceCheckpoints = localCheckpoints.get(indexShard.routingEntry().allocationId().getId());
                localCheckpoints.put(request.targetAllocationId(), sourceCheckpoints);

                final var targetNodeId = request.targetNode().getId();
                try {
                    indexShard.removePeerRecoveryRetentionLease(targetNodeId, ActionListener.noop());
                } catch (RetentionLeaseNotFoundException e) {
                    // ok, we don't know it exists here
                }
                indexShard.cloneLocalPeerRecoveryRetentionLease(targetNodeId, ActionListener.noop());

                final var retentionLeases = indexShard.getRetentionLeases();
                final var leaseId = ReplicationTracker.getPeerRecoveryRetentionLeaseId(targetNodeId);
                if (retentionLeases.contains(leaseId) == false) {
                    // This is practically impossible, we only just created this lease, but in theory it could happen since leases have
                    // time-based expiry.
                    throw new RetentionLeaseNotFoundException(leaseId);
                }

                final var beforeSendingContext = new AtomicLong();
                final var latestBccBlobLength = new AtomicLong(-1L);
                final var otherBlobFilesCount = new AtomicLong(-1L);
                final var markedShardAsRelocating = new SubscribableListener<Void>();
                final StatelessCommitService statelessCommitService = statelessCommitServiceProvider.get();
                ActionListener<Void> handoffCompleteListener = statelessCommitService.markRelocating(
                    indexShard.shardId(),
                    lastFlushedGeneration,
                    markedShardAsRelocating
                );

                // Create a compound listener which will trigger both the stateless commit service listener and top-level
                // handoffResultListener
                ActionListener<ActionResponse.Empty> compoundHandoffListener = new ActionListener<>() {
                    @Override
                    public void onResponse(ActionResponse.Empty unused) {
                        final var relocationDuration = getTimeSince(beforeRelocation);

                        logger.debug("[{}] primary context handoff succeeded", request.shardId());
                        final TimeValue secondFlushDuration = getTimeBetween(beforeFinalFlush, beforeSendingContext.get());
                        final TimeValue handOffDuration = getTimeSince(beforeSendingContext.get());
                        relocationSourceMetricsBuilder.recordInitialFlushDuration(initialFlushDuration.millis());
                        relocationSourceMetricsBuilder.recordAcquirePermitsDuration(acquirePermitsDuration.millis());
                        relocationSourceMetricsBuilder.recordSecondFlushDuration(secondFlushDuration.millis());
                        relocationSourceMetricsBuilder.recordHandoffDuration(handOffDuration.millis());

                        boolean aboveThreshold = relocationDuration.getMillis() >= slowRelocationWarningThreshold.getMillis();
                        if (aboveThreshold || logger.isDebugEnabled()) {
                            final var indexingStats = indexShard.indexingStats().getTotal();
                            final boolean shuttingDown = isShuttingDown();
                            final var fields = new HashMap<String, Object>();
                            fields.put("elasticsearch.primary.relocation.shard", request.shardId().toString());
                            fields.put("elasticsearch.primary.relocation.target_allocation_id", request.targetAllocationId());
                            fields.put("elasticsearch.primary.relocation.source_node", clusterService.localNode().getName());
                            fields.put("elasticsearch.primary.relocation.target_node", request.targetNode().getName());
                            fields.put("elasticsearch.primary.relocation.shutting_down", shuttingDown);
                            fields.put("elasticsearch.primary.relocation.hollow", relocatedAsHollow);
                            fields.put("elasticsearch.primary.relocation.duration", relocationDuration.millis());
                            fields.put("elasticsearch.primary.relocation.initial_flush_duration", initialFlushDuration.millis());
                            fields.put("elasticsearch.primary.relocation.acquire_permits_duration", acquirePermitsDuration.millis());
                            fields.put("elasticsearch.primary.relocation.second_flush_duration", secondFlushDuration.millis());
                            fields.put("elasticsearch.primary.relocation.handoff_duration", handOffDuration.millis());
                            fields.put("elasticsearch.primary.relocation.has_recent_id_lookup", hasRecentIdLookup);
                            fields.put("elasticsearch.primary.write_load", indexingStats.getWriteLoad());
                            fields.put("elasticsearch.primary.recent_write_load", indexingStats.getRecentWriteLoad());
                            fields.put("elasticsearch.primary.peak_write_load", indexingStats.getPeakWriteLoad());
                            if (latestBccBlobLength.get() >= 0) {
                                fields.put("elasticsearch.primary.relocation.bcc_blob_length_in_bytes", latestBccBlobLength.get());
                                fields.put("elasticsearch.primary.relocation.other_blobs_count", otherBlobFilesCount.get());
                            }
                            final var message = new ESLogMessage(
                                "[{}] primary shard relocation took [{}] (shutting down={}, has recent id lookup={}) "
                                    + "(including [{}] to flush, [{}] to acquire permits, [{}] to flush again and [{}] to handoff context) "
                                    + "which is {} the warn threshold of [{}]",
                                request.shardId(),
                                relocationDuration,
                                shuttingDown,
                                hasRecentIdLookup,
                                initialFlushDuration,
                                acquirePermitsDuration,
                                secondFlushDuration,
                                handOffDuration,
                                aboveThreshold ? "above" : "below",
                                slowRelocationWarningThreshold
                            ).withFields(fields);
                            logger.log(Level.INFO, message);
                        }

                        try {
                            handoffCompleteListener.onResponse(null);
                        } finally {
                            handoffResultListener.onResponse(null);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        try {
                            handoffCompleteListener.onFailure(e);
                        } finally {
                            handoffResultListener.onFailure(e);
                        }
                    }
                };

                markedShardAsRelocating.addListener(compoundHandoffListener.delegateFailureAndWrap((finalHandoffListener, v) -> {
                    logger.debug("[{}] flush complete, handing off primary context", request.shardId());
                    beforeSendingContext.set(threadPool.relativeTimeInMillis());

                    assert assertLastCommitSequenceNumberConsistency(indexShard, sourceCheckpoints, false);

                    // We send info about the latest BCC and blobs, so target node can avoid LISTing the object store.
                    final BatchedCompoundCommit latestBcc = statelessCommitService.getLatestUploadedBcc(shardId);
                    assert latestBcc != null : "no uploaded BCC for shard " + shardId;
                    final long blobLength = latestBcc.calculateBccBlobLength();
                    final BlobFile latestBccBlob = latestBcc.toBlobFile();
                    final var lastCommitBlobs = latestBcc.lastCompoundCommit().getBlobFiles();
                    final var lastCommitIsHollow = latestBcc.lastCompoundCommit().hollow();
                    // This happens after markRelocating() has triggered the listener. The latest uploaded BCC will be the last. No new
                    // BCCs will be uploaded after that. However, there could still be VBCCs after the last BCC that we need to ignore.
                    // Thus, we pass the generation of the last BCC.
                    final Set<BlobFile> otherBlobFiles = statelessCommitService.getTrackedUploadedBlobFilesUpTo(
                        shardId,
                        latestBcc.primaryTermAndGeneration().generation()
                    );
                    otherBlobFiles.remove(latestBccBlob);
                    latestBccBlobLength.set(blobLength);
                    otherBlobFilesCount.set(otherBlobFiles.size());

                    remoteTransportClient.sendChildRequest(
                        request.targetNode(),
                        TransportStatelessPrimaryRelocationAction.PRIMARY_CONTEXT_HANDOFF_ACTION_NAME,
                        new TransportStatelessPrimaryRelocationAction.PrimaryContextHandoffRequest(
                            request.recoveryId(),
                            request.shardId(),
                            new ReplicationTracker.PrimaryContext(
                                primaryContext.clusterStateVersion(),
                                localCheckpoints,
                                primaryContext.getRoutingTable()
                            ),
                            retentionLeases,
                            statelessCommitService.getSearchNodesPerCommit(indexShard.shardId()),
                            new BlobFileWithLength(latestBccBlob, blobLength),
                            otherBlobFiles,
                            hasRecentIdLookup,
                            lastCommitBlobs,
                            lastCommitIsHollow
                        ),
                        task,
                        TransportRequestOptions.EMPTY,
                        new ActionListenerResponseHandler<>(finalHandoffListener, in -> ActionResponse.Empty.INSTANCE, recoveryExecutor)
                    );
                }), recoveryExecutor, threadContext);
            }, listener0.map(unused -> new StartRelocationResponse(relocationSourceMetricsBuilder.build())));
        }), recoveryExecutor, threadContext);
    }

    private void logShardStats(String message, IndexShard indexShard, Engine engine) {
        if (logger.isDebugEnabled()) {
            logger.debug(
                "{}: {}. Flush stats [{}], Translog stats [{}], Merge stats [{}], Commit stats [{}], Segments {}",
                indexShard.shardId(),
                message,
                Strings.toString(indexShard.flushStats()),
                Strings.toString(indexShard.translogStats()),
                Strings.toString(indexShard.mergeStats()),
                Strings.toString(engine.commitStats()),
                engine instanceof HollowIndexEngine ? "(empty due to hollow engine)" : engine.segments()
            );
        }
    }

    private Engine ensureIndexTierAllowedEngine(Engine engine, IndexShardState indexShardState, ShardRouting shardRouting) {
        if (engine instanceof IndexEngine || engine instanceof HollowIndexEngine || engine instanceof NoOpEngine) {
            return engine;
        } else if (engine == null) {
            throw new AlreadyClosedException("source shard closed before recovery started: " + shardRouting);
        } else {
            final var message = format(
                "not an allowed engine on indexing tier: %s [indexShardState=%s, shardRouting=%s]",
                engine,
                indexShardState,
                shardRouting
            );
            assert false : message;
            throw new IllegalStateException(message);
        }
    }

    static boolean assertLastCommitSequenceNumberConsistency(
        IndexShard indexShard,
        ReplicationTracker.CheckpointState sourceCheckpoints,
        boolean flushFirst
    ) {
        // cannot use persisted seqnos for validation when durability is async, since then the durability happens outside the
        // operation permit.
        if (indexShard.indexSettings().getTranslogDurability() == Translog.Durability.REQUEST && Randomness.get().nextBoolean()) {
            // don't acquire a commit every time, lest it disturb something else
            final var engine = indexShard.getEngineOrNull();
            if (engine == null) {
                assert indexShard.state() == IndexShardState.CLOSED : indexShard.shardId() + " engine null but index not closed";
            } else {
                try (var commitRef = engine.acquireLastIndexCommit(flushFirst)) {
                    final var indexCommit = commitRef.getIndexCommit();
                    final var userData = indexCommit.getUserData();
                    final var localCheckpoint = Long.toString(sourceCheckpoints.getLocalCheckpoint());
                    assert localCheckpoint.equals(userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY))
                        && localCheckpoint.equals(userData.get(SequenceNumbers.MAX_SEQ_NO))
                        : indexShard.shardId() + ": " + sourceCheckpoints + " vs " + userData;
                } catch (IOException e) {
                    throw new AssertionError("unexpected", e);
                } catch (IllegalStateException e) {
                    assert indexShard.state() == IndexShardState.CLOSED : e;
                }
            }
        }
        return true;
    }

    private TimeValue getTimeSince(long startTimeMillis) {
        return getTimeBetween(startTimeMillis, threadPool.relativeTimeInMillis());
    }

    private TimeValue getTimeBetween(long start, long finish) {
        return TimeValue.timeValueMillis(Math.max(0, finish - start));
    }

    private boolean isShuttingDown() {
        return clusterService.state()
            .metadata()
            .nodeShutdowns()
            .contains(clusterService.localNode().getId(), SingleNodeShutdownMetadata.Type.SIGTERM);
    }
}
