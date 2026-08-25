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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShardNotRecoveringException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.IndexShardCacheWarmer;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory;
import org.elasticsearch.xpack.stateless.recovery.metering.StatelessPrimaryRelocationMetricsCollector;

import java.util.HashMap;
import java.util.Set;

import static org.elasticsearch.common.Strings.format;
import static org.elasticsearch.xpack.stateless.recovery.TransportStatelessPrimaryRelocationAction.PrewarmRelocationRequest;
import static org.elasticsearch.xpack.stateless.recovery.TransportStatelessPrimaryRelocationAction.PrimaryContextHandoffRequest;
import static org.elasticsearch.xpack.stateless.recovery.TransportStatelessPrimaryRelocationAction.SLOW_RELOCATION_THRESHOLD_SETTING;

/// Target-side stateless primary relocation: prewarm and primary-context handoff.
public class StatelessPrimaryRelocationTargetService {

    private static final Logger logger = LogManager.getLogger(StatelessPrimaryRelocationTargetService.class);

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final StatelessCommitService statelessCommitService;
    private final IndexShardCacheWarmer indexShardCacheWarmer;
    private final StatelessPrimaryRelocationMetricsCollector relocationMetricsCollector;
    private final ThreadPool threadPool;

    private volatile TimeValue slowRelocationWarningThreshold;

    public StatelessPrimaryRelocationTargetService(
        ClusterService clusterService,
        ThreadPool threadPool,
        IndicesService indicesService,
        StatelessCommitService statelessCommitService,
        IndexShardCacheWarmer indexShardCacheWarmer,
        StatelessPrimaryRelocationMetricsCollector relocationMetricsCollector
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.indicesService = indicesService;
        this.statelessCommitService = statelessCommitService;
        this.indexShardCacheWarmer = indexShardCacheWarmer;
        this.relocationMetricsCollector = relocationMetricsCollector;

        clusterService.getClusterSettings()
            .initializeAndWatch(SLOW_RELOCATION_THRESHOLD_SETTING, value -> this.slowRelocationWarningThreshold = value);
    }

    void handlePrewarmRelocation(PrewarmRelocationRequest request, ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            logger.trace("{} prewarming due to primary relocation", request.shardId());

            final var indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
            final var indexShard = indexService.getShard(request.shardId().id());
            final var latestBccBlob = request.latestBccBlob();
            // We don't need otherBlobs for prewarming
            final var sourceBlobsInfo = new StatelessCommitService.SourceBlobsInfo(
                latestBccBlob.blobFile(),
                latestBccBlob.length(),
                Set.of()
            );
            try {
                indexShardCacheWarmer.preWarmIndexShardCacheForPeerRecovery(indexShard, sourceBlobsInfo, request.hasRecentIdLookup());
            } catch (IndexShardNotRecoveringException e) {
                // This could happen if the prewarm request is delayed. The caller decides whether to ignore this failure.
                logger.trace(format("%s not prewarming as shard is not recovering", request.shardId()), e);
                throw e;
            }
            return null;
        });
    }

    void handlePrimaryContextHandoff(PrimaryContextHandoffRequest request, ActionListener<Void> listener) {
        logger.debug("[{}] received primary context handoff request", request.shardId());
        final var indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        final var indexShard = indexService.getShard(request.shardId().id());
        statelessCommitService.setTrackedSearchNodesPerCommitOnRelocationTarget(request.shardId(), request.searchNodesPerCommit());

        final var targetAllocationId = indexShard.routingEntry().allocationId().getId();
        final var threadDumpListener = SlowRelocationLogger.slowShardOperationListener(
            indexShard,
            targetAllocationId,
            slowRelocationWarningThreshold,
            "starting",
            null
        );

        final Releasable cleanUpStatelessCommitService = () -> {
            try {
                statelessCommitService.setTrackedSearchNodesPerCommitOnRelocationTarget(request.shardId(), null);
                statelessCommitService.clearRecoveryInfoFromSourceEntry(request.shardId());
            } catch (AlreadyClosedException ignored) {
                // engine is closed
            }
        };

        final var recoveryHintsFromSource = request.recoveryInfoFromSource();
        if (recoveryHintsFromSource != null) {
            statelessCommitService.putRecoveryInfoFromSourceEntry(request.shardId(), recoveryHintsFromSource);
        }

        final var blobCacheDirectory = BlobStoreCacheDirectory.unwrapDirectory(indexShard.store().directory());
        final long bytesReadBeforeHandoff = blobCacheDirectory.totalBytesReadFromObjectStore();
        final long bytesWarmedBeforeHandoff = blobCacheDirectory.totalBytesWarmedFromObjectStore();
        final long preRecoveryStartMillis = threadPool.relativeTimeInMillis();
        ActionListener.run(
            ActionListener.releaseAfter(listener, cleanUpStatelessCommitService),
            l -> indexShard.preRecovery(l.map(ignored -> {
                final long preRecoveryEndMillis = threadPool.relativeTimeInMillis();
                final long preRecoveryDuration = preRecoveryEndMillis - preRecoveryStartMillis;
                relocationMetricsCollector.recordRelocationTargetPreRecoveryDuration(preRecoveryDuration);

                indexShard.updateRetentionLeasesOnReplica(request.retentionLeases());
                final var recoveryState = indexShard.recoveryState();
                recoveryState.setStage(RecoveryState.Stage.VERIFY_INDEX);
                recoveryState.setStage(RecoveryState.Stage.TRANSLOG);
                indexShard.openEngineAndSkipTranslogRecovery();

                // Should not actually have recovered anything from the translog, so the MSN and LCP should remain equal and unchanged
                // from the ones we received in the primary context handoff.
                assert StatelessPrimaryRelocationSourceService.assertLastCommitSequenceNumberConsistency(
                    indexShard,
                    request.primaryContext().getCheckpointStates().get(indexShard.routingEntry().allocationId().getId()),
                    true
                );

                recoveryState.getIndex().setFileDetailsComplete();
                recoveryState.setStage(RecoveryState.Stage.FINALIZE);
                indexShard.activateWithPrimaryContext(request.primaryContext());

                final long openEngineDuration = threadPool.relativeTimeInMillis() - preRecoveryEndMillis;
                relocationMetricsCollector.recordRelocationTargetOpenEngineDuration(openEngineDuration);

                threadDumpListener.onResponse(null);

                final long targetHandoffDuration = preRecoveryDuration + openEngineDuration;
                boolean aboveThreshold = targetHandoffDuration >= slowRelocationWarningThreshold.getMillis();
                if (aboveThreshold || logger.isDebugEnabled()) {
                    final var fields = new HashMap<String, Object>();
                    fields.put("elasticsearch.primary.relocation.shard", request.shardId().toString());
                    fields.put("elasticsearch.primary.relocation.target_allocation_id", targetAllocationId);
                    fields.put("elasticsearch.primary.relocation.source_node", recoveryState.getSourceNode().getName());
                    fields.put("elasticsearch.primary.relocation.target_node", clusterService.localNode().getName());
                    fields.put("elasticsearch.primary.relocation.target_handoff_duration", targetHandoffDuration);
                    fields.put("elasticsearch.primary.relocation.target_pre_recovery_duration", preRecoveryDuration);
                    fields.put("elasticsearch.primary.relocation.target_open_engine_duration", openEngineDuration);
                    fields.put(
                        "elasticsearch.primary.relocation.target_object_store_bytes_read",
                        blobCacheDirectory.totalBytesReadFromObjectStore() - bytesReadBeforeHandoff
                    );
                    fields.put(
                        "elasticsearch.primary.relocation.target_object_store_bytes_warmed",
                        blobCacheDirectory.totalBytesWarmedFromObjectStore() - bytesWarmedBeforeHandoff
                    );
                    final var message = new ESLogMessage(
                        "[{}] recovery [{}]: primary context handoff on target took [{}] "
                            + "(including [{}] in pre-recovery and [{}] opening the engine) "
                            + "which is {} the warn threshold of [{}]",
                        request.shardId(),
                        targetAllocationId,
                        TimeValue.timeValueMillis(targetHandoffDuration),
                        TimeValue.timeValueMillis(preRecoveryDuration),
                        TimeValue.timeValueMillis(openEngineDuration),
                        aboveThreshold ? "above" : "below",
                        slowRelocationWarningThreshold
                    ).withFields(fields);
                    logger.log(Level.INFO, message);
                }
                return null;
            }))
        );
    }
}
