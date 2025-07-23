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

package co.elastic.elasticsearch.stateless.reshard;

import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;

import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

public class SplitSourceService {
    private static final Logger logger = LogManager.getLogger(SplitSourceService.class);

    private final Client client;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final StatelessCommitService commitService;
    private final ObjectStoreService objectStoreService;
    private final ReshardIndexService reshardIndexService;

    private final ConcurrentHashMap<IndexShard, Split> onGoingSplits = new ConcurrentHashMap<>();

    // ES-12460 for testing purposes, until pre-handoff logic (flush etc) is built out
    @Nullable
    private Runnable preHandoffHook;

    public SplitSourceService(
        Client client,
        ClusterService clusterService,
        IndicesService indicesService,
        StatelessCommitService commitService,
        ObjectStoreService objectStoreService,
        ReshardIndexService reshardIndexService
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.commitService = commitService;
        this.objectStoreService = objectStoreService;
        this.reshardIndexService = reshardIndexService;
    }

    /**
     * Start populating a target shard that has been newly created by a resharding operation
     * <p>
     * A newly created shard starts in CLONE state and has no backing lucene index. This operation
     * sets up a lucene index on the target shard by copying the source shard's lucene index to the target
     * directory on the blob store, and arranging for any new commits to also be copied to the target until the
     * target shard enters handoff state.
     * <p>
     * This function also registers a cluster state observer to trigger cleaning up the local shard when all
     * targets have advanced to split, if one isn't already registered.
     * @param targetShardId the id of the target shard to populate
     * @param sourcePrimaryTerm the primary term of the source shard seen by the target when it requested population
     * @param targetPrimaryTerm the primary term of the target shard when it requested population
     *     These are used to ensure that the copy operation hasn't become stale due to advances on either side.
     * @param listener called when the target shard has been set up
     */
    public void setupTargetShard(ShardId targetShardId, long sourcePrimaryTerm, long targetPrimaryTerm, ActionListener<Void> listener) {
        Index index = targetShardId.getIndex();

        var indexMetadata = clusterService.state().metadata().projectFor(index).getIndexSafe(index);
        var reshardingMetadata = indexMetadata.getReshardingMetadata();
        assert reshardingMetadata != null && reshardingMetadata.isSplit() : "Unexpected resharding state";
        int sourceShardIndex = reshardingMetadata.getSplit().sourceShard(targetShardId.getId());
        var sourceShardId = new ShardId(index, sourceShardIndex);

        if (reshardingMetadata.getSplit().getSourceShardState(sourceShardIndex) != IndexReshardingState.Split.SourceShardState.SOURCE) {
            String message = String.format(
                Locale.ROOT,
                "Split [%s -> %s]. Source shard state should be SOURCE but it is [%s]. Failing the request.",
                sourceShardId,
                targetShardId,
                reshardingMetadata.getSplit().getSourceShardState(sourceShardIndex)
            );
            logger.error(message);

            throw new IllegalStateException(message);
        }

        if (reshardingMetadata.getSplit().getTargetShardState(targetShardId.getId()) != IndexReshardingState.Split.TargetShardState.CLONE) {
            String message = String.format(
                Locale.ROOT,
                "Split [%s -> %s]. Target shard state should be CLONE but it is [%s]. Failing the request.",
                sourceShardId,
                targetShardId,
                reshardingMetadata.getSplit().getTargetShardState(targetShardId.getId())
            );
            logger.error(message);

            throw new IllegalStateException(message);
        }

        long currentSourcePrimaryTerm = indexMetadata.primaryTerm(sourceShardId.getId());
        long currentTargetPrimaryTerm = indexMetadata.primaryTerm(targetShardId.getId());

        if (currentTargetPrimaryTerm > targetPrimaryTerm) {
            // This request is stale, we will handle it when target recovers with new primary term. Fail the target recovery process.
            String message = String.format(
                Locale.ROOT,
                "Split [%s -> %s]. Target primary term advanced [%s -> %s] before start split request was handled. Failing the request.",
                sourceShardId,
                targetShardId,
                targetPrimaryTerm,
                currentTargetPrimaryTerm
            );
            logger.info(message);

            throw new IllegalStateException(message);
        }
        if (currentSourcePrimaryTerm > sourcePrimaryTerm) {
            // We need to keep the invariant that target primary term is >= source primary term.
            // So if source primary term advanced we need to fail target recovery so that it picks up new primary term.
            String message = String.format(
                Locale.ROOT,
                "Split [%s -> %s]. Source primary term advanced [%s -> %s] before start split request was handled. Failing the request.",
                sourceShardId,
                targetShardId,
                sourcePrimaryTerm,
                currentSourcePrimaryTerm
            );
            logger.info(message);

            throw new IllegalStateException(message);
        }

        var sourceShard = indicesService.indexServiceSafe(index).getShard(sourceShardIndex);
        // Defensive check so that some other process (like recovery) won't interfere with resharding.
        if (sourceShard.state() != IndexShardState.STARTED) {
            String message = String.format(
                Locale.ROOT,
                "Split [%s -> %s]. Source shard is not started when processing start split request. Failing the request.",
                sourceShardId,
                targetShardId
            );
            logger.error(message);

            throw new IllegalStateException(message);
        }

        if (onGoingSplits.putIfAbsent(sourceShard, new Split()) == null) {
            // This is the first time a target shard contacted this source shard to start a split.
            // We'll start tracking this split now to be able to eventually properly finish it.
            // If we have already seen this split before, we are all set already.
            setupSplitProgressTracking(sourceShard);
        }

        // register new commits to be copied prior to copying existing data to avoid gaps.
        commitService.markSplitting(sourceShardId, targetShardId);

        ActionListener.run(listener, l -> {
            objectStoreService.copyShard(sourceShardId, targetShardId, sourcePrimaryTerm);
            l.onResponse(null);
        });
    }

    public void setPreHandoffHook(Runnable preHandoffHook) {
        assert this.preHandoffHook == null;
        this.preHandoffHook = preHandoffHook;
    }

    /**
     * Prepare to complete handoff to target shard
     * Blocks indexing and flushes the source to synchronize the target with the latest changes,
     * and turns off commit copying after the flush.
     * @param listener a listener to be called when preparation has completed, to do the handoff or abort
     * @param targetShardId the shardId of the target of the split operation that is ready for handoff.
     */
    public void prepareForHandoff(ActionListener<Void> listener, ShardId targetShardId) {
        // testing hook, to be removed
        if (preHandoffHook != null) {
            preHandoffHook.run();
        }

        Index index = targetShardId.getIndex();
        var indexService = indicesService.indexServiceSafe(index);
        ShardId splitSource = getSplitSource(targetShardId);
        var indexShard = indexService.getShard(splitSource.id());

        indexShard.ensureMutable(listener.delegateFailureAndWrap((l, ignored) -> indexShard.withEngine(engine -> {
            ActionListener.run(l, runListener -> {
                // XXX acquire permits here (ref counted to handle multiple targets with concurrent handoff in flight)
                // With permits acquired, flush whatever may be outstanding
                engine.flush(/* force */ true, /* waitIfOngoing */ true, runListener.map(r -> null));
                // and stop copying any new commits.
                stopCopyingNewCommits(targetShardId);
            });
            return null;
        })), false);
        listener.onResponse(null);
    }

    public void stopCopyingNewCommits(ShardId targetShardId) {
        commitService.markSplitCompleting(getSplitSource(targetShardId), targetShardId);
    }

    private ShardId getSplitSource(ShardId targetShardId) {
        Index index = targetShardId.getIndex();

        var indexMetadata = clusterService.state().metadata().projectFor(index).getIndexSafe(index);
        var reshardingMetadata = indexMetadata.getReshardingMetadata();
        assert reshardingMetadata != null && reshardingMetadata.isSplit() : "Unexpected resharding state";
        int sourceShardIndex = reshardingMetadata.getSplit().sourceShard(targetShardId.getId());

        return new ShardId(index, sourceShardIndex);
    }

    public void afterSplitSourceIndexShardRecovery(
        IndexShard indexShard,
        IndexReshardingMetadata reshardingMetadata,
        ActionListener<Void> listener
    ) {

        IndexReshardingState.Split split = reshardingMetadata.getSplit();

        if (split.getSourceShardState(indexShard.shardId().getId()) == IndexReshardingState.Split.SourceShardState.DONE) {
            // Nothing to do.
            // TODO eventually we should initiate deletion of resharding metadata here if all source shards are DONE.
            // This is in case master dropped the cluster state observer that does that.
            listener.onResponse(null);
            return;
        }

        // It is possible that the shard is already STARTED at this point, see IndicesClusterStateService#updateShard.
        // As such it is possible that we are already accepting requests to start split from targets.
        // If any of them already set up tracking of the split process we don't need to do anything here.
        if (onGoingSplits.putIfAbsent(indexShard, new Split()) != null) {
            listener.onResponse(null);
            return;
        }

        if (split.targetsDone(indexShard.shardId().getId())) {
            // TODO this is not really correct.
            // This will block source shard recovery until we delete unowned documents and transition to DONE.
            // It is not necessary and this shard is serving indexing traffic so this delay
            // hurts.
            // We should not block on this but also we need to make sure this eventually happens somehow.
            // For simplicity it is done inline now.
            moveToDone(indexShard, listener);
            return;
        }

        setupSplitProgressTracking(indexShard);
        listener.onResponse(null);
    }

    public void setupSplitProgressTracking(IndexShard indexShard) {
        // Wait for all target shards to be DONE and once that is true execute source shard logic to move to DONE:
        // 1. Delete unowned documents
        // 2. Move to DONE
        ClusterStateObserver observer = new ClusterStateObserver(
            clusterService,
            null,
            logger,
            clusterService.threadPool().getThreadContext()
        );

        var allTargetsAreDonePredicate = new Predicate<ClusterState>() {
            @Override
            public boolean test(ClusterState state) {
                IndexReshardingState.Split split = getSplit(state, indexShard.shardId().getIndex());
                if (split == null) {
                    // This is possible if the source shard failed right after setting up this listener,
                    // recovered and successfully completed the split.
                    // TODO we can even we see a completely different split here
                    // TODO is project deletion possible here?
                    return true;
                }

                if (onGoingSplits.containsKey(indexShard) == false) {
                    // Shard was closed in the meantime.
                    // It will pick this work up on recovery.
                    return true;
                }

                return split.targetsDone(indexShard.shardId().getId());
            }
        };

        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                IndexReshardingState.Split split = getSplit(state, indexShard.shardId().getIndex());
                if (split == null) {
                    return;
                }

                if (onGoingSplits.containsKey(indexShard) == false) {
                    return;
                }

                // It is possible that shard gets closed right after this line.
                // It is not really a problem since delete of unowned documents is idempotent.
                // TODO track the result of this operation, fail shard if it fails?
                clusterService.threadPool().generic().submit(() -> moveToDone(indexShard, new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        logger.info(Strings.format("Split source shard %s successfully transitioned to DONE", indexShard.shardId()));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(Strings.format("Error while tracking split progress in source shard %s: %s", indexShard.shardId(), e));
                    }
                }));
            }

            @Override
            public void onClusterServiceClose() {
                // nothing to do
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                assert false;
            }
        }, allTargetsAreDonePredicate);
    }

    public void cancelSplits(IndexShard indexShard) {
        onGoingSplits.remove(indexShard);
    }

    private void moveToDone(IndexShard indexShard, ActionListener<Void> listener) {
        // Note that a shard can be closed (due to a failure) at any moment during the below flow.
        // It is not a problem since all operations are idempotent.
        SubscribableListener.<Void>newForked(l -> reshardIndexService.deleteUnownedDocuments(indexShard.shardId(), l))
            .<Void>andThen(
                l -> client.execute(
                    TransportUpdateSplitSourceStateAction.TYPE,
                    new TransportUpdateSplitSourceStateAction.Request(
                        indexShard.shardId(),
                        IndexReshardingState.Split.SourceShardState.DONE
                    ),
                    l.map(r -> null)
                )

            )
            .andThenAccept(ignored -> onGoingSplits.remove(indexShard))
            .addListener(listener);
    }

    private static IndexReshardingState.Split getSplit(ClusterState state, Index index) {
        return state.metadata()
            .lookupProject(index)
            .flatMap(p -> Optional.ofNullable(p.index(index)))
            .flatMap(m -> Optional.ofNullable(m.getReshardingMetadata()))
            .map(IndexReshardingMetadata::getSplit)
            .orElse(null);
    }

    // Currently empty, a placeholder for any metadata needed by the source shard to track the split process.
    private record Split() {}
}
