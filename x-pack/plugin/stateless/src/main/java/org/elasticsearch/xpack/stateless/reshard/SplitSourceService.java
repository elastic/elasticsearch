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

package org.elasticsearch.xpack.stateless.reshard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.state.AwaitClusterStateVersionAppliedRequest;
import org.elasticsearch.action.admin.cluster.state.TransportAwaitClusterStateVersionAppliedAction;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardNotStartedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import static org.elasticsearch.core.Strings.format;

public class SplitSourceService {
    private static final Logger logger = LogManager.getLogger(SplitSourceService.class);

    /// This is a grace period to drain queued requests before deleting unowned data and completing the split.
    /// It is needed for both search and indexing.
    ///
    /// In search logic we rely on refresh blocks to ensure consistency of search results. Since a refresh can not
    /// be performed while target search shards are being set up, it is correct to return pre-split results
    /// using only the source shard. In other words we don't "resplit" searches. So any stale requests
    /// that f.e. got queued on the coordinators or in the network buffers somewhere will be served
    /// by the source shard only, and it's not a problem from correctness perspective.
    /// This means however that we can't delete unowned data on the source shard as soon as possible because in that case
    /// stale requests described above will see a big chunk of the documents being missing - the now-unowned documents.
    /// We can allow search results for such requests to be stale, but we can't allow such a major discrepancy.
    /// Once we know that unowned documents are about to be deleted after the grace period we will reject such stale search requests.
    ///
    /// In indexing logic we currently read resharding metadata in order to figure out the corresponding target shard
    /// for the source shard when we forward broadcast requests like flush.
    /// We would lose that ability if we were to immediately delete unowned data and complete the split
    /// (thus removing resharding metadata).
    /// Note that using the resharding metadata is not strictly necessary to do that (you can work it out using shard count summary)
    /// so this may change.
    public static final Setting<TimeValue> RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD = Setting.positiveTimeSetting(
        "reshard.split.delete_unowned_grace_period",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope
    );

    /// We want to prevent the state machine from spinning in a hot loop.
    /// This setting defines how long to wait between the retries.
    /// This is not an actual registered setting and is only used in tests.
    public static final Setting<TimeValue> STATE_MACHINE_RETRY_DELAY = Setting.positiveTimeSetting(
        "reshard.split.source_shard_state_machine_retry_delay",
        TimeValue.timeValueSeconds(10),
        Setting.Property.NodeScope
    );

    private final Client client;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final StatelessCommitService commitService;
    private final ObjectStoreService objectStoreService;
    private final ReshardIndexService reshardIndexService;
    private final TaskManager taskManager;

    private final TimeValue deleteUnownedDelay;

    // Tracks active START_SPLIT requests received from target shards per source shard.
    // Target shard primary term is used to reject requests from stale shard instances or cancel ongoing request task.
    private final ConcurrentHashMap<IndexShard, SplitRequestState> activeTargetRequests = new ConcurrentHashMap<>();
    // Tracks source shard state machine that performs cleanup logic and moves source shard to DONE once all target shards are complete.
    private final ConcurrentHashMap<IndexShard, SourceShardStateMachine> activeSourceShards = new ConcurrentHashMap<>();

    // ES-12460 for testing purposes, until pre-handoff logic (flush etc) is built out
    @Nullable
    private Runnable preHandoffHook;

    public SplitSourceService(
        Client client,
        ClusterService clusterService,
        IndicesService indicesService,
        StatelessCommitService commitService,
        ObjectStoreService objectStoreService,
        ReshardIndexService reshardIndexService,
        TaskManager taskManager,
        Settings settings
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.commitService = commitService;
        this.objectStoreService = objectStoreService;
        this.reshardIndexService = reshardIndexService;
        this.taskManager = taskManager;

        this.deleteUnownedDelay = RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD.get(settings);
    }

    /**
     * Start populating a target shard that has been newly created by a resharding operation
     * <p>
     * A newly created shard starts in CLONE state and has no backing lucene index. This operation
     * sets up a lucene index on the target shard by copying the source shard's lucene index to the target
     * directory on the blob store, and arranging for any new commits to also be copied to the target until the
     * target shard enters handoff state. Once the source has finished copying all the existing commits, it prepares for
     * handoff by acquiring indexing permits, flushing, and stopping the copying of new commits, and then calls
     * the listener with a handle on the permits that are held if it succeeds, so that the listener can perform
     * the actual handoff with the target shard.
     * <p>
     * This function also registers a cluster state observer to trigger cleaning up the local shard when all
     * targets have advanced to split, if one isn't already registered.
     * @param task the task associated with the split request
     * @param targetShardId the id of the target shard to populate
     * @param sourcePrimaryTerm the primary term of the source shard seen by the target when it requested population
     * @param targetPrimaryTerm the primary term of the target shard when it requested population
     *     These are used to ensure that the copy operation hasn't become stale due to advances on either side.
     * @param listener called when the target shard is ready for handoff, with a handle on the indexing permits that must be
     *     released when handoff completes. This function may throw so the caller should wrap the listener to catch exceptions and inject
     *     them into the listener's failure handler.
     */
    public void setupTargetShard(
        CancellableTask task,
        ShardId targetShardId,
        long sourcePrimaryTerm,
        long targetPrimaryTerm,
        ActionListener<Releasable> listener
    ) {
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

        var sourceShard = indicesService.indexServiceSafe(index).getShard(sourceShardIndex);

        long currentSourcePrimaryTerm = sourceShard.getOperationPrimaryTerm();
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

            throw new StaleSplitRequestException(message);
        }
        if (currentSourcePrimaryTerm > sourcePrimaryTerm) {
            // This request is stale, fail the target recovery process.
            String message = String.format(
                Locale.ROOT,
                "Split [%s -> %s]. Source primary term advanced [%s -> %s] before start split request was handled. Failing the request.",
                sourceShardId,
                targetShardId,
                sourcePrimaryTerm,
                currentSourcePrimaryTerm
            );
            logger.info(message);

            throw new StaleSplitRequestException(message);
        }

        // We keep the invariant that targetPrimaryTerm >= sourcePrimaryTerm because this is required for data copying to work.
        // That is because commits are stored using a key that contains primary term.
        // If that invariant is not upheld, target shard will miss some of the source shard data.
        // See IndexMetadataUpdater#splitPrimaryTerm.
        if (currentSourcePrimaryTerm > targetPrimaryTerm) {
            // Source shard recovered in between target shard being allocated and target shard sending a start split request.
            // This request is invalid as per above.
            String message = String.format(
                Locale.ROOT,
                "Split [%s -> %s]. Primary term of the target shard [%s] < primary term [%s] of the source shard. Failing the request.",
                sourceShardId,
                targetShardId,
                targetPrimaryTerm,
                sourcePrimaryTerm
            );
            logger.info(message);

            throw new StaleSplitRequestException(message);
        }

        // Defensive check so that some other process (like recovery) won't interfere with resharding.
        var sourceShardState = sourceShard.state();
        if (sourceShardState != IndexShardState.STARTED) {
            String message = String.format(
                Locale.ROOT,
                "Split [%s -> %s]. Source shard is not started when processing start split request. Failing the request.",
                sourceShardId,
                targetShardId
            );
            logger.info(message);

            throw new IndexShardNotStartedException(sourceShardId, sourceShardState);
        }

        // If this is the first time a target shard contacted this source shard to start a split,
        // we need to start tracking this split to be able to eventually properly finish it.
        // If we have already seen this split before, we are all set already.
        setupSourceShardStateMachine(sourceShard);

        var currentSplit = activeTargetRequests.putIfAbsent(sourceShard, new SplitRequestState(targetPrimaryTerm, task));
        if (currentSplit != null) {
            // The source shard is currently handling a split request. This can occur if the target shard failed and is recovering or was
            // relocated. The new target shard instance will repeatedly fail recovery until the current split request completes.
            if (targetPrimaryTerm >= currentSplit.targetPrimaryTerm) {
                // Cancel current split request as it is likely stale
                taskManager.cancelTaskAndDescendants(task, "stale split request", false, ActionListener.noop());
            }
            String message = String.format(
                Locale.ROOT,
                "Split [%s -> %s]. Source shard is already setting up target shard. Failing the request.",
                sourceShardId,
                targetShardId
            );
            logger.error(message);

            throw new IllegalStateException(message);
        }

        commitService.markSplitting(sourceShardId, targetShardId);
        SubscribableListener.<Releasable>newForked(l -> sourceShard.acquirePrimaryOperationPermit(l, clusterService.threadPool().generic()))
            .<Releasable>andThen((l, permit) -> {
                try (Releasable ignore = permit) {
                    objectStoreService.copyShard(task, sourceShardId, targetShardId, sourcePrimaryTerm);
                }
                prepareForHandoff(l, sourceShard, targetShardId);
            })
            .addListener(listener.delegateResponse((l, e) -> {
                try {
                    commitService.markSplitEnding(sourceShardId, targetShardId, true);
                } catch (AlreadyClosedException ignored) {
                    // It's okay to not clean up the splitting flag since the shard is closed anyway
                    // and there will be no new commits.
                    // We explicitly swallow this exception since the contract of `delegateResponse` is to not throw.
                }
                activeTargetRequests.remove(sourceShard);
                l.onFailure(e);
            }));
    }

    public void setPreHandoffHook(Runnable preHandoffHook) {
        assert this.preHandoffHook == null;
        this.preHandoffHook = preHandoffHook;
    }

    /**
     * Prepare to complete handoff to target shard
     * Blocks indexing and flushes the source to synchronize the target with the latest changes,
     * and turns off commit copying after the flush. Indexing will resume after the provided listener
     * has completed.
     * @param handoffListener A listener to be called when preparation has completed, to attempt the handoff itself.
     *     It is responsible for releasing the provided releasable when it completes the handoff attempt.
     * @param sourceShard the source shard of the split that is ready for handoff.
     * @param targetShardId the shardId of the target of the split operation that is ready for handoff.
     */
    private void prepareForHandoff(ActionListener<Releasable> handoffListener, IndexShard sourceShard, ShardId targetShardId) {
        // testing hook, to be removed
        if (preHandoffHook != null) {
            preHandoffHook.run();
        }

        var stateMachine = activeSourceShards.get(sourceShard);
        if (stateMachine == null) {
            throw new AlreadyClosedException("Split source shard " + sourceShard.shardId() + " is closed");
        }

        logger.debug("preparing for handoff to {}", targetShardId);
        SubscribableListener<Releasable> withPermits = SubscribableListener.<Void>newForked(
            afterMutable -> sourceShard.ensureMutable(afterMutable, false)
        ).<Engine.FlushResult>andThen(afterFirstFlush -> sourceShard.withEngine(engine -> {
            logger.debug("handoff: flushing {} for {} before acquiring permits", sourceShard.shardId(), targetShardId);
            // Similar to relocation, flush before blocking operations because we expect this to reduce the amount of work done by the
            // flush that happens while operations are blocked. NB the flush has force=false so may do nothing.
            engine.flush(/* force */ false, /* waitIfOngoing */ false, afterFirstFlush);
            return null;
        }))
            .<Releasable>andThen(acquiredPermits -> stateMachine.split().withPermits(acquiredPermits))
            .andThen((afterSecondFlush, permits) -> {
                // withEngine and flush can throw, and we don't want to leak permits if it does
                try {
                    sourceShard.withEngine(engine -> {
                        logger.debug("handoff: flushing {} for {} after acquiring permits", sourceShard.shardId(), targetShardId);
                        // Don't stop copying commits until anything outstanding has been flushed.
                        engine.flush(/* force */ false, /* waitIfOngoing */ true, ActionListener.wrap(fr -> {
                            // No commits need to be copied after the flush, but it is possible that some might be if the engine generates
                            // commits spontaneously even though indexing permits are held. These are harmless to copy.
                            logger.debug("handoff: stopping commit copy from {} to {}", sourceShard.shardId(), targetShardId);
                            stopCopyingNewCommits(targetShardId);
                            activeTargetRequests.remove(sourceShard);
                            afterSecondFlush.onResponse(permits);
                        }, e -> {
                            permits.close();
                            afterSecondFlush.onFailure(e);
                        }));
                        return null;
                    });
                } catch (Exception e) {
                    permits.close();
                    afterSecondFlush.onFailure(e);
                }
            });
        withPermits.addListener(handoffListener);
    }

    public void stopCopyingNewCommits(ShardId targetShardId) {
        commitService.markSplitEnding(getSplitSource(targetShardId), targetShardId, false);
    }

    public void splitSourceShardStarted(IndexShard indexShard, IndexReshardingMetadata reshardingMetadata) {
        IndexReshardingState.Split split = reshardingMetadata.getSplit();

        if (split.getSourceShardState(indexShard.shardId().getId()) == IndexReshardingState.Split.SourceShardState.DONE) {
            // Nothing to do.
            return;
        }

        /// It is possible that the shard is already STARTED at this point,
        /// see [org.elasticsearch.indices.cluster.IndicesClusterStateService#updateShard].
        /// As such it is possible that we are already accepting requests to start split from targets.
        /// If any of them already set up cleanup infrastructure we don't need to do anything here.
        setupSourceShardStateMachine(indexShard);
    }

    private void setupSourceShardStateMachine(IndexShard sourceShard) {
        activeSourceShards.compute(sourceShard, (shard, stateMachine) -> {
            if (stateMachine == null) {
                var newMachine = new SourceShardStateMachine(shard, () -> this.activeSourceShards.remove(shard));
                newMachine.run();
                return newMachine;
            }

            return stateMachine;
        });
    }

    private final class HandoffConvergenceObserver {

        private enum Outcome {
            HANDOFF_SUCCESS,
            SOURCE_PRIMARY_ADVANCED,
            TARGET_PRIMARY_ADVANCED,
            INDEX_DELETED
        }

        private static final class Decision {
            private Outcome outcome;
            private long currentSourcePrimaryTerm;
            private long currentTargetPrimaryTerm;
        }

        private final ShardId targetShardId;
        private final ShardId sourceShardId;
        private final long expectedTargetPrimaryTerm;
        private final long expectedSourcePrimaryTerm;
        private final AtomicBoolean shouldRetry;
        private final Releasable permits;
        private final ActionListener<ActionResponse> listener;
        private final Decision decision = new Decision();

        HandoffConvergenceObserver(
            ShardId targetShardId,
            ShardId sourceShardId,
            long expectedTargetPrimaryTerm,
            long expectedSourcePrimaryTerm,
            AtomicBoolean shouldRetry,
            Releasable permits,
            ActionListener<ActionResponse> listener
        ) {
            this.targetShardId = targetShardId;
            this.sourceShardId = sourceShardId;
            this.expectedTargetPrimaryTerm = expectedTargetPrimaryTerm;
            this.expectedSourcePrimaryTerm = expectedSourcePrimaryTerm;
            this.shouldRetry = shouldRetry;
            this.permits = permits;
            this.listener = listener;
        }

        void start() {
            ClusterStateObserver.waitForState(
                clusterService,
                clusterService.threadPool().getThreadContext(),
                new ClusterStateObserver.Listener() {

                    @Override
                    public void onNewClusterState(ClusterState state) {
                        assert decision.outcome != null : "Predicate returned true without setting outcome";

                        shouldRetry.set(false);
                        permits.close();

                        switch (decision.outcome) {
                            case HANDOFF_SUCCESS -> {
                                logger.debug("Target observed in HANDOFF state by source shard {}", sourceShardId);
                                listener.onResponse(null);
                            }
                            case SOURCE_PRIMARY_ADVANCED -> {
                                String message = format(
                                    "%s source primary term advanced [%s>%s], handoff failed",
                                    sourceShardId,
                                    decision.currentSourcePrimaryTerm,
                                    expectedSourcePrimaryTerm
                                );
                                logger.debug(message);
                                listener.onFailure(new StaleSplitRequestException(message));
                            }
                            case TARGET_PRIMARY_ADVANCED -> {
                                String message = format(
                                    "%s target primary term advanced [%s>%s], handoff failed",
                                    targetShardId,
                                    decision.currentTargetPrimaryTerm,
                                    expectedTargetPrimaryTerm
                                );
                                logger.debug(message);
                                listener.onFailure(new StaleSplitRequestException(message));
                            }
                            case INDEX_DELETED -> {
                                logger.debug("Index [{}] was deleted while waiting for handoff", targetShardId.getIndex());
                                listener.onFailure(new IndexNotFoundException(targetShardId.getIndex()));
                            }
                        }
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        logger.error("{} Ignoring timeout for HANDOFF wait", sourceShardId);
                    }

                    @Override
                    public void onClusterServiceClose() {
                        // TODO: Should we release permits here ? Does it matter ?
                        permits.close();
                        listener.onFailure(new NodeClosedException(clusterService.localNode()));
                    }
                },
                this::hasConverged,
                null,
                logger
            );
        }

        private boolean hasConverged(ClusterState clusterState) {
            Index index = targetShardId.getIndex();
            Optional<IndexMetadata> indexMetadataOpt = clusterState.metadata().findIndex(index);
            if (indexMetadataOpt.isEmpty()) {
                decision.outcome = Outcome.INDEX_DELETED;
                return true;
            }
            IndexMetadata indexMetadata = indexMetadataOpt.get();

            IndexReshardingMetadata reshardingMetadata = indexMetadata.getReshardingMetadata();

            long currentTargetPrimary = indexMetadata.primaryTerm(targetShardId.getId());
            long currentSourcePrimary = indexMetadata.primaryTerm(sourceShardId.getId());

            if (reshardingMetadata != null
                && reshardingMetadata.getSplit()
                    .targetStateAtLeast(targetShardId.id(), IndexReshardingState.Split.TargetShardState.HANDOFF)) {
                decision.outcome = Outcome.HANDOFF_SUCCESS;
                return true;
            }

            if (currentSourcePrimary > expectedSourcePrimaryTerm) {
                decision.outcome = Outcome.SOURCE_PRIMARY_ADVANCED;
                decision.currentSourcePrimaryTerm = currentSourcePrimary;
                return true;
            }

            if (currentTargetPrimary > expectedTargetPrimaryTerm) {
                decision.outcome = Outcome.TARGET_PRIMARY_ADVANCED;
                decision.currentTargetPrimaryTerm = currentTargetPrimary;
                return true;
            }
            return false;
        }
    }

    /**
     * This cluster state observer waits for cluster state to converge to one of 3 states
     * - target shard in HANDOFF state  (indicates a successful handoff)
     * - source primary term advanced   (indicates handoff unsuccessful)
     * - target primary term advanced   (indicates handoff unsuccessful)
     * @param targetShardId       target shard id
     * @param targetPrimaryTerm   target shard primary term
     * @param sourcePrimaryTerm   source shard primary term
     * @param shouldRetry         set to false once cluster state converges
     * @param permits             indexing permits that were acquired before initiating handoff,
     *                            should be release once cluster state has converged
     * @param listener            listener to complete once cluster state has converged
     */
    public void waitForHandoffSuccessOrFailure(
        ShardId targetShardId,
        long targetPrimaryTerm,
        long sourcePrimaryTerm,
        AtomicBoolean shouldRetry,
        Releasable permits,
        ActionListener<ActionResponse> listener
    ) {
        Index index = targetShardId.getIndex();
        IndexMetadata indexMetadata = clusterService.state().metadata().projectFor(index).getIndexSafe(index);
        IndexReshardingMetadata reshardingMetadata = indexMetadata.getReshardingMetadata();

        assert reshardingMetadata != null && reshardingMetadata.isSplit() : "Unexpected resharding state";

        int sourceShardIndex = reshardingMetadata.getSplit().sourceShard(targetShardId.getId());
        ShardId sourceShardId = new ShardId(index, sourceShardIndex);

        new HandoffConvergenceObserver(targetShardId, sourceShardId, targetPrimaryTerm, sourcePrimaryTerm, shouldRetry, permits, listener)
            .start();
    }

    private ShardId getSplitSource(ShardId targetShardId) {
        Index index = targetShardId.getIndex();

        var indexMetadata = clusterService.state().metadata().projectFor(index).getIndexSafe(index);
        var reshardingMetadata = indexMetadata.getReshardingMetadata();
        assert reshardingMetadata != null && reshardingMetadata.isSplit() : "Unexpected resharding state";
        int sourceShardIndex = reshardingMetadata.getSplit().sourceShard(targetShardId.getId());

        return new ShardId(index, sourceShardIndex);
    }

    // visible for testing
    static class RefCountedAcquirer {
        AtomicInteger refCount = new AtomicInteger();
        Consumer<ActionListener<Releasable>> acquirer;
        LongSupplier nowInMillis;
        LongConsumer onReleased;
        @Nullable
        Releasable releasable;
        @Nullable
        SubscribableListener<Releasable> onAcquired;
        long acquireStartMillis;

        RefCountedAcquirer(Consumer<ActionListener<Releasable>> acquirer, LongSupplier nowInMillis, LongConsumer onReleased) {
            this.acquirer = acquirer;
            this.nowInMillis = nowInMillis;
            this.onReleased = onReleased;
        }

        /**
         * Notify a listener when the underlying resource is held.
         * If it is already held, its reference count will be incremented and the listener will be notified immediately.
         * If resource acquisition fails, the listener will be notified of that failure.
         * @param onAcquired a listener that will be notified when the resource managed by this acquirer has been acquired.
         *     The listener is provided a releasable on success, which it must release when it is done with the resource
         *         (which may be after the listener has been notified).
         *     The listener's onResponse handler must not throw, because it would be unclear who should release the resource
         *     in that case -- the listener might already have released it, or created a task that will before throwing.
         */
        public void acquire(ActionListener<Releasable> onAcquired) {
            synchronized (this) {
                if (refCount.incrementAndGet() == 1) {
                    // create a listener that will first acquire, and if that succeeds, squirrel away
                    // the returned releasable to be dropped when the refcount goes to zero, and then
                    // complete any listeners that may be listening for it.
                    this.onAcquired = SubscribableListener.newForked(l -> acquirer.accept(l.delegateFailure((inner, releasable) -> {
                        this.releasable = releasable;
                        this.acquireStartMillis = nowInMillis.getAsLong();
                        inner.onResponse(this::release);
                    })));
                }
            }
            assert this.onAcquired != null;
            // This method always increments the refcount, but it is only the responsibility of onAcquired to decrement it
            // if acquisition succeeded. If it fails, this takes care of decrementing it.
            this.onAcquired.addListener(onAcquired.delegateResponse((l, e) -> {
                l.onFailure(e);
                release();
            }));
        }

        public void release() {
            Releasable releasable = null;
            synchronized (this) {
                if (refCount.decrementAndGet() == 0) {
                    releasable = this.releasable;
                    this.releasable = null;
                    onAcquired = null;
                }
            }

            // may be null if onAcquired failed
            if (releasable != null) {
                // release outside of lock, since operation may take time
                releasable.close();
                long acquiredDurationMillis = nowInMillis.getAsLong() - this.acquireStartMillis;
                onReleased.accept(acquiredDurationMillis);
            }
        }
    }

    public void cancelSplits(IndexShard indexShard) {
        activeTargetRequests.remove(indexShard);
        var stateMachine = activeSourceShards.remove(indexShard);
        if (stateMachine != null) {
            stateMachine.cancel();
        }
    }

    private static IndexReshardingState.Split getSplit(ClusterState state, Index index) {
        return state.metadata()
            .lookupProject(index)
            .flatMap(p -> Optional.ofNullable(p.index(index)))
            .flatMap(m -> Optional.ofNullable(m.getReshardingMetadata()))
            .map(IndexReshardingMetadata::getSplit)
            .orElse(null);
    }

    // State of split request being processed
    private record SplitRequestState(long targetPrimaryTerm, CancellableTask task) {}

    // Holds resources needed to manage an ongoing split
    private class Split {
        // used to acquire permits on the source shard with reference counting, so that if the permits are already held
        // we can bump the refcount instead of waiting for them to be released and then reacquiring.
        // This speeds up handoff when multiple target shards enter handoff concurrently.
        final RefCountedAcquirer permitAcquirer;

        Split(IndexShard sourceShard) {
            // XXX figure out what to do with the timeout on acquiring the permit. I would guess that we're blocking any new requests that
            // come in while we're waiting for existing ones to drain, so we probably don't want to wait a very long time, but if we
            // fail we need a way to retry later with some backoff. Ticket this.
            permitAcquirer = new RefCountedAcquirer(
                releasableListener -> sourceShard.acquireAllPrimaryOperationsPermits(releasableListener, TimeValue.ONE_MINUTE),
                SplitSourceService.this.indicesService.clusterService().threadPool().relativeTimeInMillisSupplier(),
                acquiredDuration -> SplitSourceService.this.reshardIndexService.getReshardMetrics()
                    .indexingBlockedDurationHistogram()
                    .record(acquiredDuration)
            );
        }

        /**
         * Calls listener when permits for the source shard are held.
         * If they are not yet held they will be acquired before calling the listener, but if they are already held a reference count
         * on them will be incremented and the listener will be called immediately.
         * The reference count will be decremented when the listener completes, and permits will be released when the reference count
         * reaches zero.
         * @param listener a listener to call when permits have been acquired
         */
        public void withPermits(ActionListener<Releasable> listener) {
            permitAcquirer.acquire(listener);
        }
    }

    private class SourceShardStateMachine {
        private final IndexShard indexShard;
        private final Runnable onCompleted;
        private final Split split;

        private final AtomicBoolean cancelled;

        private State currentState;

        private SourceShardStateMachine(IndexShard indexShard, Runnable onCompleted) {
            this.indexShard = indexShard;
            this.onCompleted = onCompleted;

            this.split = new Split(indexShard);
            this.cancelled = new AtomicBoolean(false);

            // This is the only starting state that we support for simplicity.
            // All the operations are idempotent.
            this.currentState = new State.MonitoringTargetShards();
        }

        /// Starts the state machine from the current state.
        void run() {
            advance(currentState);
        }

        void cancel() {
            cancelled.set(true);
        }

        // It is important to share the instance of `Split` between all target shards - that's how it is designed.
        // So we keep it inside the state machine since unlike the entries in `activeTargetRequests` there is only one per
        // source shard.
        public Split split() {
            // This should only be used during handoff.
            assert currentState instanceof State.MonitoringTargetShards;

            return split;
        }

        private void advance(State newState) {
            // We always fork to generic here since we get callbacks
            // from various places like cluster state update threads
            // and transport threads which we don't want to block.
            // We only do this in a linear fashion (once the previous state transition logic completed).
            clusterService.threadPool().generic().submit(() -> advanceInternal(newState));
        }

        private void advanceInternal(State newState) {
            validateStateTransition(newState);
            this.currentState = newState;

            // TODO relax logging once implementation is stable
            logger.info("Advancing split source shard state machine for shard {} to {}", indexShard.shardId(), newState);

            switch (newState) {
                case State.MonitoringTargetShards ignored -> {
                    monitorTargetShardsState();
                }
                case State.TargetShardsDone ignored -> {
                    /// This code already runs on generic thread pool.
                    /// We use `scheduleUnlessShuttingDown` just to implement a delay.
                    /// See [RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD] for the explanation of this delay.
                    clusterService.threadPool()
                        .scheduleUnlessShuttingDown(
                            deleteUnownedDelay,
                            clusterService.threadPool().generic(),
                            this::changeStateToReadyForCleanup
                        );
                }
                case State.ReadyForCleanup ignored -> {
                    deleteUnownedData();
                }
                case State.CleanupComplete ignored -> {
                    changeStateToDone();
                }
                case State.Done ignored -> {
                    logger.info(Strings.format("Split source shard %s successfully completed split workflow", indexShard.shardId()));

                    onCompleted.run();
                }
                case State.Failed failed -> {
                    if (cancelled.get()) {
                        return;
                    }

                    // TODO relax logging once the feature is stable
                    logger.info("Failed to complete split source shard cleanup workflow, will retry", failed.exception);

                    // Source shards do not go through recovery during splits.
                    // As such we don't have a good place to reliably resume this workflow
                    // and risk stalling the split if a source shard fails and never recovers.
                    // We would like to play safe here and try to make progress.
                    // So with that in mind we will retry our idempotent workflow on any failure.
                    // There is no specific failure that this logic targets (at the time of writing),
                    // this is purely a "what if".
                    // Note that this is not recursive and cheap to do on the scheduler thread
                    // since we submit to generic thread pool inside `advance`.
                    clusterService.threadPool()
                        .schedule(
                            () -> advance(new State.MonitoringTargetShards()),
                            // Prevent from spinning as fast as possible.
                            TimeValue.timeValueMillis(100),
                            EsExecutors.DIRECT_EXECUTOR_SERVICE
                        );
                }
            }
        }

        private void validateStateTransition(State newState) {
            var validCurrentStates = newStateToValidCurrentStates.get(newState.getClass());
            if (validCurrentStates == null || validCurrentStates.contains(currentState.getClass()) == false) {
                // It's possible that this exception is not observed by anyone since we are inside a runnable on generic thread pool.
                // So we log as well.
                var message = String.format(Locale.ROOT, "Unexpected split source shard state transition %s -> %s", currentState, newState);
                logger.error(message);
                assert false : message;
                throw new IllegalStateException(message);
            }
        }

        private static final Map<Class<? extends State>, Set<Class<? extends State>>> newStateToValidCurrentStates = new HashMap<>() {
            {
                // Can go Failed -> MonitoringTargetShards due to retries.
                put(State.MonitoringTargetShards.class, Set.of(State.MonitoringTargetShards.class, State.Failed.class));
                put(State.TargetShardsDone.class, Set.of(State.MonitoringTargetShards.class));
                put(State.ReadyForCleanup.class, Set.of(State.TargetShardsDone.class));
                put(State.CleanupComplete.class, Set.of(State.ReadyForCleanup.class));
                put(State.Done.class, Set.of(State.CleanupComplete.class));
                put(State.Failed.class, Set.of(State.TargetShardsDone.class, State.ReadyForCleanup.class, State.CleanupComplete.class));
            }
        };

        sealed interface State permits State.MonitoringTargetShards, State.TargetShardsDone, State.ReadyForCleanup, State.CleanupComplete,
            State.Done, State.Failed {
            record MonitoringTargetShards() implements State {}

            record TargetShardsDone() implements State {}

            record ReadyForCleanup() implements State {}

            record CleanupComplete() implements State {}

            record Done() implements State {}

            record Failed(Exception exception) implements State {}
        }

        private void monitorTargetShardsState() {
            var allTargetsAreDonePredicate = new Predicate<ClusterState>() {
                @Override
                public boolean test(ClusterState state) {
                    if (cancelled.get()) {
                        return true;
                    }

                    IndexReshardingState.Split split = getSplit(state, indexShard.shardId().getIndex());
                    // This shouldn't be possible.
                    // If there is a new instance of the shard that already completed the split,
                    // current instance of the shard should be removed and we would hit the cancelled branch above.
                    assert split != null;

                    if (indexShard.state() != IndexShardState.STARTED) {
                        // State can be POST_RECOVERY here because split progress tracking is set up during recovery.
                        // It's possible that the very first cluster state we observe has all targets in DONE but the recovery hasn't
                        // completed yet.
                        // We need to be STARTED to properly execute deletion of unowned documents so we'll wait for the cluster state
                        // change that sets this shard to STARTED.
                        // CLOSED is also possible if state change is applied to the shard but not yet reflected in the `cancelled`.
                        // In this case we will eventually observe this change via `cancelled` and handle it properly.
                        return false;
                    }

                    return split.targetsDone(indexShard.shardId().getId());
                }
            };

            ClusterStateObserver.waitForState(
                clusterService,
                clusterService.threadPool().getThreadContext(),
                new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        if (cancelled.get()) {
                            return;
                        }

                        advance(new State.TargetShardsDone());
                    }

                    @Override
                    public void onClusterServiceClose() {
                        // nothing to do
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        // there is no timeout
                        assert false;
                    }
                },
                allTargetsAreDonePredicate,
                null,
                logger
            );
        }

        /// Signals to the search shards that they should start rejecting search requests that do not have current shard count summary.
        /// We are going to delete unowned data in the next step and if search shards still served requests with stale summaries
        /// such requests would now return incorrect results.
        /// That is because unowned data is deleted _and_ target shards are not included in the search since the request is stale.
        private void changeStateToReadyForCleanup() {
            SubscribableListener.newForked(
                l -> client.execute(
                    TransportUpdateSplitSourceShardStateAction.TYPE,
                    new TransportUpdateSplitSourceShardStateAction.Request(
                        indexShard.shardId(),
                        indexShard.routingEntry().allocationId(),
                        IndexReshardingState.Split.SourceShardState.READY_FOR_CLEANUP
                    ),
                    // the response is empty
                    l.map(ignored -> null)
                )
            )
                // We need to observe the change we just made since we need the cluster state version to await below.
                .<ClusterState>andThen(
                    clusterService.threadPool().generic(),
                    clusterService.threadPool().getThreadContext(),
                    (stepListener, ignored) -> ClusterStateObserver.waitForState(
                        clusterService,
                        clusterService.threadPool().getThreadContext(),
                        new ClusterStateObserver.Listener() {
                            @Override
                            public void onNewClusterState(ClusterState state) {
                                if (cancelled.get()) {
                                    return;
                                }

                                stepListener.onResponse(state);
                            }

                            @Override
                            public void onClusterServiceClose() {
                                // Nothing to do, shard will be closed when the node closes.
                            }

                            @Override
                            public void onTimeout(TimeValue timeout) {
                                // There is no timeout.
                            }
                        },
                        state -> {
                            if (cancelled.get()) {
                                return true;
                            }

                            // If the index is deleted or this split has completed concurrently
                            // (maybe we've received a batch of cluster state updates) we need to exit.
                            IndexReshardingState.Split split = getSplit(state, indexShard.shardId().getIndex());
                            return split == null
                                || split.getSourceShardState(indexShard.shardId().id())
                                    .ordinal() >= IndexReshardingState.Split.SourceShardState.READY_FOR_CLEANUP.ordinal();
                        },
                        // No timeout.
                        // We know we have just submitted this cluster state update,
                        // so we will either see it shortly or we are not in the cluster anymore
                        // and there is probably another instance of the shard being allocated.
                        // In the latter case we may as well wait since we are not blocking any user operations.
                        null,
                        logger
                    )
                )
                .andThen(
                    clusterService.threadPool().generic(),
                    clusterService.threadPool().getThreadContext(),
                    (stepListener, clusterState) -> {
                        Index index = indexShard.shardId().getIndex();
                        var indexRoutingTable = clusterState.metadata()
                            .lookupProject(index)
                            .flatMap(pm -> Optional.ofNullable(clusterState.routingTable(pm.id()).index(index)));
                        if (indexRoutingTable.isEmpty()) {
                            // The index was deleted, nothing to do.
                            // Eventually this shard will be closed and all split-related state will be cleaned up.
                            return;
                        }

                        // If a search shard is not assigned in this cluster state then it is going to be assigned
                        // in some subsequent cluster state version.
                        // As such it will also observe the needed READY_FOR_CLEANUP state of the source shard since it is already applied
                        // in this version.
                        var assignedUnpromotableShards = indexRoutingTable.get()
                            .shard(indexShard.shardId().id())
                            .assignedUnpromotableShards();
                        if (assignedUnpromotableShards.isEmpty()) {
                            stepListener.onResponse(null);
                            return;
                        }

                        var nodes = assignedUnpromotableShards.stream()
                            .map(shardRouting -> clusterState.nodes().get(shardRouting.currentNodeId()))
                            .toArray(DiscoveryNode[]::new);
                        client.execute(
                            TransportAwaitClusterStateVersionAppliedAction.TYPE,
                            new AwaitClusterStateVersionAppliedRequest(
                                clusterState.version(),
                                // We don't need a strict timeout here since all target shards are done, and we are not delaying
                                // any impactful operations.
                                TimeValue.MINUS_ONE,
                                nodes
                            ),
                            stepListener.delegateFailure((responseHandlingListener, response) -> {
                                if (response.hasFailures()) {
                                    // We'll retry the entire workflow for simplicity.
                                    responseHandlingListener.onFailure(new ElasticsearchException("""
                                        {} Failed to await the application of READY_FOR_CLEANUP\
                                         split source shard state on search shards, will retry""", indexShard.shardId()));
                                } else {
                                    responseHandlingListener.onResponse(null);
                                }
                            })
                        );
                    }
                )
                .addListener(new StateAdvancingListener<>(new State.ReadyForCleanup()));
        }

        private void deleteUnownedData() {
            reshardIndexService.deleteUnownedDocuments(indexShard.shardId(), new StateAdvancingListener<>(new State.CleanupComplete()));
        }

        private void changeStateToDone() {
            client.execute(
                TransportUpdateSplitSourceShardStateAction.TYPE,
                new TransportUpdateSplitSourceShardStateAction.Request(
                    indexShard.shardId(),
                    indexShard.routingEntry().allocationId(),
                    IndexReshardingState.Split.SourceShardState.DONE
                ),
                new StateAdvancingListener<>(new State.Done())
            );
        }

        private class StateAdvancingListener<T> implements ActionListener<T> {
            private final State nextState;

            private StateAdvancingListener(State nextState) {
                this.nextState = nextState;
            }

            @Override
            public void onResponse(T t) {
                advance(nextState);
            }

            @Override
            public void onFailure(Exception e) {
                advance(new State.Failed(e));
            }
        }
    }
}
