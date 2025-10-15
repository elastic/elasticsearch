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
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;

import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
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
     * target shard enters handoff state. Once it has finished copying all the existing commits, it prepares for
     * handoff by acquiring indexing permits, flushing, and stopping the copying of new commits, and then calls
     * the listener with a handle on the permits that are held if it succeeds, so that the listener can perform
     * the actual handoff with the target shard.
     * <p>
     * This function also registers a cluster state observer to trigger cleaning up the local shard when all
     * targets have advanced to split, if one isn't already registered.
     * @param targetShardId the id of the target shard to populate
     * @param sourcePrimaryTerm the primary term of the source shard seen by the target when it requested population
     * @param targetPrimaryTerm the primary term of the target shard when it requested population
     *     These are used to ensure that the copy operation hasn't become stale due to advances on either side.
     * @param listener called when the target shard is ready for handoff, with a handle on the indexing permits that must be
     *     released when handoff completes. This function may throw so the caller should wrap the listener to catch exceptions and inject
     *     them into the listener's failure handler.
     */
    public void setupTargetShard(
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

        if (onGoingSplits.putIfAbsent(sourceShard, new Split(sourceShard)) == null) {
            // This is the first time a target shard contacted this source shard to start a split.
            // We'll start tracking this split now to be able to eventually properly finish it.
            // If we have already seen this split before, we are all set already.
            setupSplitProgressTracking(sourceShard);
        }

        SubscribableListener.<Releasable>newForked(l -> {
            commitService.markSplitting(sourceShardId, targetShardId);
            objectStoreService.copyShard(sourceShardId, targetShardId, sourcePrimaryTerm);
            prepareForHandoff(l, sourceShard, targetShardId);
        }).addListener(listener.delegateResponse((l, e) -> {
            commitService.markSplitEnding(sourceShardId, targetShardId, true);
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

        var split = onGoingSplits.get(sourceShard);
        assert split != null;

        logger.debug("preparing for handoff to {}", targetShardId);
        SubscribableListener<Releasable> withPermits = SubscribableListener.newForked(split::withPermits)
            .andThen(
                (l, permits) -> SubscribableListener.<Void>newForked(afterMutable -> sourceShard.ensureMutable(afterMutable, true))
                    .andThen(afterFlush -> sourceShard.withEngine(engine -> {
                        logger.debug("handoff: flushing {} for {}", sourceShard.shardId(), targetShardId);
                        // Don't stop copying commits until anything outstanding has been flushed.
                        engine.flush(/* force */ true, /* waitIfOngoing */ true, afterFlush.map(fr -> {
                            // No commits need to be copied after the flush, but it is possible that some might be if the engine generates
                            // commits spontaneously even though indexing permits are held. These are harmless to copy.
                            logger.debug("handoff: stopping commit copy from {} to {}", sourceShard.shardId(), targetShardId);
                            stopCopyingNewCommits(targetShardId);
                            return null;
                        }));
                        return null;
                    }))
                    .addListener(l.map((ignored) -> permits))
            );
        withPermits.addListener(handoffListener);
    }

    public void stopCopyingNewCommits(ShardId targetShardId) {
        commitService.markSplitEnding(getSplitSource(targetShardId), targetShardId, false);
    }

    public void afterSplitSourceIndexShardRecovery(
        IndexShard indexShard,
        IndexReshardingMetadata reshardingMetadata,
        ActionListener<Void> listener
    ) {

        IndexReshardingState.Split split = reshardingMetadata.getSplit();

        if (split.getSourceShardState(indexShard.shardId().getId()) == IndexReshardingState.Split.SourceShardState.DONE) {
            // Nothing to do.
            listener.onResponse(null);
            return;
        }

        // It is possible that the shard is already STARTED at this point, see IndicesClusterStateService#updateShard.
        // As such it is possible that we are already accepting requests to start split from targets.
        // If any of them already set up tracking of the split process we don't need to do anything here.
        if (onGoingSplits.putIfAbsent(indexShard, new Split(indexShard)) != null) {
            listener.onResponse(null);
            return;
        }

        setupSplitProgressTracking(indexShard);
        listener.onResponse(null);
    }

    public void setupSplitProgressTracking(IndexShard indexShard) {
        var tracker = new SplitProgressTracker(indexShard, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                logger.info(Strings.format("Split source shard %s successfully transitioned to DONE", indexShard.shardId()));
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(Strings.format("Error while tracking split progress in source shard %s: %s", indexShard.shardId(), e));
            }
        });

        tracker.run();
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
        @Nullable
        Releasable releasable;
        @Nullable
        SubscribableListener<Releasable> onAcquired;

        RefCountedAcquirer(Consumer<ActionListener<Releasable>> acquirer) {
            this.acquirer = acquirer;
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
            }
        }
    }

    private class SplitProgressTracker extends RetryableAction<Void> {
        private final IndexShard indexShard;

        private SplitProgressTracker(IndexShard indexShard, ActionListener<Void> listener) {
            super(
                logger,
                clusterService.threadPool(),
                // this logic is not time-sensitive, retry delays do not need to be tight
                TimeValue.timeValueSeconds(5), // initialDelay
                TimeValue.timeValueSeconds(30), // maxDelayBound
                TimeValue.MAX_VALUE, // timeoutValue
                listener,
                clusterService.threadPool().generic()
            );
            this.indexShard = indexShard;
        }

        @Override
        public void tryAction(ActionListener<Void> listener) {
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

                    if (indexShard.state() != IndexShardState.STARTED) {
                        // State can be POST_RECOVERY here because split progress tracking is set up during recovery.
                        // It's possible that the very first cluster state we observe has all targets in DONE but the recovery hasn't
                        // completed yet.
                        // We need to be STARTED to properly execute deletion of unowned documents so we'll wait for the cluster state
                        // change that sets this shard to STARTED.
                        // CLOSED is also possible if state change is applied to the shard but not yet reflected in the `onGoingSplits`.
                        // In this case we will eventually observe this change via `onGoingSplits` and handle it properly.
                        return false;
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
                    // It is not a problem since delete of unowned documents is idempotent.
                    clusterService.threadPool().generic().submit(() -> moveToDone(indexShard, listener));
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

        @Override
        public boolean shouldRetry(Exception e) {
            if (e instanceof IndexShardClosedException) {
                // Shard is closed, but it was not reflected in `onGoingSplits`, we'll resume the tracking logic on next recovery.
                return false;
            }
            return true;
        }
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

    // Holds resources needed to manage an ongoing split
    private static class Split {
        // used to acquire permits on the source shard with reference counting, so that if the permits are already held
        // we can bump the refcount instead of waiting for them to be released and then reacquiring.
        // This speeds up handoff when multiple target shards enter handoff concurrently.
        final RefCountedAcquirer permitAcquirer;

        Split(IndexShard sourceShard) {
            // XXX figure out what to do with the timeout on acquiring the permit. I would guess that we're blocking any new requests that
            // come in while we're waiting for existing ones to drain, so we probably don't want to wait a very long time, but if we
            // fail we need a way to retry later with some backoff. Ticket this.
            permitAcquirer = new RefCountedAcquirer(
                releasableListener -> sourceShard.acquireAllPrimaryOperationsPermits(releasableListener, TimeValue.ONE_MINUTE)
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
}
