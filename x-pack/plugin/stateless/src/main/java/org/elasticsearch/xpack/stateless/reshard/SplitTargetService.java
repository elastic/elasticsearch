/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.state.AwaitClusterStateVersionAppliedRequest;
import org.elasticsearch.action.admin.cluster.state.TransportAwaitClusterStateVersionAppliedAction;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterShardHealth;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.transport.ActionTransportException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SplitTargetService {
    public static final Setting<TimeValue> RESHARD_SPLIT_SEARCH_SHARDS_ONLINE_TIMEOUT = Setting.positiveTimeSetting(
        "reshard.split.search_shards_online_timeout",
        TimeValue.timeValueSeconds(30),
        Setting.Property.NodeScope
    );

    // Refresh is blocked while we are awaiting SPLIT state application on all cluster members.
    // This is needed since otherwise a successful refresh can be performed by a coordinator aware of SPLIT
    // followed a search performed by a coordinator that is not.
    // Such a search would not contain documents that were just successfully refreshed from the clients point of view.
    // The chance of observing this decreases as the time passes
    // since we get the chance to propagate the updated cluster state and kick out stragglers.
    // This timeout balances between this chance and the impact of refresh block on client operations.
    // It does mean that in current implementation this situation can still happen once we reach this timeout and give up,
    // we are just trying to minimize the possibility.
    // In practice though nodes that are not applying cluster state updates should be kicked out of the cluster
    // unblocking us.
    public static final Setting<TimeValue> RESHARD_SPLIT_SPLIT_STATE_APPLIED_TIMEOUT = Setting.positiveTimeSetting(
        "reshard.split.split_state_applied_timeout",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope
    );

    /// Timeout for internal retries when sending start split request to the source shard.
    /// This setting is only configured in tests and is not registered.
    public static final Setting<TimeValue> START_SPLIT_RETRY_TIMEOUT = Setting.positiveTimeSetting(
        "reshard.split.target_shard_start_split_retry_timeout",
        TimeValue.timeValueSeconds(60),
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(SplitTargetService.class);

    private final Client client;
    private final ClusterService clusterService;
    private final ReshardIndexService reshardIndexService;
    // TODO use this
    private final TimeValue startSplitRetryTimeout;
    private final TimeValue searchShardsOnlineTimeout;
    private final TimeValue splitStateAppliedTimeout;

    private final ConcurrentHashMap<IndexShard, StateMachine> onGoingSplits = new ConcurrentHashMap<>();

    public SplitTargetService(Settings settings, Client client, ClusterService clusterService, ReshardIndexService reshardIndexService) {
        this.client = client;
        this.clusterService = clusterService;
        this.reshardIndexService = reshardIndexService;
        this.startSplitRetryTimeout = START_SPLIT_RETRY_TIMEOUT.get(settings);
        this.searchShardsOnlineTimeout = RESHARD_SPLIT_SEARCH_SHARDS_ONLINE_TIMEOUT.get(settings);
        this.splitStateAppliedTimeout = RESHARD_SPLIT_SPLIT_STATE_APPLIED_TIMEOUT.get(settings);
    }

    public void startSplitTargetShardRecovery(IndexShard indexShard, IndexMetadata indexMetadata, ActionListener<Void> recoveryListener) {
        RecoverySource recoverySource = indexShard.recoveryState().getRecoverySource();
        // We should never see `EMPTY_STORE` recovery here because target shards should be using state copied from the source shard
        // meaning it's either `RESHARD_SPLIT` or `EXISTING_STORE`.
        // Target shards can also be relocated with `PEER`.
        if (recoverySource.getType() == RecoverySource.Type.EMPTY_STORE) {
            throw new IllegalStateException("Unexpected recovery type for resharding split target shard: " + recoverySource.getType());
        }

        ShardId shardId = indexShard.shardId();

        assert indexMetadata.getReshardingMetadata() != null;
        IndexReshardingState.Split splitState = indexMetadata.getReshardingMetadata().getSplit();

        long targetPrimaryTerm = indexShard.getOperationPrimaryTerm();
        final DiscoveryNode sourceNode = indexShard.recoveryState().getSourceNode();
        ShardId sourceShardId = new ShardId(shardId.getIndex(), splitState.sourceShard(shardId.id()));
        long sourcePrimaryTerm = indexMetadata.primaryTerm(sourceShardId.id());

        var splitMetadata = new SplitMetadata(shardId, sourceShardId, clusterService.localNode(), targetPrimaryTerm);

        switch (splitState.getTargetShardState(shardId.id())) {
            case CLONE -> {
                var stateMachine = new StateMachine(
                    splitMetadata,
                    indexShard,
                    () -> onGoingSplits.remove(indexShard),
                    reshardIndexService.getReshardMetrics(),
                    clusterService.threadPool().relativeTimeInMillisSupplier(),
                    new StateMachine.State.Clone(
                        new StateMachine.StartSplitRetries(clusterService.threadPool().relativeTimeInMillis(), startSplitRetryTimeout),
                        recoveryListener
                    )
                );
                if (registerStateMachine(indexShard, stateMachine)) {
                    stateMachine.run();
                } else {
                    recoveryListener.onFailure(new IndexShardClosedException(shardId));
                }
            }
            case HANDOFF -> {
                var stateMachine = new StateMachine(
                    splitMetadata,
                    indexShard,
                    () -> onGoingSplits.remove(indexShard),
                    reshardIndexService.getReshardMetrics(),
                    clusterService.threadPool().relativeTimeInMillisSupplier(),
                    new StateMachine.State.RecoveringInHandoff()
                );
                boolean registered = registerStateMachine(indexShard, stateMachine);
                recoveryListener.onResponse(null);
                if (registered) {
                    stateMachine.run();
                }
            }
            case SPLIT -> {
                var stateMachine = new StateMachine(
                    splitMetadata,
                    indexShard,
                    () -> onGoingSplits.remove(indexShard),
                    reshardIndexService.getReshardMetrics(),
                    clusterService.threadPool().relativeTimeInMillisSupplier(),
                    new StateMachine.State.RecoveringInSplit()
                );
                boolean registered = registerStateMachine(indexShard, stateMachine);
                recoveryListener.onResponse(null);
                if (registered) {
                    stateMachine.run();
                }
            }
            case DONE -> recoveryListener.onResponse(null);
        }
    }

    /// Registers the state machine unless the shard is already closed. Returns whether it was registered.
    ///
    /// If this runs first and observes `CLOSED`, [#cancelSplits(IndexShard)] may or may not have been called, so we handle the case
    /// where it already ran by not registering anything.
    /// If we don't observe `CLOSED`, we can rely on `cancelSplits` running later and removing the entry.
    private boolean registerStateMachine(IndexShard indexShard, StateMachine stateMachine) {
        return onGoingSplits.compute(
            indexShard,
            (shard, existing) -> shard.state() == IndexShardState.CLOSED ? existing : stateMachine
        ) == stateMachine;
    }

    public void acceptHandoff(IndexShard indexShard, TransportReshardSplitAction.Request handoffRequest, ActionListener<Void> listener) {
        var stateMachine = onGoingSplits.get(indexShard);
        if (stateMachine == null) {
            throw new IllegalStateException("No ongoing split for target shard" + handoffRequest.shardId());
        }

        stateMachine.acceptHandoff(handoffRequest, listener);
    }

    // only for tests
    void initializeSplitInCloneState(IndexShard indexShard, SplitMetadata splitMetadata) {
        assert Thread.currentThread().getName().startsWith("TEST-");
        registerStateMachine(
            indexShard,
            new StateMachine(
                splitMetadata,
                indexShard,
                () -> {},
                reshardIndexService.getReshardMetrics(),
                () -> 0,
                new StateMachine.State.Clone(new StateMachine.StartSplitRetries(0, TimeValue.MAX_VALUE), ActionListener.noop())
            )
        );
    }

    /// This function needs to be called from [org.elasticsearch.index.shard.IndexEventListener#afterIndexShardClosed] event handler
    /// so that it runs _after_ the shard state is set to `CLOSED` and not before.
    /// We rely on this fact in [#registerStateMachine(IndexShard, StateMachine)] to handle the possible race
    /// of this function and adding a new state machine to the `onGoingSplits` map.
    public void cancelSplits(IndexShard indexShard) {
        var stateMachine = onGoingSplits.remove(indexShard);
        if (stateMachine != null) {
            stateMachine.cancel();
        }
    }

    // visible for testing: a split should leave this empty once it completes or is cancelled. A stale entry pins the IndexShard it
    // is keyed by, so the shard is never collected even after it closes.
    Set<ShardId> getShardsWithOngoingSplits() {
        return onGoingSplits.keySet().stream().map(IndexShard::shardId).collect(Collectors.toSet());
    }

    // Metadata about a split that is currently in progress with an instance of the source shard.
    record ActiveSourceShard(DiscoveryNode sourceShardNode, long sourcePrimaryTerm) {}

    // Split metadata that doesn't change for this shard instance.
    record SplitMetadata(ShardId shardId, ShardId sourceShardId, DiscoveryNode localNode, long targetPrimaryTerm) {}

    private class StateMachine {
        private final SplitMetadata splitMetadata;
        private final IndexShard shard;
        private final Runnable onCompleted;

        private final AtomicBoolean cancelled;
        private final MetricsRecorder metricsRecorder;

        private State currentState;

        private StateMachine(
            SplitMetadata splitMetadata,
            IndexShard shard,
            Runnable onCompleted,
            ReshardMetrics reshardMetrics,
            LongSupplier nowInMillis,
            State initialState
        ) {
            this.splitMetadata = splitMetadata;
            this.shard = shard;
            this.onCompleted = onCompleted;
            this.cancelled = new AtomicBoolean(false);
            this.metricsRecorder = new MetricsRecorder(reshardMetrics, nowInMillis, this.cancelled);

            this.currentState = initialState;
        }

        /// Starts the state machine from the current state.
        void run() {
            advance(currentState);
        }

        /// The state machine is cancelled when its shard closes, but only once that close has started. The shard itself is marked as closed
        /// sooner than that, and the shard's cluster state listeners can run in between.
        private boolean isCancelled() {
            return cancelled.get() || shard.state() == IndexShardState.CLOSED;
        }

        void cancel() {
            final boolean isFirstCancellation = cancelled.getAndSet(true) == false;
            assert isFirstCancellation : "split for " + shard.shardId() + " cancelled twice";
            reshardIndexService.failAndStopTrackingSplit(shard, new IndexShardClosedException(shard.shardId()));
        }

        private synchronized void advance(State newState) {
            if (!validateStateTransition(newState)) return;
            this.currentState = newState;
            metricsRecorder.advance(newState);

            logStateTransition(newState);

            // We always fork to generic here since we get callbacks
            // from various places like cluster state update threads
            // and transport threads.
            clusterService.threadPool().generic().submit(() -> advanceInternal(newState));
        }

        private void logStateTransition(State newState) {
            if (newState instanceof State.Failed && cancelled.get()) {
                logger.info(
                    "Stopping split target shard state machine for shard {}, shard is closed. Will retry after recovery.",
                    shard.shardId()
                );
            } else {
                logger.info("Advancing split target shard state machine for shard {} to {}", shard.shardId(), newState);
            }
        }

        private void advanceInternal(State newState) {
            switch (newState) {
                case State.Clone clone -> {
                    resolveSourceShard(
                        new StartSplitListener<>(
                            clone.recoveryListener,
                            clone.retryState,
                            activeSourceShard -> new State.ReadyForSplit(clone.retryState, activeSourceShard, clone.recoveryListener)
                        )
                    );
                }
                case State.ReadyForSplit readyForSplit -> {
                    // Start split RPC won't complete until the entire handoff sequence completes.
                    // So we advance to waiting for handoff immediately after sending the start split request.
                    // Once the handoff successfully completes, start split completes as well
                    // which advances the state.

                    // Advance beforehand to eliminate theoretical race conditions with sending
                    // a request to the source shard.
                    // This can be done since the advancement does nothing anyway.
                    advance(new State.WaitingForHandoff(readyForSplit.retryState, readyForSplit.activeSourceShard));
                    initiateSplitWithSourceShard(
                        readyForSplit.activeSourceShard,
                        new StartSplitListener<>(
                            readyForSplit.recoveryListener,
                            readyForSplit.retryState,
                            ignored -> new State.StartSplitRpcComplete(readyForSplit.recoveryListener)
                        )
                    );
                }
                case State.WaitingForHandoff ignored -> {
                    /// This is a special state since we are not doing any action
                    /// but waiting for the source shard to initiate handoff.
                    /// It is useful to have this state for validation purposes.
                    /// See also [StateMachine#changeStateToHandoff].
                }
                case State.HandoffReceived handoffReceived -> {
                    changeStateToHandoff(handoffReceived);
                }
                case State.StartSplitRpcComplete startSplitRpcComplete -> {
                    // Now that the handoff is complete and we know that all needed data is present,
                    // we can proceed with recovery and start this shard.
                    // We fork recovery and then wait for the shard to be started
                    // so that it is ready for the next steps.
                    clusterService.threadPool().generic().submit(() -> startSplitRpcComplete.recoveryListener.onResponse(null));
                    waitForShardStarted(new StateAdvancingListener<>(startSplitRpcComplete, new State.Handoff()));
                }
                case State.Handoff ignored -> {
                    waitUntilSearchShardsAreOnline();
                }
                case State.SearchShardsOnline searchShardsOnline -> {
                    changeStateToSplit(new StateAdvancingListener<>(searchShardsOnline, new State.Split()));
                }
                case State.Split split -> {
                    awaitSplitApplied(new StateAdvancingListener<>(split, new State.SplitApplied()));
                }
                case State.SplitApplied splitApplied -> {
                    logger.info("notifying of split completion for target shard {}", shard.shardId());
                    reshardIndexService.notifySplitCompletion(shard);
                    deleteUnownedData(new StateAdvancingListener<>(splitApplied, new State.UnownedDataDeleted()));
                }
                case State.UnownedDataDeleted unownedDataDeleted -> {
                    changeStateToDone(new StateAdvancingListener<>(unownedDataDeleted, new State.Done()));
                }
                case State.Done ignored -> {
                    reshardIndexService.stopTrackingSplit(shard);
                    onCompleted.run();
                }

                case State.RecoveringInHandoff recoveringInHandoff -> {
                    waitForShardStarted(new StateAdvancingListener<>(recoveringInHandoff, new State.Handoff()));
                }
                case State.RecoveringInSplit recoveringInSplit -> {
                    /// We need to confirm that coordinators are aware of SPLIT state before proceeding further.
                    /// If they are not we can return stale search results from the source shard
                    /// after a successful write to the target shard and refresh.
                    /// Note that we'll automatically apply a refresh block since the current state is SPLIT,
                    /// see [ReshardIndexService#maybeAwaitSplit].
                    /// As such it's okay to have the shard start here before awaiting SPLIT application.
                    SubscribableListener.newForked(this::waitForShardStarted)
                        .andThen(this::awaitSplitApplied)
                        .addListener(new StateAdvancingListener<>(recoveringInSplit, new State.SplitApplied()));
                }
                case State.StartSplitFailed startSplitFailed -> {
                    // We need to release the recovery listener so that the shard can be properly closed.
                    // Technically it doesn't matter if this is a success or a failure since the shard is closed anyway.
                    // We only use failure here to short-circuit any following work (like reading shard state)
                    // which we know is unnecessary.
                    if (cancelled.get()) {
                        advance(new State.FailedInRecovery(startSplitFailed.exception, startSplitFailed.recoveryListener));
                        return;
                    }

                    /// We apply internal retries if the source shard is not started
                    /// or not ready (tearing down a previous start split attempt).
                    /// This is because allocation only performs a limited number of retries (at the time of writing)
                    /// when a shard fails in recovery (which is what happens if start split RPC fails).
                    /// As such it is possible to exhaust this limited number of retries if f.e. source shard is relocating.
                    /// Recovering from this state requires manual intervention which is of course undesirable.
                    ///
                    /// Note that there are scenarios that _require_ a new allocation of the target shard.
                    /// For example, target shard primary term has to be >= source shard primary term.
                    /// So if a source shard recovers on a higher primary term, target primary term should be updated as well
                    /// and that can only be done by the master node.
                    /// This is also the reason we don't retry forever.
                    ///
                    /// See corresponding source shard code in
                    /// [SplitSourceService#setupTargetShard(CancellableTask, ShardId, long, long, ActionListener)].
                    Throwable cause = ExceptionsHelper.unwrapCause(startSplitFailed.exception);

                    if (TransportActions.isShardNotAvailableException(cause)
                        // Connection errors and such.
                        || cause instanceof ActionTransportException) {
                        Optional<TimeValue> retryDelay = startSplitFailed.retryState.getRetryDelay(
                            clusterService.threadPool().relativeTimeInMillis()
                        );
                        if (retryDelay.isEmpty()) {
                            logger.debug("Start split failed with retryable error but retry timeout was reached", cause);
                            advance(new State.FailedInRecovery(startSplitFailed.exception, startSplitFailed.recoveryListener));
                            return;
                        }

                        logger.debug("Start split failed with retryable error, will retry.", cause);
                        // We need to re-resolve the source shard because it may have relocated
                        // or advanced its primary term.
                        clusterService.threadPool()
                            .schedule(
                                () -> advance(new State.Clone(startSplitFailed.retryState, startSplitFailed.recoveryListener)),
                                retryDelay.get(),
                                clusterService.threadPool().generic()
                            );
                    } else {
                        logger.info("Start split failed with non-retryable error: ", cause);
                        // Nothing we can do here, fail the recovery.
                        advance(new State.FailedInRecovery(startSplitFailed.exception, startSplitFailed.recoveryListener));
                    }
                }
                // Recovery listener should only be failed in this state in order to produce clear state transition logs
                // and metrics.
                case State.FailedInRecovery failedInRecovery -> {
                    failedInRecovery.recoveryListener.onFailure(failedInRecovery.exception);
                }
                case State.Failed failed -> {
                    if (cancelled.get()) {
                        return;
                    }

                    logger.warn("Failed to complete split target shard sequence", failed.exception);

                    if (failed.destinationState instanceof State.Split) {
                        reshardIndexService.notifySplitFailure(shard, failed.exception);
                        /// Transition to SPLIT failed in some unexpected way and now we are failing all incoming refresh requests
                        /// due to the `notifySplitFailure` call above.
                        /// There is nothing we can really do at this point to recover so we hope we can figure this out on recovery.
                        /// Note that this is technically impossible in the existing implementation since we retry all failures
                        /// until the shard is closed in [ChangeState#shouldRetry].
                        /// That logic may change though and then this becomes relevant.
                        /// For example if we discover that the primary term of the shard advanced when we tried to update state,
                        /// there is no reason to retry since we will never succeed.
                        shard.failShard("Failed to transition split target shard to SPLIT state", failed.exception);
                    }

                    // TODO consider failing shard here when appropriate
                    // currently we assume that if the split flow has failed, the engine failed as well.
                }
            }
        }

        private boolean handoffReceivedAfterFailure(State newState) {
            if (currentState.getClass() == State.FailedInRecovery.class && newState.getClass() == State.HandoffReceived.class) {
                return true;
            }
            return false;
        }

        // Return true if state transition is valid. If state transition is not valid, this method will either
        // throw an error or return false for certain known but invalid state transition requests. If false is
        // returned by this method, the caller should handle it appropriately.
        // Right now, there is only one case where false is returned and that is treated as a
        // NOOP by the caller i.e. state is never transitioned from FailedInRecovery -> HandoffReceived.
        private boolean validateStateTransition(State newState) {
            // After the target shard initiates split with the source shard, it can
            // receive a Handoff request (from the source) at the same time as a failure
            // occurs on the source shard (e.g. source node gets disconnected)
            // As a result, we might try to transition from FailedInRecovery -> HandoffReceived
            // which is not a valid state transition. We treat this state transition as a noop.
            // TODO fix
            if (handoffReceivedAfterFailure(newState)) {
                return false;
            }
            var validCurrentStates = newStateToValidCurrentStates.get(newState.getClass());
            if (validCurrentStates == null || validCurrentStates.contains(currentState.getClass()) == false) {
                // It's possible that this exception is not observed by anyone since we are inside a runnable on generic thread pool.
                // So we log as well.
                var message = String.format(Locale.ROOT, "Unexpected split target shard state transition %s -> %s", currentState, newState);
                logger.error(message);
                assert false : message;
                throw new IllegalStateException(message);
            }
            return true;
        }

        private static final Map<Class<? extends State>, Set<Class<? extends State>>> newStateToValidCurrentStates = new HashMap<>() {
            {
                // StartSplitFailed -> Clone happens during retries.
                put(State.Clone.class, Set.of(State.Clone.class, State.StartSplitFailed.class));
                put(State.ReadyForSplit.class, Set.of(State.Clone.class));
                put(State.WaitingForHandoff.class, Set.of(State.ReadyForSplit.class));
                // Target can receive multiple HANDOFF requests due to retries by source shard
                put(State.HandoffReceived.class, Set.of(State.WaitingForHandoff.class, State.HandoffReceived.class));
                put(State.StartSplitRpcComplete.class, Set.of(State.HandoffReceived.class));
                put(State.Handoff.class, Set.of(State.StartSplitRpcComplete.class, State.RecoveringInHandoff.class));
                put(State.SearchShardsOnline.class, Set.of(State.Handoff.class));
                put(State.Split.class, Set.of(State.SearchShardsOnline.class));
                put(State.SplitApplied.class, Set.of(State.Split.class, State.RecoveringInSplit.class));
                put(State.UnownedDataDeleted.class, Set.of(State.SplitApplied.class));
                put(State.Done.class, Set.of(State.UnownedDataDeleted.class));

                put(State.RecoveringInHandoff.class, Set.of(State.RecoveringInHandoff.class));
                put(State.RecoveringInSplit.class, Set.of(State.RecoveringInSplit.class));

                // See `changeStateToHandoff` for details on `HandoffReceived` -> `StartSplitFailed`.
                put(State.StartSplitFailed.class, Set.of(State.Clone.class, State.WaitingForHandoff.class, State.HandoffReceived.class));

                put(State.FailedInRecovery.class, Set.of(State.StartSplitFailed.class));
                put(
                    State.Failed.class,
                    Set.of(
                        State.SearchShardsOnline.class,
                        State.Split.class,
                        State.SplitApplied.class,
                        State.UnownedDataDeleted.class,
                        State.RecoveringInSplit.class
                    )
                );
            }
        };

        /// All the possible states that a target shard state machine can be in.
        /// State transitions are always performed in the order of definition except for the failure states.
        /// E.g. we always transition SearchShardsOnline -> Split.
        sealed interface State permits State.Clone, State.ReadyForSplit, State.WaitingForHandoff, State.HandoffReceived,
            State.StartSplitRpcComplete, State.Handoff, State.SearchShardsOnline, State.Split, State.SplitApplied, State.UnownedDataDeleted,
            State.Done, State.RecoveringInHandoff, State.RecoveringInSplit, State.StartSplitFailed, State.FailedInRecovery, State.Failed {
            // Corresponds to CLONE state of a target shard.
            record Clone(StartSplitRetries retryState, ActionListener<Void> recoveryListener) implements State {}

            record ReadyForSplit(StartSplitRetries retryState, ActiveSourceShard activeSourceShard, ActionListener<Void> recoveryListener)
                implements
                    State {}

            record WaitingForHandoff(StartSplitRetries retryState, ActiveSourceShard activeSourceShard) implements State {}

            record HandoffReceived(ActiveSourceShard activeSourceShard, ActionListener<Void> handoffRpcListener) implements State {}

            record StartSplitRpcComplete(ActionListener<Void> recoveryListener) implements State {}

            // Corresponds to HANDOFF state of a target shard.
            record Handoff() implements State {}

            record SearchShardsOnline() implements State {}

            // SPLIT state was applied on the master node.
            // It is not necessarily applied on other nodes in the cluster.
            record Split() implements State {}

            record SplitApplied() implements State {}

            record UnownedDataDeleted() implements State {}

            // Corresponds to DONE state of a target shard.
            record Done() implements State {}

            // States for resuming the state machine from a particular target shard state.

            // Special state for when the target shard recovers in HANDOFF state.
            // This state simply skips the logic to notify the recovery listener
            // since the caller already does it.
            record RecoveringInHandoff() implements State {}

            // Special state for when the target shard recovers in SPLIT state.
            // We need the shard to be started before moving to State.Split
            // since it is required for delete unowned logic to work.
            record RecoveringInSplit() implements State {}

            record StartSplitFailed(StartSplitRetries retryState, Exception exception, ActionListener<Void> recoveryListener)
                implements
                    State {}

            // We distinguish these failed states because the shard state is different
            // (it's not started during recovery) and failure handling is different as a result.
            record FailedInRecovery(Exception exception, ActionListener<Void> recoveryListener) implements State {}

            /// @param destinationState next state that we attempted to transition to but failed
            record Failed(Exception exception, State destinationState) implements State {}
        }

        void acceptHandoff(TransportReshardSplitAction.Request handoffRequest, ActionListener<Void> handoffRpcListener) {
            var state = currentState;
            // TODO this is weird, we should always be able to perform this validation
            // but ActiveSourceShard in HandoffReceived state can be stale (??)
            // probably need to to model handoff failure better

            if (state instanceof State.WaitingForHandoff waitingForHandoff) {
                ActiveSourceShard activeSourceShard = waitingForHandoff.activeSourceShard;

                if (handoffRequest.shardId().equals(splitMetadata.shardId) == false
                    || handoffRequest.targetNode().equals(splitMetadata.localNode) == false
                    || handoffRequest.targetPrimaryTerm() != splitMetadata.targetPrimaryTerm
                    || handoffRequest.sourceNode().equals(activeSourceShard.sourceShardNode) == false
                    || handoffRequest.sourcePrimaryTerm() != activeSourceShard.sourcePrimaryTerm) {
                    logger.debug("Received a stale split handoff request: {}", handoffRequest);
                    throw new IllegalStateException("Received a stale split handoff request for shard " + handoffRequest.shardId());
                }

                advance(new State.HandoffReceived(waitingForHandoff.activeSourceShard, handoffRpcListener));
            } else if (state instanceof State.HandoffReceived handoffReceived) {
                // It's possible we are already in `HandoffReceived` due to retries that the source shard performs.
                // If that is the case it means that we previously failed to perform the state transition.
                // TODO just do nothing???
                advance(new State.HandoffReceived(handoffReceived.activeSourceShard, handoffReceived.handoffRpcListener));
            } else {
                throw new IllegalStateException("Received an unexpected handoff request from the source shard");
            }
        }

        private void resolveSourceShard(ActionListener<ActiveSourceShard> listener) {
            // Resolve current nodeId of the source shard.
            // It can change between retries due to failures or relocations.
            Predicate<ClusterState> predicate = state -> {
                if (isCancelled()) {
                    return true;
                }

                return getStartedSourceShardRouting(state, splitMetadata).isPresent();
            };

            // We specifically use waitForState because it won't wait for the next cluster state version
            // if predicate already matches (which is most of the time).
            ClusterStateObserver.waitForState(
                clusterService,
                clusterService.threadPool().getThreadContext(),
                new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        if (isCancelled()) {
                            // It's very important to not simply stop here since that leaks recovery listener.
                            listener.onFailure(new CancellationException());
                            return;
                        }

                        // Both of these have to be present, it's checked in the predicate.
                        ShardRouting routing = getStartedSourceShardRouting(state, splitMetadata).get();
                        IndexMetadata indexMetadata = state.metadata().findIndex(splitMetadata.shardId.getIndex()).get();

                        var result = new ActiveSourceShard(
                            state.nodes().get(routing.currentNodeId()),
                            indexMetadata.primaryTerm(splitMetadata.sourceShardId.id())
                        );
                        listener.onResponse(result);
                    }

                    @Override
                    public void onClusterServiceClose() {
                        listener.onFailure(new NodeClosedException(clusterService.localNode()));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        // There is no timeout except for cancellation logic.
                    }
                },
                predicate,
                // We can't proceed with this split without being able to reach the source shard.
                // So we keep trying.
                null,
                logger
            );
        }

        private static Optional<ShardRouting> getStartedSourceShardRouting(ClusterState clusterState, SplitMetadata splitMetadata) {
            Index index = splitMetadata.shardId.getIndex();

            // We return empty on deleted project/index since the shard should be closed soon as a result of the deletion,
            // and we'll stop retries via the cancelled flag.
            return clusterState.metadata().lookupProject(index).flatMap(pm -> {
                var sourceShardRouting = clusterState.routingTable(pm.id()).shardRoutingTable(splitMetadata.sourceShardId).primaryShard();

                return Optional.ofNullable(sourceShardRouting);
            })
                // If it is relocating, just wait it out.
                .filter(sr -> sr.relocating() == false)
                .filter(ShardRouting::started);
        }

        private void initiateSplitWithSourceShard(ActiveSourceShard activeSourceShard, ActionListener<Void> listener) {
            var request = new TransportReshardSplitAction.SplitRequest(
                splitMetadata.shardId,
                activeSourceShard.sourceShardNode,
                splitMetadata.localNode,
                activeSourceShard.sourcePrimaryTerm,
                splitMetadata.targetPrimaryTerm
            );

            client.execute(TransportReshardSplitAction.TYPE, request, listener.map(ignored -> null));
        }

        private void changeStateToHandoff(State.HandoffReceived handoffReceived) {
            SplitStateRequest splitStateRequest = new SplitStateRequest(
                splitMetadata.shardId,
                IndexReshardingState.Split.TargetShardState.HANDOFF,
                handoffReceived.activeSourceShard.sourcePrimaryTerm,
                splitMetadata.targetPrimaryTerm
            );
            // Result of this operation does not directly change the state.
            // Instead, once the source shard receives a successful response to the handoff request
            // it completes the start split request initiated in State.Clone (see #initiateSplitWithSourceShard)
            // and that advances the state of the state machine.
            // Because of that we can simply use handoffRpcListener here.
            // We could model this as an explicit state, but it doesn't really improve anything.
            // It can be done if the logic becomes more complex than simply chaining two operations.
            //
            // Note that it is possible that the source shard fails start split request at any time during the logic in this method
            // (e.g. the node disconnects).
            // In that case we'll transition to `StartSplitFailed` and handle it as usual.
            // Since this method doesn't perform state transitions, there is no conflict here.
            client.execute(
                TransportUpdateSplitTargetShardStateAction.TYPE,
                splitStateRequest,
                handoffReceived.handoffRpcListener.map(ignored -> null)
            );
        }

        private void waitUntilSearchShardsAreOnline() {
            ShardId shardId = splitMetadata.shardId();

            ClusterStateObserver.waitForState(
                clusterService,
                clusterService.threadPool().getThreadContext(),
                new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        if (isCancelled()) {
                            return;
                        }
                        final Index index = shardId.getIndex();
                        final ProjectMetadata projectMetadata = state.metadata().lookupProject(index).orElse(null);
                        if (projectMetadata != null) {
                            if (newPrimaryTerm(index, projectMetadata, shardId, splitMetadata.targetPrimaryTerm()) == false) {
                                assert isShardGreen(state, projectMetadata, shardId);
                                advance(new State.SearchShardsOnline());
                            }
                        }
                    }

                    @Override
                    public void onClusterServiceClose() {
                        // Nothing to do, shard will be closed when the node closes.
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        if (isCancelled()) {
                            return;
                        }
                        // After the timeout we proceed to SPLIT anyway.
                        // The timeout is just best effort to ensure search shards are running to prevent downtime.
                        advance(new State.SearchShardsOnline());
                    }
                },
                // A closed shard has nothing to wait for
                state -> isCancelled() || searchShardsOnlineOrNewPrimaryTerm(state, shardId, splitMetadata.targetPrimaryTerm()),
                searchShardsOnlineTimeout,
                logger
            );
        }

        private void changeStateToSplit(ActionListener<Void> listener) {
            ChangeState changeState = new ChangeState(
                clusterService,
                client,
                splitMetadata,
                IndexReshardingState.Split.TargetShardState.SPLIT,
                cancelled,
                listener.map(ignored -> null)
            );
            changeState.run();
        }

        private void awaitSplitApplied(ActionListener<Void> listener) {
            SubscribableListener.<ClusterState>newForked(
                l -> ClusterStateObserver.waitForState(
                    clusterService,
                    clusterService.threadPool().getThreadContext(),
                    new ClusterStateObserver.Listener() {
                        @Override
                        public void onNewClusterState(ClusterState state) {
                            if (cancelled.get()) {
                                l.onFailure(new CancellationException());
                                return;
                            }

                            Optional<IndexReshardingMetadata> reshardingMetadata = getReshardingMetadata(state, splitMetadata.shardId);
                            if (reshardingMetadata.isEmpty()) {
                                // If there is no metadata anymore, there is nothing for us to do.
                                // An example situation is a relocation that happens during final steps of the workflow
                                // (e.g. moving to DONE).
                                // The relocated shard is able to move to DONE since the primary term doesn't change during relocation.
                                // Then during recovery of the relocation target shard we'll see that there is no metadata.
                                // TODO this will be fixed with the grace period before moving source shards to DONE.
                                l.onFailure(new CancellationException());
                            } else {
                                l.onResponse(state);
                            }
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
                        Optional<IndexReshardingMetadata> reshardingMetadata = getReshardingMetadata(state, splitMetadata.shardId);
                        return reshardingMetadata.isEmpty()
                            || reshardingMetadata.get()
                                .getSplit()
                                .targetStateAtLeast(splitMetadata.shardId.id(), IndexReshardingState.Split.TargetShardState.SPLIT);
                    },
                    // No timeout.
                    // We know we have just submitted this cluster state update,
                    // so we will either see it shortly or we are not in the cluster anymore
                    // and there is probably another instance of the shard being allocated.
                    // In the latter case we may as well wait since we are not blocking any user operations.
                    null,
                    logger
                )
            ).<Void>andThen((l, state) -> {
                var awaitSplitStateApplied = new AwaitSplitStateAppliedAction(state, l);
                awaitSplitStateApplied.run();
            }).addListener(listener);
        }

        private static Optional<IndexReshardingMetadata> getReshardingMetadata(ClusterState state, ShardId shardId) {
            return state.metadata()
                .findIndex(shardId.getIndex())
                .flatMap(indexMetadata -> Optional.ofNullable(indexMetadata.getReshardingMetadata()));
        }

        private void deleteUnownedData(ActionListener<Void> listener) {
            var deleteUnowned = new DeleteUnowned(clusterService, reshardIndexService, shard, cancelled, listener);
            deleteUnowned.run();
        }

        private void changeStateToDone(ActionListener<Void> listener) {
            ChangeState changeState = new ChangeState(
                clusterService,
                client,
                splitMetadata,
                IndexReshardingState.Split.TargetShardState.DONE,
                cancelled,
                listener.map(ignored -> null)
            );
            changeState.run();
        }

        private void waitForShardStarted(ActionListener<Void> listener) {
            // We don't really need the cluster state as can be seen below,
            // but we know that if the shard is not currently started one of the cluster state updates will start it.
            // So we use the observer as a way to deterministically wait for this to happen
            // (as opposed to f.e. sleeping).
            Predicate<ClusterState> predicate = state -> {
                if (cancelled.get()) {
                    return true;
                }

                // We don't look at CLOSED since it will trigger cancellation flow.
                return shard.state() == IndexShardState.STARTED;
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

                        listener.onResponse(null);
                    }

                    @Override
                    public void onClusterServiceClose() {
                        // Nothing to do, shard will be closed when the node closes.
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        // There is no timeout
                    }
                },
                predicate,
                null,
                logger
            );
        }

        private static class SplitCancelledException extends CancellationException {

        }

        private static class StartSplitRetries {
            private final long startedMillis;
            private final TimeValue retryTimeout;

            private final Iterator<TimeValue> backoffPolicyIterator;

            StartSplitRetries(long startedMillis, TimeValue retryTimeout) {
                this.startedMillis = startedMillis;
                this.retryTimeout = retryTimeout;

                this.backoffPolicyIterator = BackoffPolicy.linearBackoff(
                    TimeValue.timeValueMillis(500),
                    Integer.MAX_VALUE,
                    TimeValue.timeValueSeconds(5)
                ).iterator();
            }

            Optional<TimeValue> getRetryDelay(long currentTimeMillis) {
                if (currentTimeMillis > startedMillis + retryTimeout.millis()) {
                    return Optional.empty();
                }

                assert backoffPolicyIterator.hasNext(); // since we don't limit the number of retries
                return Optional.of(backoffPolicyIterator.next());
            }

        }

        /// Listener used by multi-state start split logic.
        /// All states should transition to [State.StartSplitFailed] on failure
        /// to implement retries and avoid leaking recovery listener.
        private class StartSplitListener<T> implements ActionListener<T> {
            private final ActionListener<Void> recoveryListener;
            private final StartSplitRetries retryState;
            private final Function<T, State> nextStateTransition;

            private StartSplitListener(
                ActionListener<Void> recoveryListener,
                StartSplitRetries retryState,
                Function<T, State> nextStateTransition
            ) {
                this.recoveryListener = recoveryListener;
                this.retryState = retryState;
                this.nextStateTransition = nextStateTransition;
            }

            @Override
            public void onResponse(T result) {
                advance(nextStateTransition.apply(result));
            }

            @Override
            public void onFailure(Exception e) {
                advance(new State.StartSplitFailed(retryState, e, recoveryListener));
            }
        }

        private class StateAdvancingListener<T> implements ActionListener<T> {
            private final State currentState;
            private final State nextState;

            private StateAdvancingListener(State currentState, State nextState) {
                this.currentState = currentState;
                this.nextState = nextState;
            }

            @Override
            public void onResponse(T t) {
                advance(nextState);
            }

            @Override
            public void onFailure(Exception e) {
                advance(new State.Failed(e, currentState));
            }
        }

        private static class MetricsRecorder {
            private record TimestampEntry(Class<? extends State> state, long timestampMillis) {}

            private final Map<Class<? extends State>, Runnable> targetStates;
            private final LongSupplier nowInMillis;
            private final List<TimestampEntry> timestamps;

            MetricsRecorder(ReshardMetrics reshardMetrics, LongSupplier nowInMillis, AtomicBoolean cancelled) {
                this.nowInMillis = nowInMillis;
                this.timestamps = new ArrayList<>();
                this.targetStates = new HashMap<>() {
                    {
                        put(State.Handoff.class, () -> {
                            deltaFromState(State.Clone.class).ifPresent(
                                delta -> reshardMetrics.targetCloneDurationHistogram().record(delta / 1000.0)
                            );
                        });
                        put(State.Split.class, () -> {
                            deltaFromState(State.Handoff.class).ifPresent(
                                delta -> reshardMetrics.targetHandoffDurationHistogram().record(delta)
                            );
                        });
                        put(State.Done.class, () -> {
                            deltaFromState(State.Split.class).ifPresent(
                                delta -> reshardMetrics.targetSplitDurationHistogram().record(delta)
                            );
                        });
                        put(State.Failed.class, () -> {
                            if (cancelled.get() == false) {
                                reshardMetrics.targetShardFailureCounter().increment();
                            }
                        });
                        put(State.FailedInRecovery.class, reshardMetrics.targetShardRecoveryFailureCounter()::increment);
                    }
                };
            }

            private OptionalLong deltaFromState(Class<? extends State> lastState) {
                long currentTimestamp = timestamps.getLast().timestampMillis();
                for (int i = timestamps.size() - 2; i >= 0; i--) {
                    TimestampEntry entry = timestamps.get(i);
                    if (entry.state().equals(lastState)) {
                        return OptionalLong.of(currentTimestamp - entry.timestampMillis());
                    }
                }
                return OptionalLong.empty();
            }

            void advance(State newState) {
                timestamps.add(new TimestampEntry(newState.getClass(), nowInMillis.getAsLong()));
                Runnable action = targetStates.get(newState.getClass());
                if (action != null) {
                    action.run();
                }
            }
        }
    }

    // visible for tests
    RetryableAction<Void> createInitiateSplitWithSourceShardAction(
        SplitMetadata splitMetadata,
        ActiveSourceShard activeSourceShard,
        AtomicBoolean cancelled,
        TimeValue retryTimeout,
        ActionListener<Void> listener
    ) {
        return new InitiateSplitWithSourceShardAction(
            clusterService,
            client,
            splitMetadata,
            activeSourceShard,
            cancelled,
            retryTimeout,
            listener
        );
    }

    private static class InitiateSplitWithSourceShardAction extends RetryableAction<Void> {
        private final ClusterService clusterService;
        private final Client client;
        private final SplitMetadata splitMetadata;
        private final ActiveSourceShard activeSourceShard;
        private final AtomicBoolean cancelled;

        InitiateSplitWithSourceShardAction(
            ClusterService clusterService,
            Client client,
            SplitMetadata splitMetadata,
            ActiveSourceShard activeSourceShard,
            AtomicBoolean cancelled,
            TimeValue retryTimeout,
            ActionListener<Void> listener
        ) {
            super(
                logger,
                clusterService.threadPool(),
                TimeValue.timeValueMillis(500),
                TimeValue.timeValueSeconds(5),
                // If we haven't made progress in this much time, fail back to allocator.
                // Maybe the source shard is assigned to a different node now.
                retryTimeout,
                listener,
                clusterService.threadPool().generic()
            );
            this.clusterService = clusterService;
            this.client = client;
            this.splitMetadata = splitMetadata;
            this.activeSourceShard = activeSourceShard;
            this.cancelled = cancelled;
        }

        @Override
        public void tryAction(ActionListener<Void> listener) {

            var request = new TransportReshardSplitAction.SplitRequest(
                splitMetadata.shardId,
                activeSourceShard.sourceShardNode,
                splitMetadata.localNode,
                activeSourceShard.sourcePrimaryTerm,
                splitMetadata.targetPrimaryTerm
            );

            client.execute(TransportReshardSplitAction.TYPE, request, listener.map(ignored -> null));
        }

        @Override
        public boolean shouldRetry(Exception e) {
            if (cancelled.get()) {
                // TODO cancelled exception
                return false;
            }

            /// We apply internal retries if the source shard is not started or not ready (tearing down a previous start split attempt).
            /// This is because allocation only performs a limited number of retries (at the time of writing)
            /// when a shard fails in recovery (which is what happens if start split RPC fails).
            /// As such it is possible to exhaust this limited number of retries if f.e. source shard is relocating.
            /// Recovering from this state requires manual intervention which is of course undesirable.
            ///
            /// Note that there are scenarios that _require_ a new allocation of the target shard.
            /// For example, target shard primary term has to be >= source shard primary term.
            /// So if a source shard recovers on a higher primary term, target primary term should be updated as well
            /// and that can only be done by the master node.
            /// This is also the reason we don't retry forever.
            ///
            /// See corresponding source shard code in
            /// [SplitSourceService#setupTargetShard(CancellableTask, ShardId, long, long, ActionListener)].
            Throwable cause = ExceptionsHelper.unwrapCause(e);

            return (TransportActions.isShardNotAvailableException(cause)
                // Connection errors and such.
                || cause instanceof ActionTransportException);
        }

        private boolean isCancelled() {
            return cancelled.get();
        }
    }

    private static class ChangeState extends RetryableAction<ActionResponse> {
        private final Client client;

        private final SplitMetadata splitMetadata;
        private final IndexReshardingState.Split.TargetShardState state;
        private final AtomicBoolean cancelled;

        private ChangeState(
            ClusterService clusterService,
            Client client,
            SplitMetadata splitMetadata,
            IndexReshardingState.Split.TargetShardState state,
            AtomicBoolean cancelled,
            ActionListener<ActionResponse> listener
        ) {
            super(
                logger,
                clusterService.threadPool(),
                TimeValue.timeValueMillis(10),
                TimeValue.timeValueSeconds(5),
                TimeValue.MAX_VALUE,
                listener,
                clusterService.threadPool().generic()
            );
            this.client = client;
            this.splitMetadata = splitMetadata;
            this.state = state;
            this.cancelled = cancelled;
        }

        @Override
        public void tryAction(ActionListener<ActionResponse> listener) {
            if (cancelled.get()) {
                listener.onFailure(new CancellationException());
                return;
            }

            // TODO this 0 is really ugly
            var request = new SplitStateRequest(splitMetadata.shardId(), state, 0, splitMetadata.targetPrimaryTerm());
            client.execute(TransportUpdateSplitTargetShardStateAction.TYPE, request, listener);
        }

        @Override
        public boolean shouldRetry(Exception e) {
            // Retry forever unless canceled (shard is closed).
            // TODO consider not retrying IllegalStateException or introducing a specific exception for cases
            // when we know that split needs to be restarted
            boolean isCancelled = cancelled.get();
            return isCancelled == false;
        }
    }

    // visible for tests
    RetryableAction<Void> createDeleteUnownedAction(IndexShard shard, AtomicBoolean cancelled, ActionListener<Void> listener) {
        return new DeleteUnowned(clusterService, reshardIndexService, shard, cancelled, listener);
    }

    // visible for tests
    RetryableAction<ActionResponse> createChangeStateAction(
        SplitMetadata splitMetadata,
        IndexReshardingState.Split.TargetShardState state,
        AtomicBoolean cancelled,
        ActionListener<ActionResponse> listener
    ) {
        return new ChangeState(clusterService, client, splitMetadata, state, cancelled, listener);
    }

    // visible for tests
    RetryableAction<Void> createAwaitSplitStateAppliedAction(ClusterState stateWithSplitApplied, ActionListener<Void> listener) {
        return new AwaitSplitStateAppliedAction(stateWithSplitApplied, listener);
    }

    private class AwaitSplitStateAppliedAction extends RetryableAction<Void> {
        private final long clusterStateVersion;
        private final ActionListener<Void> finalListener;

        private HashSet<DiscoveryNode> nodes;

        AwaitSplitStateAppliedAction(ClusterState stateWithSplitApplied, ActionListener<Void> listener) {
            super(
                logger,
                clusterService.threadPool(),
                // We are fairly aggressive because the refresh is blocked at this point.
                TimeValue.timeValueMillis(200), // initialDelay
                TimeValue.timeValueSeconds(1), // maxDelayBound
                /// See [RESHARD_SPLIT_SPLIT_STATE_APPLIED_TIMEOUT].
                splitStateAppliedTimeout, // timeoutValue
                /// See [#onFinished()].
                ActionListener.noop(),
                clusterService.threadPool().generic()
            );

            this.clusterStateVersion = stateWithSplitApplied.version();
            this.finalListener = listener;
            this.nodes = new HashSet<>(stateWithSplitApplied.nodes().getAllNodes());
        }

        @Override
        public void tryAction(ActionListener<Void> listener) {
            client.execute(
                TransportAwaitClusterStateVersionAppliedAction.TYPE,
                new AwaitClusterStateVersionAppliedRequest(
                    clusterStateVersion,
                    splitStateAppliedTimeout,
                    nodes.toArray(new DiscoveryNode[0])
                ),
                listener.delegateFailure((l, response) -> {
                    if (response.hasFailures()) {
                        // We only need to retry on the nodes that had failures.
                        // But also these nodes may be gone and there may be new nodes in the cluster.
                        // We expect new nodes joining the cluster to get a fresh version of the cluster state before serving requests.
                        // But in some cases that is not true (e.g. a node drops from the cluster and re-joins)
                        // so we'll include new nodes for completeness.
                        // Note that this is still best-effort since a node can be added right after this line.
                        // This should be rare though.
                        // A proper fix would be to not allow nodes that don't have a recent enough cluster state
                        // to execute searches.
                        var state = clusterService.state();

                        var failedNodes = response.failures().stream().map(FailedNodeException::nodeId).collect(Collectors.toSet());
                        var newNodes = new HashSet<DiscoveryNode>();

                        // By iterating though the new state we'll automatically skip nodes that were present
                        // in the previous cluster state but are not in this one.
                        for (var node : state.nodes().getAllNodes()) {
                            if (nodes.contains(node) == false || failedNodes.contains(node.getId())) {
                                newNodes.add(node);
                            }
                        }

                        nodes = newNodes;

                        l.onFailure(new PartialFailureException());
                    } else {
                        l.onResponse(null);
                    }
                })
            );
        }

        @Override
        public boolean shouldRetry(Exception e) {
            return true;
        }

        @Override
        public void onFinished() {
            // We use `RetryableAction` only to perform retries.
            // We proceed even if there were failures since we don't want to block refreshes for a long time.
            finalListener.onResponse(null);
        }

        private static class PartialFailureException extends RuntimeException {}
    }

    // We need retries for things like relocations that can impact delete unowned logic
    // by marking this shard non-primary.
    // We can't predict all possible exceptions that can happen here
    // so we'll retry until eventually the shard is closed.
    // If something unexpected happens on this code path it would (likely) fail the engine and lead to shard being closed anyway.
    private static class DeleteUnowned extends RetryableAction<Void> {
        private final ReshardIndexService reshardIndexService;
        private final IndexShard shard;
        private final AtomicBoolean cancelled;

        DeleteUnowned(
            ClusterService clusterService,
            ReshardIndexService reshardIndexService,
            IndexShard shard,
            AtomicBoolean cancelled,
            ActionListener<Void> listener
        ) {
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
            this.reshardIndexService = reshardIndexService;
            this.shard = shard;
            this.cancelled = cancelled;
        }

        @Override
        public void tryAction(ActionListener<Void> listener) {
            if (cancelled.get()) {
                listener.onFailure(new CancellationException());
                return;
            }
            // Work on this action's own shard instance. If the shard has been closed and replaced (e.g. by a restore over the index) the
            // shard with the same id must not be touched, and this fails because the shard is closed.
            reshardIndexService.deleteUnownedDocuments(shard, listener);
        }

        @Override
        public boolean shouldRetry(Exception e) {
            if (cancelled.get()) {
                return false;
            }

            // No reason to retry if the shard/engine is closed.
            return switch (e) {
                case AlreadyClosedException ignored -> false;
                case IndexShardClosedException ignored -> false;
                default -> true;
            };
        }
    }

    // package-private for testing
    static boolean searchShardsOnlineOrNewPrimaryTerm(ClusterState state, ShardId shardId, long primaryTerm) {
        final Index index = shardId.getIndex();
        final ProjectMetadata projectMetadata = state.metadata().lookupProject(index).orElse(null);
        if (projectMetadata == null) {
            logger.debug("index not found while checking if shard {} is search shards online", shardId);
            return false;
        }

        return newPrimaryTerm(index, projectMetadata, shardId, primaryTerm) || isShardGreen(state, projectMetadata, shardId);
    }

    // package-private for testing
    static boolean newPrimaryTerm(Index index, ProjectMetadata projectMetadata, ShardId shardId, long primaryTerm) {
        final var indexMetadata = projectMetadata.index(index);
        if (indexMetadata == null || shardId.id() >= indexMetadata.getNumberOfShards()) {
            // The shard is gone, e.g. a restore replaced the index with one that has fewer shards. It is not the shard we are waiting for.
            return true;
        }
        long currentPrimaryTerm = indexMetadata.primaryTerm(shardId.id());
        return currentPrimaryTerm > primaryTerm;
    }

    private static boolean isShardGreen(ClusterState state, ProjectMetadata projectMetadata, ShardId shardId) {
        final Index index = shardId.getIndex();

        var indexRoutingTable = state.routingTable(projectMetadata.id()).index(index);
        if (indexRoutingTable == null) {
            // This should not be possible since https://github.com/elastic/elasticsearch/issues/33888
            logger.warn("found no index routing for {} but found it in metadata", shardId);
            assert false;
            // better safe than sorry in this case.
            return false;
        }
        var shardRoutingTable = indexRoutingTable.shard(shardId.id());
        if (shardRoutingTable == null) {
            // As above, this can be a state in which the shard no longer exists, e.g. after a restore over the index
            return false;
        }
        return new ClusterShardHealth(shardId.getId(), shardRoutingTable).getStatus() == ClusterHealthStatus.GREEN;
    }
}
