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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.state.AwaitClusterStateVersionAppliedRequest;
import org.elasticsearch.action.admin.cluster.state.TransportAwaitClusterStateVersionAppliedAction;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.action.support.SubscribableListener;
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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
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

    private static final Logger logger = LogManager.getLogger(SplitTargetService.class);

    private final Client client;
    private final ClusterService clusterService;
    private final ReshardIndexService reshardIndexService;
    private final TimeValue searchShardsOnlineTimeout;
    private final TimeValue splitStateAppliedTimeout;

    private final ConcurrentHashMap<IndexShard, StateMachine> onGoingSplits = new ConcurrentHashMap<>();

    public SplitTargetService(Settings settings, Client client, ClusterService clusterService, ReshardIndexService reshardIndexService) {
        this.client = client;
        this.clusterService = clusterService;
        this.reshardIndexService = reshardIndexService;
        this.searchShardsOnlineTimeout = RESHARD_SPLIT_SEARCH_SHARDS_ONLINE_TIMEOUT.get(settings);
        this.splitStateAppliedTimeout = RESHARD_SPLIT_SPLIT_STATE_APPLIED_TIMEOUT.get(settings);
    }

    public void startSplitTargetShardRecovery(IndexShard indexShard, IndexMetadata indexMetadata, ActionListener<Void> recoveryListener) {
        ShardId shardId = indexShard.shardId();

        assert indexMetadata.getReshardingMetadata() != null;
        IndexReshardingState.Split splitMetadata = indexMetadata.getReshardingMetadata().getSplit();

        long targetPrimaryTerm = indexShard.getOperationPrimaryTerm();
        final DiscoveryNode sourceNode = indexShard.recoveryState().getSourceNode();
        long sourcePrimaryTerm = indexMetadata.primaryTerm(splitMetadata.sourceShard(shardId.id()));

        var split = new Split(shardId, sourceNode, clusterService.localNode(), sourcePrimaryTerm, targetPrimaryTerm);

        switch (splitMetadata.getTargetShardState(shardId.id())) {
            case CLONE -> {
                var stateMachine = new StateMachine(
                    split,
                    indexShard,
                    () -> onGoingSplits.remove(indexShard),
                    new StateMachine.State.Clone(recoveryListener)
                );
                onGoingSplits.put(indexShard, stateMachine);
                stateMachine.run();
            }
            case HANDOFF -> {
                var stateMachine = new StateMachine(
                    split,
                    indexShard,
                    () -> onGoingSplits.remove(indexShard),
                    new StateMachine.State.RecoveringInHandoff()
                );
                onGoingSplits.put(indexShard, stateMachine);
                recoveryListener.onResponse(null);
                stateMachine.run();
            }
            case SPLIT -> {
                var stateMachine = new StateMachine(
                    split,
                    indexShard,
                    () -> onGoingSplits.remove(indexShard),
                    new StateMachine.State.RecoveringInSplit()
                );
                onGoingSplits.put(indexShard, stateMachine);
                recoveryListener.onResponse(null);
                stateMachine.run();
            }
            case DONE -> recoveryListener.onResponse(null);
        }
    }

    public void acceptHandoff(IndexShard indexShard, TransportReshardSplitAction.Request handoffRequest, ActionListener<Void> listener) {
        var stateMachine = onGoingSplits.get(indexShard);
        if (stateMachine == null) {
            throw new IllegalStateException("No ongoing split for target shard" + handoffRequest.shardId());
        }

        var splitFromRequest = new Split(
            handoffRequest.shardId(),
            handoffRequest.sourceNode(),
            handoffRequest.targetNode(),
            handoffRequest.sourcePrimaryTerm(),
            handoffRequest.targetPrimaryTerm()
        );

        stateMachine.acceptHandoff(splitFromRequest, listener);
    }

    // only for tests
    void initializeSplitInCloneState(IndexShard indexShard, Split split) {
        assert Thread.currentThread().getName().startsWith("TEST-");
        onGoingSplits.put(indexShard, new StateMachine(split, indexShard, () -> {}, new StateMachine.State.Clone(ActionListener.noop())));
    }

    public void cancelSplits(IndexShard indexShard) {
        var stateMachine = onGoingSplits.get(indexShard);
        if (stateMachine != null) {
            stateMachine.cancel();
            onGoingSplits.remove(indexShard);
        }
    }

    record Split(ShardId shardId, DiscoveryNode sourceNode, DiscoveryNode targetNode, long sourcePrimaryTerm, long targetPrimaryTerm) {}

    private class StateMachine {
        private final Split split;
        private final IndexShard shard;
        private final Runnable onCompleted;

        private final AtomicBoolean cancelled;

        private State currentState;

        private StateMachine(Split split, IndexShard shard, Runnable onCompleted, State initialState) {
            this.split = split;
            this.shard = shard;
            this.onCompleted = onCompleted;

            this.cancelled = new AtomicBoolean(false);

            this.currentState = initialState;
        }

        /// Starts the state machine from the current state.
        void run() {
            advance(currentState);
        }

        void cancel() {
            cancelled.set(true);
        }

        private void advance(State newState) {
            // We always fork to generic here since we get callbacks
            // from various places like cluster state update threads
            // and transport threads.
            // We only do this in a linear fashion (once the previous state transition logic completed).
            clusterService.threadPool().generic().submit(() -> advanceInternal(newState));
        }

        private void advanceInternal(State newState) {
            validateStateTransition(newState);
            this.currentState = newState;

            // TODO relax logging once implementation is stable
            logger.info("Advancing split target shard state machine for shard {} to {}", shard.shardId(), newState);

            switch (newState) {
                case State.Clone clone -> {
                    // Start split RPC won't complete until the entire handoff sequence completes.
                    // So we advance to waiting for handoff immediately after sending the start split request.
                    // Once the handoff successfully completes, start split completes as well
                    // which advances the state.

                    // Advance beforehand to eliminate theoretical race conditions with sending
                    // a request to the source shard.
                    // This can be done since the advancement does nothing anyway.
                    advanceInternal(new State.WaitingForHandoff());
                    initiateSplitWithSourceShard(clone);
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
                    waitForShardStarted(new StateAdvancingListener<>(new State.Handoff()));
                }
                case State.Handoff ignored -> {
                    waitUntilSearchShardsAreOnline();
                }
                case State.SearchShardsOnline ignored -> {
                    changeStateToSplit();
                }
                case State.Split ignored -> {
                    awaitSplitApplied(new StateAdvancingListener<>(new State.SplitApplied()));
                }
                case State.SplitApplied splitApplied -> {
                    logger.info("notifying of split completion for target shard {}", shard.shardId());
                    reshardIndexService.notifySplitCompletion(shard.shardId());
                    deleteUnownedData();
                }
                case State.UnownedDataDeleted ignored -> {
                    changeStateToDone();
                }
                case State.Done ignored -> {
                    reshardIndexService.stopTrackingSplit(shard.shardId());
                    onCompleted.run();
                }

                case State.RecoveringInHandoff ignored -> {
                    waitForShardStarted(new StateAdvancingListener<>(new State.Handoff()));
                }
                case State.RecoveringInSplit ignored -> {
                    /// We need to confirm that coordinators are aware of SPLIT state before proceeding further.
                    /// If they are not we can return stale search results from the source shard
                    /// after a successful write to the target shard and refresh.
                    /// Note that we'll automatically apply a refresh block since the current state is SPLIT,
                    /// see [ReshardIndexService#maybeAwaitSplit].
                    /// As such it's okay to have the shard start here before awaiting SPLIT application.
                    SubscribableListener.newForked(this::waitForShardStarted)
                        .andThen(this::awaitSplitApplied)
                        .addListener(new StateAdvancingListener<>(new State.SplitApplied()));
                }

                case State.FailedInRecovery failedInRecovery -> {
                    failedInRecovery.recoveryListener.onFailure(failedInRecovery.exception);
                }
                case State.Failed failed -> {
                    if (cancelled.get()) {
                        return;
                    }

                    logger.warn("Failed to complete split target shard sequence", failed.exception);

                    if (failed.destinationState instanceof State.Split) {
                        reshardIndexService.notifySplitFailure(shard.shardId(), failed.exception);
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

        private void validateStateTransition(State newState) {
            var validCurrentStates = newStateToValidCurrentStates.get(newState.getClass());
            if (validCurrentStates.contains(currentState.getClass()) == false) {
                // It's possible that this exception is not observed by anyone since we are inside a runnable on generic thread pool.
                // So we log as well.
                var message = String.format(Locale.ROOT, "Unexpected state transition %s -> %s", currentState, newState);
                assert false : message;
                logger.error(message);
                throw new IllegalStateException(message);
            }
        }

        private static final Map<Class<? extends State>, Set<Class<? extends State>>> newStateToValidCurrentStates = new HashMap<>() {
            {
                put(State.Clone.class, Set.of(State.Clone.class));
                put(State.WaitingForHandoff.class, Set.of(State.Clone.class));
                put(State.HandoffReceived.class, Set.of(State.WaitingForHandoff.class));
                put(State.StartSplitRpcComplete.class, Set.of(State.HandoffReceived.class));
                put(State.Handoff.class, Set.of(State.StartSplitRpcComplete.class, State.RecoveringInHandoff.class));
                put(State.SearchShardsOnline.class, Set.of(State.Handoff.class));
                put(State.Split.class, Set.of(State.SearchShardsOnline.class));
                put(State.SplitApplied.class, Set.of(State.Split.class, State.RecoveringInSplit.class));
                put(State.UnownedDataDeleted.class, Set.of(State.SplitApplied.class));
                put(State.Done.class, Set.of(State.UnownedDataDeleted.class));

                put(State.RecoveringInHandoff.class, Set.of(State.RecoveringInHandoff.class));
                put(State.RecoveringInSplit.class, Set.of(State.RecoveringInSplit.class));

                put(State.FailedInRecovery.class, Set.of(State.WaitingForHandoff.class, State.HandoffReceived.class));
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
        /// E.g.  we always transition SearchShardsOnline -> Split.
        sealed interface State permits State.Clone, State.WaitingForHandoff, State.HandoffReceived, State.StartSplitRpcComplete,
            State.Handoff, State.SearchShardsOnline, State.Split, State.SplitApplied, State.UnownedDataDeleted, State.Done,
            State.RecoveringInHandoff, State.RecoveringInSplit, State.FailedInRecovery, State.Failed {
            // Corresponds to CLONE state of a target shard.
            record Clone(ActionListener<Void> recoveryListener) implements State {}

            record WaitingForHandoff() implements State {}

            record HandoffReceived(ActionListener<Void> handoffRpcListener) implements State {}

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

            // We distinguish these failed states because the shard state is different
            // (it's not started during recovery) and failure handling is different as a result.
            record FailedInRecovery(Exception exception, ActionListener<Void> recoveryListener) implements State {}

            /// @param destinationState next state that we attempted to transition to but failed
            record Failed(Exception exception, State destinationState) implements State {}
        }

        void acceptHandoff(Split splitFromRequest, ActionListener<Void> handoffRpcListener) {
            if (currentState instanceof State.WaitingForHandoff == false) {
                throw new IllegalStateException("Received an unexpected handoff request from the source shard");
            }

            if (split.equals(splitFromRequest) == false) {
                logger.debug("Received a stale split handoff request: {}", splitFromRequest);
                throw new IllegalStateException("Received a stale split handoff request for shard " + splitFromRequest.shardId());
            }

            advance(new State.HandoffReceived(handoffRpcListener));
        }

        private void initiateSplitWithSourceShard(State.Clone state) {
            client.execute(TransportReshardSplitAction.TYPE, new TransportReshardSplitAction.SplitRequest(split), new ActionListener<>() {
                @Override
                public void onResponse(ActionResponse ignored) {
                    advance(new State.StartSplitRpcComplete(state.recoveryListener));
                }

                @Override
                public void onFailure(Exception e) {
                    advance(new State.FailedInRecovery(e, state.recoveryListener));
                }
            });
        }

        private void changeStateToHandoff(State.HandoffReceived handoffReceived) {
            SplitStateRequest splitStateRequest = new SplitStateRequest(
                split.shardId,
                IndexReshardingState.Split.TargetShardState.HANDOFF,
                split.sourcePrimaryTerm,
                split.targetPrimaryTerm
            );
            // Result of this operation does not directly change the state.
            // Instead, once the source shard receives a successful response to the handoff request
            // it completes the start split request initiated in State.Clone and that advances the state of the state machine.
            // Because of that we can simply use handoffRpcListener here.
            // We could model this as an explicit state, but it doesn't really improve anything.
            // It can be done if the logic becomes more complex than simply chaining two operations.
            client.execute(
                TransportUpdateSplitTargetShardStateAction.TYPE,
                splitStateRequest,
                handoffReceived.handoffRpcListener.map(ignored -> null)
            );
        }

        private void waitUntilSearchShardsAreOnline() {
            ShardId shardId = split.shardId();

            ClusterStateObserver.waitForState(
                clusterService,
                clusterService.threadPool().getThreadContext(),
                new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        final Index index = shardId.getIndex();
                        final ProjectMetadata projectMetadata = state.metadata().lookupProject(index).orElse(null);
                        if (projectMetadata != null) {
                            if (newPrimaryTerm(index, projectMetadata, shardId, split.targetPrimaryTerm()) == false) {
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
                        // After the timeout we proceed to SPLIT anyway.
                        // The timeout is just best effort to ensure search shards are running to prevent downtime.
                        advance(new State.SearchShardsOnline());
                    }
                },
                state -> searchShardsOnlineOrNewPrimaryTerm(state, shardId, split.targetPrimaryTerm()),
                searchShardsOnlineTimeout,
                logger
            );
        }

        private void changeStateToSplit() {
            ChangeState changeState = new ChangeState(
                clusterService,
                client,
                split,
                IndexReshardingState.Split.TargetShardState.SPLIT,
                cancelled,
                new StateAdvancingListener<>(new State.Split())
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

                            Optional<IndexReshardingMetadata> reshardingMetadata = getReshardingMetadata(state, split.shardId);
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
                        Optional<IndexReshardingMetadata> reshardingMetadata = getReshardingMetadata(state, split.shardId);
                        return reshardingMetadata.isEmpty()
                            || reshardingMetadata.get()
                                .getSplit()
                                .targetStateAtLeast(split.shardId.id(), IndexReshardingState.Split.TargetShardState.SPLIT);
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

        private void deleteUnownedData() {
            var deleteUnowned = new DeleteUnowned(
                clusterService,
                reshardIndexService,
                shard.shardId(),
                new StateAdvancingListener<>(new State.UnownedDataDeleted()),
                cancelled
            );
            deleteUnowned.run();
        }

        private void changeStateToDone() {
            ChangeState changeState = new ChangeState(
                clusterService,
                client,
                split,
                IndexReshardingState.Split.TargetShardState.DONE,
                cancelled,
                new StateAdvancingListener<>(new State.Done())
            );
            changeState.run();
        }

        private void waitForShardStarted(ActionListener<Void> listener) {
            Predicate<ClusterState> predicate = state -> {
                if (cancelled.get()) {
                    return true;
                }

                var project = state.metadata().projectFor(split.shardId().getIndex());
                var primaryShardRouting = state.routingTable(project.id()).shardRoutingTable(split.shardId()).primaryShard();
                return primaryShardRouting.allocationId().equals(shard.routingEntry().allocationId()) && primaryShardRouting.started();
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
                advance(new State.Failed(e, nextState));
            }
        }
    }

    private static class ChangeState extends RetryableAction<ActionResponse> {
        private final Client client;

        private final Split split;
        private final IndexReshardingState.Split.TargetShardState state;
        private final AtomicBoolean cancelled;

        private ChangeState(
            ClusterService clusterService,
            Client client,
            Split split,
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
            this.split = split;
            this.state = state;
            this.cancelled = cancelled;
        }

        @Override
        public void tryAction(ActionListener<ActionResponse> listener) {
            if (cancelled.get()) {
                listener.onFailure(new CancellationException());
            }

            var request = new SplitStateRequest(split.shardId(), state, split.sourcePrimaryTerm(), split.targetPrimaryTerm());
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
                ///  See [RESHARD_SPLIT_SPLIT_STATE_APPLIED_TIMEOUT].
                splitStateAppliedTimeout, // timeoutValue
                ///  See [#onFinished()].
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
        private final ShardId shardId;
        private final AtomicBoolean cancelled;

        DeleteUnowned(
            ClusterService clusterService,
            ReshardIndexService reshardIndexService,
            ShardId shardId,
            ActionListener<Void> listener,
            AtomicBoolean cancelled
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
            this.shardId = shardId;
            this.cancelled = cancelled;
        }

        @Override
        public void tryAction(ActionListener<Void> listener) {
            if (cancelled.get()) {
                listener.onFailure(new CancellationException());
            }
            reshardIndexService.deleteUnownedDocuments(shardId, listener);
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

    private static boolean searchShardsOnlineOrNewPrimaryTerm(ClusterState state, ShardId shardId, long primaryTerm) {
        final Index index = shardId.getIndex();
        final ProjectMetadata projectMetadata = state.metadata().lookupProject(index).orElse(null);
        if (projectMetadata == null) {
            logger.debug("index not found while checking if shard {} is search shards online", shardId);
            return false;
        }

        return newPrimaryTerm(index, projectMetadata, shardId, primaryTerm) || isShardGreen(state, projectMetadata, shardId);
    }

    private static boolean newPrimaryTerm(Index index, ProjectMetadata projectMetadata, ShardId shardId, long primaryTerm) {
        long currentPrimaryTerm = projectMetadata.index(index).primaryTerm(shardId.id());
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
        assert shardRoutingTable != null;
        return new ClusterShardHealth(shardId.getId(), shardRoutingTable).getStatus() == ClusterHealthStatus.GREEN;
    }
}
