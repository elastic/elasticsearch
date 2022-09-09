/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.action.shard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ResultDeduplicator;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.MasterNodeChangePredicate;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.StaleShard;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.logging.log4j.Level.DEBUG;
import static org.apache.logging.log4j.Level.ERROR;
import static org.elasticsearch.cluster.service.MasterService.isPublishFailureException;
import static org.elasticsearch.core.Strings.format;

public class ShardStateAction {

    private static final Logger logger = LogManager.getLogger(ShardStateAction.class);

    public static final String SHARD_STARTED_ACTION_NAME = "internal:cluster/shard/started";
    public static final String SHARD_FAILED_ACTION_NAME = "internal:cluster/shard/failure";

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    // we deduplicate these shard state requests in order to avoid sending duplicate failed/started shard requests for a shard
    private final ResultDeduplicator<TransportRequest, Void> remoteShardStateUpdateDeduplicator;

    @Inject
    public ShardStateAction(
        ClusterService clusterService,
        TransportService transportService,
        AllocationService allocationService,
        RerouteService rerouteService,
        ThreadPool threadPool
    ) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.remoteShardStateUpdateDeduplicator = new ResultDeduplicator<>(threadPool.getThreadContext());

        transportService.registerRequestHandler(
            SHARD_STARTED_ACTION_NAME,
            ThreadPool.Names.SAME,
            StartedShardEntry::new,
            new ShardStartedTransportHandler(clusterService, new ShardStartedClusterStateTaskExecutor(allocationService, rerouteService))
        );
        transportService.registerRequestHandler(
            SHARD_FAILED_ACTION_NAME,
            ThreadPool.Names.SAME,
            FailedShardEntry::new,
            new ShardFailedTransportHandler(clusterService, new ShardFailedClusterStateTaskExecutor(allocationService, rerouteService))
        );
    }

    private void sendShardAction(
        final String actionName,
        final ClusterState currentState,
        final TransportRequest request,
        final ActionListener<Void> listener
    ) {
        ClusterStateObserver observer = new ClusterStateObserver(currentState, clusterService, null, logger, threadPool.getThreadContext());
        DiscoveryNode masterNode = currentState.nodes().getMasterNode();
        Predicate<ClusterState> changePredicate = MasterNodeChangePredicate.build(currentState);
        if (masterNode == null) {
            logger.warn("no master known for action [{}] for shard entry [{}]", actionName, request);
            waitForNewMasterAndRetry(actionName, observer, request, listener, changePredicate);
        } else {
            logger.debug("sending [{}] to [{}] for shard entry [{}]", actionName, masterNode.getId(), request);
            transportService.sendRequest(masterNode, actionName, request, new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                @Override
                public void handleResponse(TransportResponse.Empty response) {
                    listener.onResponse(null);
                }

                @Override
                public void handleException(TransportException exp) {
                    if (isMasterChannelException(exp)) {
                        waitForNewMasterAndRetry(actionName, observer, request, listener, changePredicate);
                    } else {
                        logger.warn(
                            () -> format(
                                "unexpected failure while sending request [%s]" + " to [%s] for shard entry [%s]",
                                actionName,
                                masterNode,
                                request
                            ),
                            exp
                        );
                        listener.onFailure(
                            exp instanceof RemoteTransportException
                                ? (Exception) (exp.getCause() instanceof Exception
                                    ? exp.getCause()
                                    : new ElasticsearchException(exp.getCause()))
                                : exp
                        );
                    }
                }
            });
        }
    }

    private static final Class<?>[] MASTER_CHANNEL_EXCEPTIONS = new Class<?>[] {
        NotMasterException.class,
        ConnectTransportException.class,
        FailedToCommitClusterStateException.class };

    private static boolean isMasterChannelException(TransportException exp) {
        return ExceptionsHelper.unwrap(exp, MASTER_CHANNEL_EXCEPTIONS) != null;
    }

    /**
     * Send a shard failed request to the master node to update the cluster state with the failure of a shard on another node. This means
     * that the shard should be failed because a write made it into the primary but was not replicated to this shard copy. If the shard
     * does not exist anymore but still has an entry in the in-sync set, remove its allocation id from the in-sync set.
     *
     * @param shardId            shard id of the shard to fail
     * @param allocationId       allocation id of the shard to fail
     * @param primaryTerm        the primary term associated with the primary shard that is failing the shard. Must be strictly positive.
     * @param markAsStale        whether or not to mark a failing shard as stale (eg. removing from in-sync set) when failing the shard.
     * @param message            the reason for the failure
     * @param failure            the underlying cause of the failure
     * @param listener           callback upon completion of the request
     */
    public void remoteShardFailed(
        final ShardId shardId,
        String allocationId,
        long primaryTerm,
        boolean markAsStale,
        final String message,
        @Nullable final Exception failure,
        ActionListener<Void> listener
    ) {
        assert primaryTerm > 0L : "primary term should be strictly positive";
        remoteShardStateUpdateDeduplicator.executeOnce(
            new FailedShardEntry(shardId, allocationId, primaryTerm, message, failure, markAsStale),
            listener,
            (req, reqListener) -> sendShardAction(SHARD_FAILED_ACTION_NAME, clusterService.state(), req, reqListener)
        );
    }

    int remoteShardRequestsInFlight() {
        return remoteShardStateUpdateDeduplicator.size();
    }

    /**
     * Clears out {@link #remoteShardStateUpdateDeduplicator}. Called by
     * {@link org.elasticsearch.indices.cluster.IndicesClusterStateService} in case of a master failover to enable sending fresh requests
     * to the new master right away on master failover.
     * This method is best effort in so far that it might clear out valid requests in edge cases during master failover. This is not an
     * issue functionally and merely results in some unnecessary transport requests.
     */
    public void clearRemoteShardRequestDeduplicator() {
        remoteShardStateUpdateDeduplicator.clear();
    }

    /**
     * Send a shard failed request to the master node to update the cluster state when a shard on the local node failed.
     */
    public void localShardFailed(
        final ShardRouting shardRouting,
        final String message,
        @Nullable final Exception failure,
        ActionListener<Void> listener
    ) {
        localShardFailed(shardRouting, message, failure, listener, clusterService.state());
    }

    /**
     * Send a shard failed request to the master node to update the cluster state when a shard on the local node failed.
     */
    public void localShardFailed(
        final ShardRouting shardRouting,
        final String message,
        @Nullable final Exception failure,
        ActionListener<Void> listener,
        final ClusterState currentState
    ) {
        FailedShardEntry shardEntry = new FailedShardEntry(
            shardRouting.shardId(),
            shardRouting.allocationId().getId(),
            0L,
            message,
            failure,
            true
        );
        sendShardAction(SHARD_FAILED_ACTION_NAME, currentState, shardEntry, listener);
    }

    // visible for testing
    protected void waitForNewMasterAndRetry(
        String actionName,
        ClusterStateObserver observer,
        TransportRequest request,
        ActionListener<Void> listener,
        Predicate<ClusterState> changePredicate
    ) {
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                if (logger.isTraceEnabled()) {
                    logger.trace("new cluster state [{}] after waiting for master election for shard entry [{}]", state, request);
                }
                sendShardAction(actionName, state, request, listener);
            }

            @Override
            public void onClusterServiceClose() {
                logger.warn("node closed while execution action [{}] for shard entry [{}]", actionName, request);
                listener.onFailure(new NodeClosedException(clusterService.localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                // we wait indefinitely for a new master
                assert false;
            }
        }, changePredicate);
    }

    // TODO: Make this a TransportMasterNodeAction and remove duplication of master failover retrying from upstream code
    private static class ShardFailedTransportHandler implements TransportRequestHandler<FailedShardEntry> {
        private final ClusterService clusterService;
        private final ShardFailedClusterStateTaskExecutor shardFailedClusterStateTaskExecutor;

        ShardFailedTransportHandler(
            ClusterService clusterService,
            ShardFailedClusterStateTaskExecutor shardFailedClusterStateTaskExecutor
        ) {
            this.clusterService = clusterService;
            this.shardFailedClusterStateTaskExecutor = shardFailedClusterStateTaskExecutor;
        }

        private static final String TASK_SOURCE = "shard-failed";

        @Override
        public void messageReceived(FailedShardEntry request, TransportChannel channel, Task task) throws Exception {
            logger.debug(() -> format("%s received shard failed for [%s]", request.getShardId(), request), request.failure);
            var update = new FailedShardUpdateTask(request, new ChannelActionListener<>(channel, TASK_SOURCE, request));
            clusterService.submitStateUpdateTask(
                TASK_SOURCE,
                update,
                ClusterStateTaskConfig.build(Priority.HIGH),
                shardFailedClusterStateTaskExecutor
            );
        }
    }

    public static class ShardFailedClusterStateTaskExecutor implements ClusterStateTaskExecutor<FailedShardUpdateTask> {
        private final AllocationService allocationService;
        private final RerouteService rerouteService;

        public ShardFailedClusterStateTaskExecutor(AllocationService allocationService, RerouteService rerouteService) {
            this.allocationService = allocationService;
            this.rerouteService = rerouteService;
        }

        @Override
        public ClusterState execute(BatchExecutionContext<FailedShardUpdateTask> batchExecutionContext) throws Exception {
            List<ClusterStateTaskExecutor.TaskContext<FailedShardUpdateTask>> tasksToBeApplied = new ArrayList<>();
            List<FailedShard> failedShardsToBeApplied = new ArrayList<>();
            List<StaleShard> staleShardsToBeApplied = new ArrayList<>();
            final ClusterState initialState = batchExecutionContext.initialState();
            for (final var taskContext : batchExecutionContext.taskContexts()) {
                final var task = taskContext.getTask();
                FailedShardEntry entry = task.entry();
                IndexMetadata indexMetadata = initialState.metadata().index(entry.getShardId().getIndex());
                if (indexMetadata == null) {
                    // tasks that correspond to non-existent indices are marked as successful
                    logger.debug(
                        "{} ignoring shard failed task [{}] (unknown index {})",
                        entry.getShardId(),
                        entry,
                        entry.getShardId().getIndex()
                    );
                    taskContext.success(() -> task.listener().onResponse(TransportResponse.Empty.INSTANCE));
                } else {
                    // The primary term is 0 if the shard failed itself. It is > 0 if a write was done on a primary but was failed to be
                    // replicated to the shard copy with the provided allocation id. In case where the shard failed itself, it's ok to just
                    // remove the corresponding routing entry from the routing table. In case where a write could not be replicated,
                    // however, it is important to ensure that the shard copy with the missing write is considered as stale from that point
                    // on, which is implemented by removing the allocation id of the shard copy from the in-sync allocations set.
                    // We check here that the primary to which the write happened was not already failed in an earlier cluster state update.
                    // This prevents situations where a new primary has already been selected and replication failures from an old stale
                    // primary unnecessarily fail currently active shards.
                    if (entry.primaryTerm > 0) {
                        long currentPrimaryTerm = indexMetadata.primaryTerm(entry.getShardId().id());
                        if (currentPrimaryTerm != entry.primaryTerm) {
                            assert currentPrimaryTerm > entry.primaryTerm
                                : "received a primary term with a higher term than in the "
                                    + "current cluster state (received ["
                                    + entry.primaryTerm
                                    + "] but current is ["
                                    + currentPrimaryTerm
                                    + "])";
                            logger.debug(
                                "{} failing shard failed task [{}] (primary term {} does not match current term {})",
                                entry.getShardId(),
                                entry,
                                entry.primaryTerm,
                                indexMetadata.primaryTerm(entry.getShardId().id())
                            );
                            taskContext.onFailure(
                                new NoLongerPrimaryShardException(
                                    entry.getShardId(),
                                    "primary term ["
                                        + entry.primaryTerm
                                        + "] did not match current primary term ["
                                        + currentPrimaryTerm
                                        + "]"
                                )
                            );
                            continue;
                        }
                    }

                    ShardRouting matched = initialState.getRoutingTable().getByAllocationId(entry.getShardId(), entry.getAllocationId());
                    if (matched == null) {
                        Set<String> inSyncAllocationIds = indexMetadata.inSyncAllocationIds(entry.getShardId().id());
                        // mark shard copies without routing entries that are in in-sync allocations set only as stale if the reason why
                        // they were failed is because a write made it into the primary but not to this copy (which corresponds to
                        // the check "primaryTerm > 0").
                        if (entry.primaryTerm > 0 && inSyncAllocationIds.contains(entry.getAllocationId())) {
                            logger.debug(
                                "{} marking shard {} as stale (shard failed task: [{}])",
                                entry.getShardId(),
                                entry.getAllocationId(),
                                entry
                            );
                            tasksToBeApplied.add(taskContext);
                            staleShardsToBeApplied.add(new StaleShard(entry.getShardId(), entry.getAllocationId()));
                        } else {
                            // tasks that correspond to non-existent shards are marked as successful
                            logger.debug("{} ignoring shard failed task [{}] (shard does not exist anymore)", entry.getShardId(), entry);
                            taskContext.success(() -> task.listener().onResponse(TransportResponse.Empty.INSTANCE));
                        }
                    } else {
                        // failing a shard also possibly marks it as stale (see IndexMetadataUpdater)
                        logger.debug("{} failing shard {} (shard failed task: [{}])", entry.getShardId(), matched, task);
                        tasksToBeApplied.add(taskContext);
                        failedShardsToBeApplied.add(new FailedShard(matched, entry.message, entry.failure, entry.markAsStale));
                    }
                }
            }
            assert tasksToBeApplied.size() == failedShardsToBeApplied.size() + staleShardsToBeApplied.size();

            ClusterState maybeUpdatedState = initialState;
            try (var ignored = batchExecutionContext.dropHeadersContext()) {
                // drop deprecation warnings arising from the computation (reroute etc).
                maybeUpdatedState = applyFailedShards(initialState, failedShardsToBeApplied, staleShardsToBeApplied);
                for (final var taskContext : tasksToBeApplied) {
                    taskContext.success(() -> taskContext.getTask().listener().onResponse(TransportResponse.Empty.INSTANCE));
                }
            } catch (Exception e) {
                logger.warn(() -> format("failed to apply failed shards %s", failedShardsToBeApplied), e);
                // failures are communicated back to the requester
                // cluster state will not be updated in this case
                for (final var taskContext : tasksToBeApplied) {
                    taskContext.onFailure(e);
                }
            }

            return maybeUpdatedState;
        }

        // visible for testing
        ClusterState applyFailedShards(ClusterState currentState, List<FailedShard> failedShards, List<StaleShard> staleShards) {
            return allocationService.applyFailedShards(currentState, failedShards, staleShards);
        }

        @Override
        public void clusterStatePublished(ClusterState newClusterState) {
            int numberOfUnassignedShards = newClusterState.getRoutingNodes().unassigned().size();
            if (numberOfUnassignedShards > 0) {
                // The reroute called after failing some shards will not assign any shard back to the node on which it failed. If there were
                // no other options for a failed shard then it is left unassigned. However, absent other options it's better to try and
                // assign it again, even if that means putting it back on the node on which it previously failed:
                final String reason = String.format(Locale.ROOT, "[%d] unassigned shards after failing shards", numberOfUnassignedShards);
                logger.trace("{}, scheduling a reroute", reason);
                rerouteService.reroute(
                    reason,
                    Priority.NORMAL,
                    ActionListener.wrap(
                        r -> logger.trace("{}, reroute completed", reason),
                        e -> logger.debug(() -> format("%s, reroute failed", reason), e)
                    )
                );
            }
        }
    }

    public static class FailedShardEntry extends TransportRequest {
        final ShardId shardId;
        final String allocationId;
        final long primaryTerm;
        final String message;
        @Nullable
        final Exception failure;
        final boolean markAsStale;

        FailedShardEntry(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
            allocationId = in.readString();
            primaryTerm = in.readVLong();
            message = in.readString();
            failure = in.readException();
            markAsStale = in.readBoolean();
        }

        public FailedShardEntry(
            ShardId shardId,
            String allocationId,
            long primaryTerm,
            String message,
            @Nullable Exception failure,
            boolean markAsStale
        ) {
            this.shardId = shardId;
            this.allocationId = allocationId;
            this.primaryTerm = primaryTerm;
            this.message = message;
            this.failure = failure;
            this.markAsStale = markAsStale;
        }

        public ShardId getShardId() {
            return shardId;
        }

        public String getAllocationId() {
            return allocationId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeString(allocationId);
            out.writeVLong(primaryTerm);
            out.writeString(message);
            out.writeException(failure);
            out.writeBoolean(markAsStale);
        }

        @Override
        public String toString() {
            List<String> components = new ArrayList<>(6);
            components.add("shard id [" + shardId + "]");
            components.add("allocation id [" + allocationId + "]");
            components.add("primary term [" + primaryTerm + "]");
            components.add("message [" + message + "]");
            components.add("markAsStale [" + markAsStale + "]");
            if (failure != null) {
                components.add("failure [" + ExceptionsHelper.stackTrace(failure) + "]");
            }
            return String.join(", ", components);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FailedShardEntry that = (FailedShardEntry) o;
            // Exclude message and exception from equals and hashCode
            return Objects.equals(this.shardId, that.shardId)
                && Objects.equals(this.allocationId, that.allocationId)
                && primaryTerm == that.primaryTerm
                && markAsStale == that.markAsStale;
        }

        @Override
        public int hashCode() {
            return Objects.hash(shardId, allocationId, primaryTerm, markAsStale);
        }
    }

    public record FailedShardUpdateTask(FailedShardEntry entry, ActionListener<TransportResponse.Empty> listener)
        implements
            ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            logger.log(
                isPublishFailureException(e) ? DEBUG : ERROR,
                () -> format("%s unexpected failure while failing shard [%s]", entry.shardId, entry),
                e
            );
            listener.onFailure(e);
        }
    }

    public void shardStarted(
        final ShardRouting shardRouting,
        final long primaryTerm,
        final String message,
        final ShardLongFieldRange timestampRange,
        final ActionListener<Void> listener
    ) {
        shardStarted(shardRouting, primaryTerm, message, timestampRange, listener, clusterService.state());
    }

    public void shardStarted(
        final ShardRouting shardRouting,
        final long primaryTerm,
        final String message,
        final ShardLongFieldRange timestampRange,
        final ActionListener<Void> listener,
        final ClusterState currentState
    ) {
        remoteShardStateUpdateDeduplicator.executeOnce(
            new StartedShardEntry(shardRouting.shardId(), shardRouting.allocationId().getId(), primaryTerm, message, timestampRange),
            listener,
            (req, l) -> sendShardAction(SHARD_STARTED_ACTION_NAME, currentState, req, l)
        );
    }

    // TODO: Make this a TransportMasterNodeAction and remove duplication of master failover retrying from upstream code
    private static class ShardStartedTransportHandler implements TransportRequestHandler<StartedShardEntry> {
        private final ClusterService clusterService;
        private final ShardStartedClusterStateTaskExecutor shardStartedClusterStateTaskExecutor;

        ShardStartedTransportHandler(
            ClusterService clusterService,
            ShardStartedClusterStateTaskExecutor shardStartedClusterStateTaskExecutor
        ) {
            this.clusterService = clusterService;
            this.shardStartedClusterStateTaskExecutor = shardStartedClusterStateTaskExecutor;
        }

        @Override
        public void messageReceived(StartedShardEntry request, TransportChannel channel, Task task) throws Exception {
            logger.debug("{} received shard started for [{}]", request.shardId, request);
            final ChannelActionListener<TransportResponse.Empty, StartedShardEntry> listener = new ChannelActionListener<>(
                channel,
                SHARD_STARTED_ACTION_NAME,
                request
            );

            var update = new StartedShardUpdateTask(request, listener);

            clusterService.submitStateUpdateTask(
                "shard-started " + request,
                update,
                ClusterStateTaskConfig.build(Priority.URGENT),
                shardStartedClusterStateTaskExecutor
            );
        }
    }

    public static class ShardStartedClusterStateTaskExecutor implements ClusterStateTaskExecutor<StartedShardUpdateTask> {
        private final AllocationService allocationService;
        private final RerouteService rerouteService;

        public ShardStartedClusterStateTaskExecutor(AllocationService allocationService, RerouteService rerouteService) {
            this.allocationService = allocationService;
            this.rerouteService = rerouteService;
        }

        @Override
        public ClusterState execute(BatchExecutionContext<StartedShardUpdateTask> batchExecutionContext) throws Exception {
            List<TaskContext<StartedShardUpdateTask>> tasksToBeApplied = new ArrayList<>();
            List<ShardRouting> shardRoutingsToBeApplied = new ArrayList<>(batchExecutionContext.taskContexts().size());
            Set<ShardRouting> seenShardRoutings = new HashSet<>(); // to prevent duplicates
            final Map<Index, IndexLongFieldRange> updatedTimestampRanges = new HashMap<>();
            final ClusterState initialState = batchExecutionContext.initialState();
            for (var taskContext : batchExecutionContext.taskContexts()) {
                final var task = taskContext.getTask();
                StartedShardEntry entry = task.getEntry();
                final ShardRouting matched = initialState.getRoutingTable().getByAllocationId(entry.shardId, entry.allocationId);
                if (matched == null) {
                    // tasks that correspond to non-existent shards are marked as successful. The reason is that we resend shard started
                    // events on every cluster state publishing that does not contain the shard as started yet. This means that old stale
                    // requests might still be in flight even after the shard has already been started or failed on the master. We just
                    // ignore these requests for now.
                    logger.debug("{} ignoring shard started task [{}] (shard does not exist anymore)", entry.shardId, entry);
                    taskContext.success(() -> task.listener().onResponse(TransportResponse.Empty.INSTANCE));
                } else {
                    if (matched.primary() && entry.primaryTerm > 0) {
                        final IndexMetadata indexMetadata = initialState.metadata().index(entry.shardId.getIndex());
                        assert indexMetadata != null;
                        final long currentPrimaryTerm = indexMetadata.primaryTerm(entry.shardId.id());
                        if (currentPrimaryTerm != entry.primaryTerm) {
                            assert currentPrimaryTerm > entry.primaryTerm
                                : "received a primary term with a higher term than in the "
                                    + "current cluster state (received ["
                                    + entry.primaryTerm
                                    + "] but current is ["
                                    + currentPrimaryTerm
                                    + "])";
                            logger.debug(
                                "{} ignoring shard started task [{}] (primary term {} does not match current term {})",
                                entry.shardId,
                                entry,
                                entry.primaryTerm,
                                currentPrimaryTerm
                            );
                            taskContext.success(() -> task.listener().onResponse(TransportResponse.Empty.INSTANCE));
                            continue;
                        }
                    }
                    if (matched.initializing() == false) {
                        assert matched.active() : "expected active shard routing for task " + entry + " but found " + matched;
                        // same as above, this might have been a stale in-flight request, so we just ignore.
                        logger.debug(
                            "{} ignoring shard started task [{}] (shard exists but is not initializing: {})",
                            entry.shardId,
                            entry,
                            matched
                        );
                        taskContext.success(() -> task.listener().onResponse(TransportResponse.Empty.INSTANCE));
                    } else {
                        // remove duplicate actions as allocation service expects a clean list without duplicates
                        if (seenShardRoutings.contains(matched)) {
                            logger.trace(
                                "{} ignoring shard started task [{}] (already scheduled to start {})",
                                entry.shardId,
                                entry,
                                matched
                            );
                            tasksToBeApplied.add(taskContext);
                        } else {
                            logger.debug("{} starting shard {} (shard started task: [{}])", entry.shardId, matched, entry);
                            tasksToBeApplied.add(taskContext);
                            shardRoutingsToBeApplied.add(matched);
                            seenShardRoutings.add(matched);

                            // expand the timestamp range recorded in the index metadata if needed
                            final Index index = entry.shardId.getIndex();
                            IndexLongFieldRange currentTimestampMillisRange = updatedTimestampRanges.get(index);
                            final IndexMetadata indexMetadata = initialState.metadata().index(index);
                            if (currentTimestampMillisRange == null) {
                                currentTimestampMillisRange = indexMetadata.getTimestampRange();
                            }
                            final IndexLongFieldRange newTimestampMillisRange;
                            newTimestampMillisRange = currentTimestampMillisRange.extendWithShardRange(
                                entry.shardId.id(),
                                indexMetadata.getNumberOfShards(),
                                entry.timestampRange
                            );
                            if (newTimestampMillisRange != currentTimestampMillisRange) {
                                updatedTimestampRanges.put(index, newTimestampMillisRange);
                            }
                        }
                    }
                }
            }
            assert tasksToBeApplied.size() >= shardRoutingsToBeApplied.size();

            ClusterState maybeUpdatedState = initialState;
            try {
                maybeUpdatedState = allocationService.applyStartedShards(initialState, shardRoutingsToBeApplied);

                if (updatedTimestampRanges.isEmpty() == false) {
                    final Metadata.Builder metadataBuilder = Metadata.builder(maybeUpdatedState.metadata());
                    for (Map.Entry<Index, IndexLongFieldRange> updatedTimestampRangeEntry : updatedTimestampRanges.entrySet()) {
                        metadataBuilder.put(
                            IndexMetadata.builder(metadataBuilder.getSafe(updatedTimestampRangeEntry.getKey()))
                                .timestampRange(updatedTimestampRangeEntry.getValue())
                        );
                    }
                    maybeUpdatedState = ClusterState.builder(maybeUpdatedState).metadata(metadataBuilder).build();
                }

                assert assertStartedIndicesHaveCompleteTimestampRanges(maybeUpdatedState);

                for (final var taskContext : tasksToBeApplied) {
                    taskContext.success(() -> taskContext.getTask().listener().onResponse(TransportResponse.Empty.INSTANCE));
                }
            } catch (Exception e) {
                logger.warn(() -> format("failed to apply started shards %s", shardRoutingsToBeApplied), e);
                for (final var taskContext : tasksToBeApplied) {
                    taskContext.onFailure(e);
                }
            }

            return maybeUpdatedState;
        }

        private static boolean assertStartedIndicesHaveCompleteTimestampRanges(ClusterState clusterState) {
            for (Map.Entry<String, IndexRoutingTable> cursor : clusterState.getRoutingTable().getIndicesRouting().entrySet()) {
                assert cursor.getValue().allPrimaryShardsActive() == false
                    || clusterState.metadata().index(cursor.getKey()).getTimestampRange().isComplete()
                    : "index ["
                        + cursor.getKey()
                        + "] should have complete timestamp range, but got "
                        + clusterState.metadata().index(cursor.getKey()).getTimestampRange()
                        + " for "
                        + cursor.getValue().prettyPrint();
            }
            return true;
        }

        @Override
        public void clusterStatePublished(ClusterState newClusterState) {
            rerouteService.reroute(
                "reroute after starting shards",
                Priority.NORMAL,
                ActionListener.wrap(
                    r -> logger.trace("reroute after starting shards succeeded"),
                    e -> logger.debug("reroute after starting shards failed", e)
                )
            );
        }
    }

    public static class StartedShardEntry extends TransportRequest {
        final ShardId shardId;
        final String allocationId;
        final long primaryTerm;
        final String message;
        final ShardLongFieldRange timestampRange;

        StartedShardEntry(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
            allocationId = in.readString();
            primaryTerm = in.readVLong();
            this.message = in.readString();
            this.timestampRange = ShardLongFieldRange.readFrom(in);
        }

        public StartedShardEntry(
            final ShardId shardId,
            final String allocationId,
            final long primaryTerm,
            final String message,
            final ShardLongFieldRange timestampRange
        ) {
            this.shardId = shardId;
            this.allocationId = allocationId;
            this.primaryTerm = primaryTerm;
            this.message = message;
            this.timestampRange = timestampRange;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeString(allocationId);
            out.writeVLong(primaryTerm);
            out.writeString(message);
            timestampRange.writeTo(out);
        }

        @Override
        public String toString() {
            return String.format(
                Locale.ROOT,
                "StartedShardEntry{shardId [%s], allocationId [%s], primary term [%d], message [%s]}",
                shardId,
                allocationId,
                primaryTerm,
                message
            );
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StartedShardEntry that = (StartedShardEntry) o;
            return primaryTerm == that.primaryTerm
                && shardId.equals(that.shardId)
                && allocationId.equals(that.allocationId)
                && message.equals(that.message)
                && timestampRange.equals(that.timestampRange);
        }

        @Override
        public int hashCode() {
            return Objects.hash(shardId, allocationId, primaryTerm, message, timestampRange);
        }
    }

    public record StartedShardUpdateTask(StartedShardEntry entry, ActionListener<TransportResponse.Empty> listener)
        implements
            ClusterStateTaskListener {

        public StartedShardEntry getEntry() {
            return entry;
        }

        @Override
        public void onFailure(Exception e) {
            if (e instanceof NotMasterException) {
                logger.debug(() -> format("%s no longer master while starting shard [%s]", entry.shardId, entry));
            } else if (e instanceof FailedToCommitClusterStateException) {
                logger.debug(() -> format("%s unexpected failure while starting shard [%s]", entry.shardId, entry), e);
            } else {
                logger.error(() -> format("%s unexpected failure while starting shard [%s]", entry.shardId, entry), e);
            }
            listener.onFailure(e);
        }

        @Override
        public String toString() {
            return "StartedShardUpdateTask{entry=" + entry + ", listener=" + listener + "}";
        }
    }

    public static class NoLongerPrimaryShardException extends ElasticsearchException {

        public NoLongerPrimaryShardException(ShardId shardId, String msg) {
            super(msg);
            setShard(shardId);
        }

        public NoLongerPrimaryShardException(StreamInput in) throws IOException {
            super(in);
        }

    }

}
