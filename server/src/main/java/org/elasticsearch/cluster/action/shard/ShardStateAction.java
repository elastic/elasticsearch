/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.action.shard;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.action.ResultDeduplicator;
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

public class ShardStateAction {

    private static final Logger logger = LogManager.getLogger(ShardStateAction.class);

    public static final String SHARD_STARTED_ACTION_NAME = "internal:cluster/shard/started";
    public static final String SHARD_FAILED_ACTION_NAME = "internal:cluster/shard/failure";

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    // a list of shards that failed during replication
    // we keep track of these shards in order to avoid sending duplicate failed shard requests for a single failing shard.
    private final ResultDeduplicator<FailedShardEntry, Void> remoteFailedShardsDeduplicator = new ResultDeduplicator<>();

    @Inject
    public ShardStateAction(ClusterService clusterService, TransportService transportService,
                            AllocationService allocationService, RerouteService rerouteService, ThreadPool threadPool) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;

        transportService.registerRequestHandler(SHARD_STARTED_ACTION_NAME, ThreadPool.Names.SAME, StartedShardEntry::new,
            new ShardStartedTransportHandler(clusterService,
                new ShardStartedClusterStateTaskExecutor(allocationService, rerouteService, logger),
                logger));
        transportService.registerRequestHandler(SHARD_FAILED_ACTION_NAME, ThreadPool.Names.SAME, FailedShardEntry::new,
            new ShardFailedTransportHandler(clusterService,
                new ShardFailedClusterStateTaskExecutor(allocationService, rerouteService, logger),
                logger));
    }

    private void sendShardAction(final String actionName, final ClusterState currentState,
                                 final TransportRequest request, final ActionListener<Void> listener) {
        ClusterStateObserver observer =
            new ClusterStateObserver(currentState, clusterService, null, logger, threadPool.getThreadContext());
        DiscoveryNode masterNode = currentState.nodes().getMasterNode();
        Predicate<ClusterState> changePredicate = MasterNodeChangePredicate.build(currentState);
        if (masterNode == null) {
            logger.warn("no master known for action [{}] for shard entry [{}]", actionName, request);
            waitForNewMasterAndRetry(actionName, observer, request, listener, changePredicate);
        } else {
            logger.debug("sending [{}] to [{}] for shard entry [{}]", actionName, masterNode.getId(), request);
            transportService.sendRequest(masterNode,
                actionName, request, new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                    @Override
                    public void handleResponse(TransportResponse.Empty response) {
                        listener.onResponse(null);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        if (isMasterChannelException(exp)) {
                            waitForNewMasterAndRetry(actionName, observer, request, listener, changePredicate);
                        } else {
                            logger.warn(new ParameterizedMessage("unexpected failure while sending request [{}]" +
                                " to [{}] for shard entry [{}]", actionName, masterNode, request), exp);
                            listener.onFailure(exp instanceof RemoteTransportException ?
                                (Exception) (exp.getCause() instanceof Exception ? exp.getCause() :
                                    new ElasticsearchException(exp.getCause())) : exp);
                        }
                    }
                });
        }
    }

    private static final Class<?>[] MASTER_CHANNEL_EXCEPTIONS = new Class<?>[]{
        NotMasterException.class,
        ConnectTransportException.class,
        FailedToCommitClusterStateException.class
    };

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
    public void remoteShardFailed(final ShardId shardId, String allocationId, long primaryTerm, boolean markAsStale, final String message,
                                  @Nullable final Exception failure, ActionListener<Void> listener) {
        assert primaryTerm > 0L : "primary term should be strictly positive";
        remoteFailedShardsDeduplicator.executeOnce(
            new FailedShardEntry(shardId, allocationId, primaryTerm, message, failure, markAsStale), listener,
            (req, reqListener) -> sendShardAction(SHARD_FAILED_ACTION_NAME, clusterService.state(), req, reqListener));
    }

    int remoteShardFailedCacheSize() {
        return remoteFailedShardsDeduplicator.size();
    }

    /**
     * Send a shard failed request to the master node to update the cluster state when a shard on the local node failed.
     */
    public void localShardFailed(final ShardRouting shardRouting, final String message,
                                 @Nullable final Exception failure, ActionListener<Void> listener) {
        localShardFailed(shardRouting, message, failure, listener, clusterService.state());
    }

    /**
     * Send a shard failed request to the master node to update the cluster state when a shard on the local node failed.
     */
    public void localShardFailed(final ShardRouting shardRouting, final String message, @Nullable final Exception failure,
                                 ActionListener<Void> listener, final ClusterState currentState) {
        FailedShardEntry shardEntry = new FailedShardEntry(shardRouting.shardId(), shardRouting.allocationId().getId(),
            0L, message, failure, true);
        sendShardAction(SHARD_FAILED_ACTION_NAME, currentState, shardEntry, listener);
    }

    // visible for testing
    protected void waitForNewMasterAndRetry(String actionName, ClusterStateObserver observer,
                                            TransportRequest request, ActionListener<Void> listener,
                                            Predicate<ClusterState> changePredicate) {
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

    private static class ShardFailedTransportHandler implements TransportRequestHandler<FailedShardEntry> {
        private final ClusterService clusterService;
        private final ShardFailedClusterStateTaskExecutor shardFailedClusterStateTaskExecutor;
        private final Logger logger;

        ShardFailedTransportHandler(ClusterService clusterService,
                                    ShardFailedClusterStateTaskExecutor shardFailedClusterStateTaskExecutor, Logger logger) {
            this.clusterService = clusterService;
            this.shardFailedClusterStateTaskExecutor = shardFailedClusterStateTaskExecutor;
            this.logger = logger;
        }

        @Override
        public void messageReceived(FailedShardEntry request, TransportChannel channel, Task task) throws Exception {
            logger.debug(() -> new ParameterizedMessage("{} received shard failed for {}",
                request.shardId, request), request.failure);
            clusterService.submitStateUpdateTask(
                "shard-failed",
                request,
                ClusterStateTaskConfig.build(Priority.HIGH),
                shardFailedClusterStateTaskExecutor,
                new ClusterStateTaskListener() {
                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.error(() -> new ParameterizedMessage("{} unexpected failure while failing shard [{}]",
                            request.shardId, request), e);
                        try {
                            channel.sendResponse(e);
                        } catch (Exception channelException) {
                            channelException.addSuppressed(e);
                            logger.warn(() ->
                                new ParameterizedMessage("{} failed to send failure [{}] while failing shard [{}]",
                                    request.shardId, e, request), channelException);
                        }
                    }

                    @Override
                    public void onNoLongerMaster(String source) {
                        logger.error("{} no longer master while failing shard [{}]", request.shardId, request);
                        try {
                            channel.sendResponse(new NotMasterException(source));
                        } catch (Exception channelException) {
                            logger.warn(() ->
                                new ParameterizedMessage("{} failed to send no longer master while failing shard [{}]",
                                    request.shardId, request), channelException);
                        }
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        try {
                            channel.sendResponse(TransportResponse.Empty.INSTANCE);
                        } catch (Exception channelException) {
                            logger.warn(() ->
                                new ParameterizedMessage("{} failed to send response while failing shard [{}]",
                                    request.shardId, request), channelException);
                        }
                    }
                }
            );
        }
    }

    public static class ShardFailedClusterStateTaskExecutor implements ClusterStateTaskExecutor<FailedShardEntry> {
        private final AllocationService allocationService;
        private final RerouteService rerouteService;
        private final Logger logger;

        public ShardFailedClusterStateTaskExecutor(AllocationService allocationService, RerouteService rerouteService, Logger logger) {
            this.allocationService = allocationService;
            this.rerouteService = rerouteService;
            this.logger = logger;
        }

        @Override
        public ClusterTasksResult<FailedShardEntry> execute(ClusterState currentState, List<FailedShardEntry> tasks) throws Exception {
            ClusterTasksResult.Builder<FailedShardEntry> batchResultBuilder = ClusterTasksResult.builder();
            List<FailedShardEntry> tasksToBeApplied = new ArrayList<>();
            List<FailedShard> failedShardsToBeApplied = new ArrayList<>();
            List<StaleShard> staleShardsToBeApplied = new ArrayList<>();

            for (FailedShardEntry task : tasks) {
                IndexMetadata indexMetadata = currentState.metadata().index(task.shardId.getIndex());
                if (indexMetadata == null) {
                    // tasks that correspond to non-existent indices are marked as successful
                    logger.debug("{} ignoring shard failed task [{}] (unknown index {})",
                        task.shardId, task, task.shardId.getIndex());
                    batchResultBuilder.success(task);
                } else {
                    // The primary term is 0 if the shard failed itself. It is > 0 if a write was done on a primary but was failed to be
                    // replicated to the shard copy with the provided allocation id. In case where the shard failed itself, it's ok to just
                    // remove the corresponding routing entry from the routing table. In case where a write could not be replicated,
                    // however, it is important to ensure that the shard copy with the missing write is considered as stale from that point
                    // on, which is implemented by removing the allocation id of the shard copy from the in-sync allocations set.
                    // We check here that the primary to which the write happened was not already failed in an earlier cluster state update.
                    // This prevents situations where a new primary has already been selected and replication failures from an old stale
                    // primary unnecessarily fail currently active shards.
                    if (task.primaryTerm > 0) {
                        long currentPrimaryTerm = indexMetadata.primaryTerm(task.shardId.id());
                        if (currentPrimaryTerm != task.primaryTerm) {
                            assert currentPrimaryTerm > task.primaryTerm : "received a primary term with a higher term than in the " +
                                "current cluster state (received [" + task.primaryTerm + "] but current is [" + currentPrimaryTerm + "])";
                            logger.debug("{} failing shard failed task [{}] (primary term {} does not match current term {})", task.shardId,
                                task, task.primaryTerm, indexMetadata.primaryTerm(task.shardId.id()));
                            batchResultBuilder.failure(task, new NoLongerPrimaryShardException(
                                task.shardId,
                                "primary term [" + task.primaryTerm + "] did not match current primary term [" + currentPrimaryTerm + "]"));
                            continue;
                        }
                    }

                    ShardRouting matched = currentState.getRoutingTable().getByAllocationId(task.shardId, task.allocationId);
                    if (matched == null) {
                        Set<String> inSyncAllocationIds = indexMetadata.inSyncAllocationIds(task.shardId.id());
                        // mark shard copies without routing entries that are in in-sync allocations set only as stale if the reason why
                        // they were failed is because a write made it into the primary but not to this copy (which corresponds to
                        // the check "primaryTerm > 0").
                        if (task.primaryTerm > 0 && inSyncAllocationIds.contains(task.allocationId)) {
                            logger.debug("{} marking shard {} as stale (shard failed task: [{}])",
                                task.shardId, task.allocationId, task);
                            tasksToBeApplied.add(task);
                            staleShardsToBeApplied.add(new StaleShard(task.shardId, task.allocationId));
                        } else {
                            // tasks that correspond to non-existent shards are marked as successful
                            logger.debug("{} ignoring shard failed task [{}] (shard does not exist anymore)", task.shardId, task);
                            batchResultBuilder.success(task);
                        }
                    } else {
                        // failing a shard also possibly marks it as stale (see IndexMetadataUpdater)
                        logger.debug("{} failing shard {} (shard failed task: [{}])", task.shardId, matched, task);
                        tasksToBeApplied.add(task);
                        failedShardsToBeApplied.add(new FailedShard(matched, task.message, task.failure, task.markAsStale));
                    }
                }
            }
            assert tasksToBeApplied.size() == failedShardsToBeApplied.size() + staleShardsToBeApplied.size();

            ClusterState maybeUpdatedState = currentState;
            try {
                maybeUpdatedState = applyFailedShards(currentState, failedShardsToBeApplied, staleShardsToBeApplied);
                batchResultBuilder.successes(tasksToBeApplied);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to apply failed shards {}", failedShardsToBeApplied), e);
                // failures are communicated back to the requester
                // cluster state will not be updated in this case
                batchResultBuilder.failures(tasksToBeApplied, e);
            }

            return batchResultBuilder.build(maybeUpdatedState);
        }

        // visible for testing
        ClusterState applyFailedShards(ClusterState currentState, List<FailedShard> failedShards, List<StaleShard> staleShards) {
            return allocationService.applyFailedShards(currentState, failedShards, staleShards);
        }

        @Override
        public void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {
            int numberOfUnassignedShards = clusterChangedEvent.state().getRoutingNodes().unassigned().size();
            if (numberOfUnassignedShards > 0) {
                // The reroute called after failing some shards will not assign any shard back to the node on which it failed. If there were
                // no other options for a failed shard then it is left unassigned. However, absent other options it's better to try and
                // assign it again, even if that means putting it back on the node on which it previously failed:
                final String reason = String.format(Locale.ROOT, "[%d] unassigned shards after failing shards", numberOfUnassignedShards);
                logger.trace("{}, scheduling a reroute", reason);
                rerouteService.reroute(reason, Priority.NORMAL, ActionListener.wrap(
                    r -> logger.trace("{}, reroute completed", reason),
                    e -> logger.debug(new ParameterizedMessage("{}, reroute failed", reason), e)));
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

        public FailedShardEntry(ShardId shardId, String allocationId, long primaryTerm,
                                String message, @Nullable Exception failure, boolean markAsStale) {
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
            return Objects.equals(this.shardId, that.shardId) &&
                Objects.equals(this.allocationId, that.allocationId) &&
                primaryTerm == that.primaryTerm &&
                markAsStale == that.markAsStale;
        }

        @Override
        public int hashCode() {
            return Objects.hash(shardId, allocationId, primaryTerm, markAsStale);
        }
    }

    public void shardStarted(final ShardRouting shardRouting,
                             final long primaryTerm,
                             final String message,
                             final ShardLongFieldRange timestampRange,
                             final ActionListener<Void> listener) {
        shardStarted(shardRouting, primaryTerm, message, timestampRange, listener, clusterService.state());
    }

    public void shardStarted(final ShardRouting shardRouting,
                             final long primaryTerm,
                             final String message,
                             final ShardLongFieldRange timestampRange,
                             final ActionListener<Void> listener,
                             final ClusterState currentState) {
        final StartedShardEntry entry = new StartedShardEntry(
                shardRouting.shardId(),
                shardRouting.allocationId().getId(),
                primaryTerm,
                message,
                timestampRange);
        sendShardAction(SHARD_STARTED_ACTION_NAME, currentState, entry, listener);
    }

    private static class ShardStartedTransportHandler implements TransportRequestHandler<StartedShardEntry> {
        private final ClusterService clusterService;
        private final ShardStartedClusterStateTaskExecutor shardStartedClusterStateTaskExecutor;
        private final Logger logger;

        ShardStartedTransportHandler(ClusterService clusterService,
                                     ShardStartedClusterStateTaskExecutor shardStartedClusterStateTaskExecutor, Logger logger) {
            this.clusterService = clusterService;
            this.shardStartedClusterStateTaskExecutor = shardStartedClusterStateTaskExecutor;
            this.logger = logger;
        }

        @Override
        public void messageReceived(StartedShardEntry request, TransportChannel channel, Task task) throws Exception {
            logger.debug("{} received shard started for [{}]", request.shardId, request);
            clusterService.submitStateUpdateTask(
                "shard-started " + request,
                request,
                ClusterStateTaskConfig.build(Priority.URGENT),
                shardStartedClusterStateTaskExecutor,
                shardStartedClusterStateTaskExecutor);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    public static class ShardStartedClusterStateTaskExecutor
            implements ClusterStateTaskExecutor<StartedShardEntry>, ClusterStateTaskListener {
        private final AllocationService allocationService;
        private final Logger logger;
        private final RerouteService rerouteService;

        public ShardStartedClusterStateTaskExecutor(AllocationService allocationService, RerouteService rerouteService, Logger logger) {
            this.allocationService = allocationService;
            this.logger = logger;
            this.rerouteService = rerouteService;
        }

        @Override
        public ClusterTasksResult<StartedShardEntry> execute(ClusterState currentState, List<StartedShardEntry> tasks) throws Exception {
            ClusterTasksResult.Builder<StartedShardEntry> builder = ClusterTasksResult.builder();
            List<StartedShardEntry> tasksToBeApplied = new ArrayList<>();
            List<ShardRouting> shardRoutingsToBeApplied = new ArrayList<>(tasks.size());
            Set<ShardRouting> seenShardRoutings = new HashSet<>(); // to prevent duplicates
            final Map<Index, IndexLongFieldRange> updatedTimestampRanges = new HashMap<>();
            for (StartedShardEntry task : tasks) {
                final ShardRouting matched = currentState.getRoutingTable().getByAllocationId(task.shardId, task.allocationId);
                if (matched == null) {
                    // tasks that correspond to non-existent shards are marked as successful. The reason is that we resend shard started
                    // events on every cluster state publishing that does not contain the shard as started yet. This means that old stale
                    // requests might still be in flight even after the shard has already been started or failed on the master. We just
                    // ignore these requests for now.
                    logger.debug("{} ignoring shard started task [{}] (shard does not exist anymore)", task.shardId, task);
                    builder.success(task);
                } else {
                    if (matched.primary() && task.primaryTerm > 0) {
                        final IndexMetadata indexMetadata = currentState.metadata().index(task.shardId.getIndex());
                        assert indexMetadata != null;
                        final long currentPrimaryTerm = indexMetadata.primaryTerm(task.shardId.id());
                        if (currentPrimaryTerm != task.primaryTerm) {
                            assert currentPrimaryTerm > task.primaryTerm : "received a primary term with a higher term than in the " +
                                "current cluster state (received [" + task.primaryTerm + "] but current is [" + currentPrimaryTerm + "])";
                            logger.debug("{} ignoring shard started task [{}] (primary term {} does not match current term {})",
                                task.shardId, task, task.primaryTerm, currentPrimaryTerm);
                            builder.success(task);
                            continue;
                        }
                    }
                    if (matched.initializing() == false) {
                        assert matched.active() : "expected active shard routing for task " + task + " but found " + matched;
                        // same as above, this might have been a stale in-flight request, so we just ignore.
                        logger.debug("{} ignoring shard started task [{}] (shard exists but is not initializing: {})",
                            task.shardId, task, matched);
                        builder.success(task);
                    } else {
                        // remove duplicate actions as allocation service expects a clean list without duplicates
                        if (seenShardRoutings.contains(matched)) {
                            logger.trace("{} ignoring shard started task [{}] (already scheduled to start {})",
                                task.shardId, task, matched);
                            tasksToBeApplied.add(task);
                        } else {
                            logger.debug("{} starting shard {} (shard started task: [{}])", task.shardId, matched, task);
                            tasksToBeApplied.add(task);
                            shardRoutingsToBeApplied.add(matched);
                            seenShardRoutings.add(matched);

                            // expand the timestamp range recorded in the index metadata if needed
                            final Index index = task.shardId.getIndex();
                            IndexLongFieldRange currentTimestampMillisRange = updatedTimestampRanges.get(index);
                            final IndexMetadata indexMetadata = currentState.metadata().index(index);
                            if (currentTimestampMillisRange == null) {
                                currentTimestampMillisRange = indexMetadata.getTimestampRange();
                            }
                            final IndexLongFieldRange newTimestampMillisRange;
                            newTimestampMillisRange = currentTimestampMillisRange.extendWithShardRange(
                                    task.shardId.id(),
                                    indexMetadata.getNumberOfShards(),
                                    task.timestampRange);
                            if (newTimestampMillisRange != currentTimestampMillisRange) {
                                updatedTimestampRanges.put(index, newTimestampMillisRange);
                            }
                        }
                    }
                }
            }
            assert tasksToBeApplied.size() >= shardRoutingsToBeApplied.size();

            ClusterState maybeUpdatedState = currentState;
            try {
                maybeUpdatedState = allocationService.applyStartedShards(currentState, shardRoutingsToBeApplied);

                if (updatedTimestampRanges.isEmpty() == false) {
                    final Metadata.Builder metadataBuilder = Metadata.builder(maybeUpdatedState.metadata());
                    for (Map.Entry<Index, IndexLongFieldRange> updatedTimestampRangeEntry : updatedTimestampRanges.entrySet()) {
                        metadataBuilder.put(IndexMetadata
                                .builder(metadataBuilder.getSafe(updatedTimestampRangeEntry.getKey()))
                                .timestampRange(updatedTimestampRangeEntry.getValue()));
                    }
                    maybeUpdatedState = ClusterState.builder(maybeUpdatedState).metadata(metadataBuilder).build();
                }

                assert assertStartedIndicesHaveCompleteTimestampRanges(maybeUpdatedState);

                builder.successes(tasksToBeApplied);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to apply started shards {}", shardRoutingsToBeApplied), e);
                builder.failures(tasksToBeApplied, e);
            }

            return builder.build(maybeUpdatedState);
        }

        private static boolean assertStartedIndicesHaveCompleteTimestampRanges(ClusterState clusterState) {
            for (ObjectObjectCursor<String, IndexRoutingTable> cursor : clusterState.getRoutingTable().getIndicesRouting()) {
                assert cursor.value.allPrimaryShardsActive() == false
                        || clusterState.metadata().index(cursor.key).getTimestampRange().isComplete()
                        : "index [" + cursor.key + "] should have complete timestamp range, but got "
                        + clusterState.metadata().index(cursor.key).getTimestampRange() + " for " + cursor.value.prettyPrint();
            }
            return true;
        }

        @Override
        public void onFailure(String source, Exception e) {
            if (e instanceof FailedToCommitClusterStateException || e instanceof NotMasterException) {
                logger.debug(() -> new ParameterizedMessage("failure during [{}]", source), e);
            } else {
                logger.error(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
            }
        }

        @Override
        public void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {
            rerouteService.reroute("reroute after starting shards", Priority.NORMAL, ActionListener.wrap(
                r -> logger.trace("reroute after starting shards succeeded"),
                e -> logger.debug("reroute after starting shards failed", e)));
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

        public StartedShardEntry(final ShardId shardId,
                                 final String allocationId,
                                 final long primaryTerm,
                                 final String message,
                                 final ShardLongFieldRange timestampRange) {
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
            return String.format(Locale.ROOT,  "StartedShardEntry{shardId [%s], allocationId [%s], primary term [%d], message [%s]}",
                shardId, allocationId, primaryTerm, message);
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
