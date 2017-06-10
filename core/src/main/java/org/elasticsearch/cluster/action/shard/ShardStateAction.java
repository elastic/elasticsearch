/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.action.shard;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.MasterNodeChangePredicate;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.StaleShard;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Predicate;

public class ShardStateAction extends AbstractComponent {

    public static final String SHARD_STARTED_ACTION_NAME = "internal:cluster/shard/started";
    public static final String SHARD_FAILED_ACTION_NAME = "internal:cluster/shard/failure";

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    @Inject
    public ShardStateAction(Settings settings, ClusterService clusterService, TransportService transportService,
                            AllocationService allocationService, RoutingService routingService, ThreadPool threadPool) {
        super(settings);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;

        transportService.registerRequestHandler(SHARD_STARTED_ACTION_NAME, ShardEntry::new, ThreadPool.Names.SAME, new ShardStartedTransportHandler(clusterService, new ShardStartedClusterStateTaskExecutor(allocationService, logger), logger));
        transportService.registerRequestHandler(SHARD_FAILED_ACTION_NAME, ShardEntry::new, ThreadPool.Names.SAME, new ShardFailedTransportHandler(clusterService, new ShardFailedClusterStateTaskExecutor(allocationService, routingService, logger), logger));
    }

    private void sendShardAction(final String actionName, final ClusterState currentState, final ShardEntry shardEntry, final Listener listener) {
        ClusterStateObserver observer = new ClusterStateObserver(currentState, clusterService, null, logger, threadPool.getThreadContext());
        DiscoveryNode masterNode = currentState.nodes().getMasterNode();
        Predicate<ClusterState> changePredicate = MasterNodeChangePredicate.build(currentState);
        if (masterNode == null) {
            logger.warn("{} no master known for action [{}] for shard entry [{}]", shardEntry.shardId, actionName, shardEntry);
            waitForNewMasterAndRetry(actionName, observer, shardEntry, listener, changePredicate);
        } else {
            logger.debug("{} sending [{}] to [{}] for shard entry [{}]", shardEntry.shardId, actionName, masterNode.getId(), shardEntry);
            transportService.sendRequest(masterNode,
                actionName, shardEntry, new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                    @Override
                    public void handleResponse(TransportResponse.Empty response) {
                        listener.onSuccess();
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        if (isMasterChannelException(exp)) {
                            waitForNewMasterAndRetry(actionName, observer, shardEntry, listener, changePredicate);
                        } else {
                            logger.warn((Supplier<?>) () -> new ParameterizedMessage("{} unexpected failure while sending request [{}] to [{}] for shard entry [{}]", shardEntry.shardId, actionName, masterNode, shardEntry), exp);
                            listener.onFailure(exp instanceof RemoteTransportException ? (Exception) (exp.getCause() instanceof Exception ? exp.getCause() : new ElasticsearchException(exp.getCause())) : exp);
                        }
                    }
                });
        }
    }

    private static Class[] MASTER_CHANNEL_EXCEPTIONS = new Class[]{
        NotMasterException.class,
        ConnectTransportException.class,
        Discovery.FailedToCommitClusterStateException.class
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
     * @param message            the reason for the failure
     * @param failure            the underlying cause of the failure
     * @param listener           callback upon completion of the request
     */
    public void remoteShardFailed(final ShardId shardId, String allocationId, long primaryTerm, final String message, @Nullable final Exception failure, Listener listener) {
        assert primaryTerm > 0L : "primary term should be strictly positive";
        shardFailed(shardId, allocationId, primaryTerm, message, failure, listener, clusterService.state());
    }

    /**
     * Send a shard failed request to the master node to update the cluster state when a shard on the local node failed.
     */
    public void localShardFailed(final ShardRouting shardRouting, final String message, @Nullable final Exception failure, Listener listener) {
        localShardFailed(shardRouting, message, failure, listener, clusterService.state());
    }

    /**
     * Send a shard failed request to the master node to update the cluster state when a shard on the local node failed.
     */
    public void localShardFailed(final ShardRouting shardRouting, final String message, @Nullable final Exception failure, Listener listener,
                                 final ClusterState currentState) {
        shardFailed(shardRouting.shardId(), shardRouting.allocationId().getId(), 0L, message, failure, listener, currentState);
    }

    private void shardFailed(final ShardId shardId, String allocationId, long primaryTerm, final String message,
                             @Nullable final Exception failure, Listener listener, ClusterState currentState) {
        ShardEntry shardEntry = new ShardEntry(shardId, allocationId, primaryTerm, message, failure);
        sendShardAction(SHARD_FAILED_ACTION_NAME, currentState, shardEntry, listener);
    }

    // visible for testing
    protected void waitForNewMasterAndRetry(String actionName, ClusterStateObserver observer, ShardEntry shardEntry, Listener listener, Predicate<ClusterState> changePredicate) {
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                if (logger.isTraceEnabled()) {
                    logger.trace("new cluster state [{}] after waiting for master election to fail shard entry [{}]", state, shardEntry);
                }
                sendShardAction(actionName, state, shardEntry, listener);
            }

            @Override
            public void onClusterServiceClose() {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("{} node closed while execution action [{}] for shard entry [{}]", shardEntry.shardId, actionName, shardEntry), shardEntry.failure);
                listener.onFailure(new NodeClosedException(clusterService.localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                // we wait indefinitely for a new master
                assert false;
            }
        }, changePredicate);
    }

    private static class ShardFailedTransportHandler implements TransportRequestHandler<ShardEntry> {
        private final ClusterService clusterService;
        private final ShardFailedClusterStateTaskExecutor shardFailedClusterStateTaskExecutor;
        private final Logger logger;

        ShardFailedTransportHandler(ClusterService clusterService, ShardFailedClusterStateTaskExecutor shardFailedClusterStateTaskExecutor, Logger logger) {
            this.clusterService = clusterService;
            this.shardFailedClusterStateTaskExecutor = shardFailedClusterStateTaskExecutor;
            this.logger = logger;
        }

        @Override
        public void messageReceived(ShardEntry request, TransportChannel channel) throws Exception {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("{} received shard failed for {}", request.shardId, request), request.failure);
            clusterService.submitStateUpdateTask(
                "shard-failed",
                request,
                ClusterStateTaskConfig.build(Priority.HIGH),
                shardFailedClusterStateTaskExecutor,
                new ClusterStateTaskListener() {
                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.error((Supplier<?>) () -> new ParameterizedMessage("{} unexpected failure while failing shard [{}]", request.shardId, request), e);
                        try {
                            channel.sendResponse(e);
                        } catch (Exception channelException) {
                            channelException.addSuppressed(e);
                            logger.warn((Supplier<?>) () -> new ParameterizedMessage("{} failed to send failure [{}] while failing shard [{}]", request.shardId, e, request), channelException);
                        }
                    }

                    @Override
                    public void onNoLongerMaster(String source) {
                        logger.error("{} no longer master while failing shard [{}]", request.shardId, request);
                        try {
                            channel.sendResponse(new NotMasterException(source));
                        } catch (Exception channelException) {
                            logger.warn((Supplier<?>) () -> new ParameterizedMessage("{} failed to send no longer master while failing shard [{}]", request.shardId, request), channelException);
                        }
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        try {
                            channel.sendResponse(TransportResponse.Empty.INSTANCE);
                        } catch (Exception channelException) {
                            logger.warn((Supplier<?>) () -> new ParameterizedMessage("{} failed to send response while failing shard [{}]", request.shardId, request), channelException);
                        }
                    }
                }
            );
        }
    }

    public static class ShardFailedClusterStateTaskExecutor implements ClusterStateTaskExecutor<ShardEntry> {
        private final AllocationService allocationService;
        private final RoutingService routingService;
        private final Logger logger;

        public ShardFailedClusterStateTaskExecutor(AllocationService allocationService, RoutingService routingService, Logger logger) {
            this.allocationService = allocationService;
            this.routingService = routingService;
            this.logger = logger;
        }

        @Override
        public ClusterTasksResult<ShardEntry> execute(ClusterState currentState, List<ShardEntry> tasks) throws Exception {
            ClusterTasksResult.Builder<ShardEntry> batchResultBuilder = ClusterTasksResult.builder();
            List<ShardEntry> tasksToBeApplied = new ArrayList<>();
            List<FailedShard> failedShardsToBeApplied = new ArrayList<>();
            List<StaleShard> staleShardsToBeApplied = new ArrayList<>();

            for (ShardEntry task : tasks) {
                IndexMetaData indexMetaData = currentState.metaData().index(task.shardId.getIndex());
                if (indexMetaData == null) {
                    // tasks that correspond to non-existent indices are marked as successful
                    logger.debug("{} ignoring shard failed task [{}] (unknown index {})", task.shardId, task, task.shardId.getIndex());
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
                        long currentPrimaryTerm = indexMetaData.primaryTerm(task.shardId.id());
                        if (currentPrimaryTerm != task.primaryTerm) {
                            assert currentPrimaryTerm > task.primaryTerm : "received a primary term with a higher term than in the " +
                                "current cluster state (received [" + task.primaryTerm + "] but current is [" + currentPrimaryTerm + "])";
                            logger.debug("{} failing shard failed task [{}] (primary term {} does not match current term {})", task.shardId,
                                task, task.primaryTerm, indexMetaData.primaryTerm(task.shardId.id()));
                            batchResultBuilder.failure(task, new NoLongerPrimaryShardException(
                                task.shardId,
                                "primary term [" + task.primaryTerm + "] did not match current primary term [" + currentPrimaryTerm + "]"));
                            continue;
                        }
                    }

                    ShardRouting matched = currentState.getRoutingTable().getByAllocationId(task.shardId, task.allocationId);
                    if (matched == null) {
                        Set<String> inSyncAllocationIds = indexMetaData.inSyncAllocationIds(task.shardId.id());
                        // mark shard copies without routing entries that are in in-sync allocations set only as stale if the reason why
                        // they were failed is because a write made it into the primary but not to this copy (which corresponds to
                        // the check "primaryTerm > 0").
                        if (task.primaryTerm > 0 && inSyncAllocationIds.contains(task.allocationId)) {
                            logger.debug("{} marking shard {} as stale (shard failed task: [{}])", task.shardId, task.allocationId, task);
                            tasksToBeApplied.add(task);
                            staleShardsToBeApplied.add(new StaleShard(task.shardId, task.allocationId));
                        } else {
                            // tasks that correspond to non-existent shards are marked as successful
                            logger.debug("{} ignoring shard failed task [{}] (shard does not exist anymore)", task.shardId, task);
                            batchResultBuilder.success(task);
                        }
                    } else {
                        // failing a shard also possibly marks it as stale (see IndexMetaDataUpdater)
                        logger.debug("{} failing shard {} (shard failed task: [{}])", task.shardId, matched, task);
                        tasksToBeApplied.add(task);
                        failedShardsToBeApplied.add(new FailedShard(matched, task.message, task.failure));
                    }
                }
            }
            assert tasksToBeApplied.size() == failedShardsToBeApplied.size() + staleShardsToBeApplied.size();

            ClusterState maybeUpdatedState = currentState;
            try {
                maybeUpdatedState = applyFailedShards(currentState, failedShardsToBeApplied, staleShardsToBeApplied);
                batchResultBuilder.successes(tasksToBeApplied);
            } catch (Exception e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to apply failed shards {}", failedShardsToBeApplied), e);
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
                String reason = String.format(Locale.ROOT, "[%d] unassigned shards after failing shards", numberOfUnassignedShards);
                if (logger.isTraceEnabled()) {
                    logger.trace("{}, scheduling a reroute", reason);
                }
                routingService.reroute(reason);
            }
        }
    }

    public void shardStarted(final ShardRouting shardRouting, final String message, Listener listener) {
        shardStarted(shardRouting, message, listener, clusterService.state());
    }
    public void shardStarted(final ShardRouting shardRouting, final String message, Listener listener, ClusterState currentState) {
        ShardEntry shardEntry = new ShardEntry(shardRouting.shardId(), shardRouting.allocationId().getId(), 0L, message, null);
        sendShardAction(SHARD_STARTED_ACTION_NAME, currentState, shardEntry, listener);
    }

    private static class ShardStartedTransportHandler implements TransportRequestHandler<ShardEntry> {
        private final ClusterService clusterService;
        private final ShardStartedClusterStateTaskExecutor shardStartedClusterStateTaskExecutor;
        private final Logger logger;

        ShardStartedTransportHandler(ClusterService clusterService, ShardStartedClusterStateTaskExecutor shardStartedClusterStateTaskExecutor, Logger logger) {
            this.clusterService = clusterService;
            this.shardStartedClusterStateTaskExecutor = shardStartedClusterStateTaskExecutor;
            this.logger = logger;
        }

        @Override
        public void messageReceived(ShardEntry request, TransportChannel channel) throws Exception {
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

    public static class ShardStartedClusterStateTaskExecutor implements ClusterStateTaskExecutor<ShardEntry>, ClusterStateTaskListener {
        private final AllocationService allocationService;
        private final Logger logger;

        public ShardStartedClusterStateTaskExecutor(AllocationService allocationService, Logger logger) {
            this.allocationService = allocationService;
            this.logger = logger;
        }

        @Override
        public ClusterTasksResult<ShardEntry> execute(ClusterState currentState, List<ShardEntry> tasks) throws Exception {
            ClusterTasksResult.Builder<ShardEntry> builder = ClusterTasksResult.builder();
            List<ShardEntry> tasksToBeApplied = new ArrayList<>();
            List<ShardRouting> shardRoutingsToBeApplied = new ArrayList<>(tasks.size());
            Set<ShardRouting> seenShardRoutings = new HashSet<>(); // to prevent duplicates
            for (ShardEntry task : tasks) {
                assert task.primaryTerm == 0L : "shard is only started by itself: " + task;

                ShardRouting matched = currentState.getRoutingTable().getByAllocationId(task.shardId, task.allocationId);
                if (matched == null) {
                    // tasks that correspond to non-existent shards are marked as successful. The reason is that we resend shard started
                    // events on every cluster state publishing that does not contain the shard as started yet. This means that old stale
                    // requests might still be in flight even after the shard has already been started or failed on the master. We just
                    // ignore these requests for now.
                    logger.debug("{} ignoring shard started task [{}] (shard does not exist anymore)", task.shardId, task);
                    builder.success(task);
                } else {
                    if (matched.initializing() == false) {
                        assert matched.active() : "expected active shard routing for task " + task + " but found " + matched;
                        // same as above, this might have been a stale in-flight request, so we just ignore.
                        logger.debug("{} ignoring shard started task [{}] (shard exists but is not initializing: {})", task.shardId, task,
                            matched);
                        builder.success(task);
                    } else {
                        // remove duplicate actions as allocation service expects a clean list without duplicates
                        if (seenShardRoutings.contains(matched)) {
                            logger.trace("{} ignoring shard started task [{}] (already scheduled to start {})", task.shardId, task, matched);
                            tasksToBeApplied.add(task);
                        } else {
                            logger.debug("{} starting shard {} (shard started task: [{}])", task.shardId, matched, task);
                            tasksToBeApplied.add(task);
                            shardRoutingsToBeApplied.add(matched);
                            seenShardRoutings.add(matched);
                        }
                    }
                }
            }
            assert tasksToBeApplied.size() >= shardRoutingsToBeApplied.size();

            ClusterState maybeUpdatedState = currentState;
            try {
                maybeUpdatedState = allocationService.applyStartedShards(currentState, shardRoutingsToBeApplied);
                builder.successes(tasksToBeApplied);
            } catch (Exception e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to apply started shards {}", shardRoutingsToBeApplied), e);
                builder.failures(tasksToBeApplied, e);
            }

            return builder.build(maybeUpdatedState);
        }

        @Override
        public void onFailure(String source, Exception e) {
            logger.error((Supplier<?>) () -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
        }
    }

    public static class ShardEntry extends TransportRequest {
        ShardId shardId;
        String allocationId;
        long primaryTerm;
        String message;
        Exception failure;

        public ShardEntry() {
        }

        public ShardEntry(ShardId shardId, String allocationId, long primaryTerm, String message, @Nullable Exception failure) {
            this.shardId = shardId;
            this.allocationId = allocationId;
            this.primaryTerm = primaryTerm;
            this.message = message;
            this.failure = failure;
        }

        public ShardId getShardId() {
            return shardId;
        }

        public String getAllocationId() {
            return allocationId;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardId = ShardId.readShardId(in);
            allocationId = in.readString();
            primaryTerm = in.readVLong();
            message = in.readString();
            failure = in.readException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeString(allocationId);
            out.writeVLong(primaryTerm);
            out.writeString(message);
            out.writeException(failure);
        }

        @Override
        public String toString() {
            List<String> components = new ArrayList<>(4);
            components.add("shard id [" + shardId + "]");
            components.add("allocation id [" + allocationId + "]");
            components.add("primary term [" + primaryTerm + "]");
            components.add("message [" + message + "]");
            if (failure != null) {
                components.add("failure [" + ExceptionsHelper.detailedMessage(failure) + "]");
            }
            return String.join(", ", components);
        }
    }

    public interface Listener {

        default void onSuccess() {
        }

        /**
         * Notification for non-channel exceptions that are not handled
         * by {@link ShardStateAction}.
         *
         * The exceptions that are handled by {@link ShardStateAction}
         * are:
         *  - {@link NotMasterException}
         *  - {@link NodeDisconnectedException}
         *  - {@link Discovery.FailedToCommitClusterStateException}
         *
         * Any other exception is communicated to the requester via
         * this notification.
         *
         * @param e the unexpected cause of the failure on the master
         */
        default void onFailure(final Exception e) {
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
