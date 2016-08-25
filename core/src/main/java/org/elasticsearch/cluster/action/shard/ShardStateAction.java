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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.MasterNodeChangePredicate;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
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

    private void sendShardAction(final String actionName, final ClusterStateObserver observer, final ShardEntry shardEntry, final Listener listener) {
        DiscoveryNode masterNode = observer.observedState().nodes().getMasterNode();
        if (masterNode == null) {
            logger.warn("{} no master known for action [{}] for shard entry [{}]", shardEntry.shardId, actionName, shardEntry);
            waitForNewMasterAndRetry(actionName, observer, shardEntry, listener);
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
                            waitForNewMasterAndRetry(actionName, observer, shardEntry, listener);
                        } else {
                            logger.warn("{} unexpected failure while sending request [{}] to [{}] for shard entry [{}]", exp, shardEntry.shardId, actionName, masterNode, shardEntry);
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
     * Send a shard failed request to the master node to update the cluster state with the failure of a shard on another node.
     *
     * @param shardRouting       the shard to fail
     * @param primaryTerm        the primary term associated with the primary shard that is failing the shard.
     * @param message            the reason for the failure
     * @param failure            the underlying cause of the failure
     * @param listener           callback upon completion of the request
     */
    public void remoteShardFailed(final ShardRouting shardRouting, long primaryTerm, final String message, @Nullable final Exception failure, Listener listener) {
        assert primaryTerm > 0L : "primary term should be strictly positive";
        shardFailed(shardRouting, primaryTerm, message, failure, listener);
    }

    /**
     * Send a shard failed request to the master node to update the cluster state when a shard on the local node failed.
     */
    public void localShardFailed(final ShardRouting shardRouting, final String message, @Nullable final Exception failure, Listener listener) {
        shardFailed(shardRouting, 0L, message, failure, listener);
    }

    private void shardFailed(final ShardRouting shardRouting, long primaryTerm, final String message, @Nullable final Exception failure, Listener listener) {
        ClusterStateObserver observer = new ClusterStateObserver(clusterService, null, logger, threadPool.getThreadContext());
        ShardEntry shardEntry = new ShardEntry(shardRouting.shardId(), shardRouting.allocationId().getId(), primaryTerm, message, failure);
        sendShardAction(SHARD_FAILED_ACTION_NAME, observer, shardEntry, listener);
    }

    // visible for testing
    protected void waitForNewMasterAndRetry(String actionName, ClusterStateObserver observer, ShardEntry shardEntry, Listener listener) {
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                if (logger.isTraceEnabled()) {
                    logger.trace("new cluster state [{}] after waiting for master election to fail shard entry [{}]", state.prettyPrint(), shardEntry);
                }
                sendShardAction(actionName, observer, shardEntry, listener);
            }

            @Override
            public void onClusterServiceClose() {
                logger.warn("{} node closed while execution action [{}] for shard entry [{}]", shardEntry.failure, shardEntry.shardId, actionName, shardEntry);
                listener.onFailure(new NodeClosedException(clusterService.localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                // we wait indefinitely for a new master
                assert false;
            }
        }, MasterNodeChangePredicate.INSTANCE);
    }

    private static class ShardFailedTransportHandler implements TransportRequestHandler<ShardEntry> {
        private final ClusterService clusterService;
        private final ShardFailedClusterStateTaskExecutor shardFailedClusterStateTaskExecutor;
        private final ESLogger logger;

        public ShardFailedTransportHandler(ClusterService clusterService, ShardFailedClusterStateTaskExecutor shardFailedClusterStateTaskExecutor, ESLogger logger) {
            this.clusterService = clusterService;
            this.shardFailedClusterStateTaskExecutor = shardFailedClusterStateTaskExecutor;
            this.logger = logger;
        }

        @Override
        public void messageReceived(ShardEntry request, TransportChannel channel) throws Exception {
            logger.warn("{} received shard failed for {}", request.failure, request.shardId, request);
            clusterService.submitStateUpdateTask(
                "shard-failed",
                request,
                ClusterStateTaskConfig.build(Priority.HIGH),
                shardFailedClusterStateTaskExecutor,
                new ClusterStateTaskListener() {
                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.error("{} unexpected failure while failing shard [{}]", e, request.shardId, request);
                        try {
                            channel.sendResponse(e);
                        } catch (Exception channelException) {
                            channelException.addSuppressed(e);
                            logger.warn("{} failed to send failure [{}] while failing shard [{}]", channelException, request.shardId, e, request);
                        }
                    }

                    @Override
                    public void onNoLongerMaster(String source) {
                        logger.error("{} no longer master while failing shard [{}]", request.shardId, request);
                        try {
                            channel.sendResponse(new NotMasterException(source));
                        } catch (Exception channelException) {
                            logger.warn("{} failed to send no longer master while failing shard [{}]", channelException, request.shardId, request);
                        }
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        try {
                            channel.sendResponse(TransportResponse.Empty.INSTANCE);
                        } catch (Exception channelException) {
                            logger.warn("{} failed to send response while failing shard [{}]", channelException, request.shardId, request);
                        }
                    }
                }
            );
        }
    }

    public static class ShardFailedClusterStateTaskExecutor implements ClusterStateTaskExecutor<ShardEntry> {
        private final AllocationService allocationService;
        private final RoutingService routingService;
        private final ESLogger logger;

        public ShardFailedClusterStateTaskExecutor(AllocationService allocationService, RoutingService routingService, ESLogger logger) {
            this.allocationService = allocationService;
            this.routingService = routingService;
            this.logger = logger;
        }

        @Override
        public BatchResult<ShardEntry> execute(ClusterState currentState, List<ShardEntry> tasks) throws Exception {
            BatchResult.Builder<ShardEntry> batchResultBuilder = BatchResult.builder();
            List<ShardEntry> tasksToBeApplied = new ArrayList<>();
            List<FailedRerouteAllocation.FailedShard> shardRoutingsToBeApplied = new ArrayList<>();
            Set<ShardRouting> seenShardRoutings = new HashSet<>(); // to prevent duplicates

            for (ShardEntry task : tasks) {
                IndexMetaData indexMetaData = currentState.metaData().index(task.shardId.getIndex());
                if (indexMetaData == null) {
                    // tasks that correspond to non-existent shards are marked as successful
                    logger.debug("{} ignoring shard failed task [{}] (unknown index {})", task.shardId, task, task.shardId.getIndex());
                    batchResultBuilder.success(task);
                } else {
                    // non-local requests
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
                        // tasks that correspond to non-existent shards are marked as successful
                        logger.debug("{} ignoring shard failed task [{}] (shard does not exist anymore)", task.shardId, task);
                        batchResultBuilder.success(task);
                    } else {
                        // remove duplicate actions as allocation service expects a clean list without duplicates
                        if (seenShardRoutings.contains(matched)) {
                            logger.trace("{} ignoring shard failed task [{}] (already scheduled to fail {})", task.shardId, task, matched);
                            tasksToBeApplied.add(task);
                        } else {
                            logger.debug("{} failing shard {} (shard failed task: [{}])", task.shardId, matched, task);
                            tasksToBeApplied.add(task);
                            shardRoutingsToBeApplied.add(new FailedRerouteAllocation.FailedShard(matched, task.message, task.failure));
                            seenShardRoutings.add(matched);
                        }
                    }
                }
            }
            assert tasksToBeApplied.size() >= shardRoutingsToBeApplied.size();

            ClusterState maybeUpdatedState = currentState;
            try {
                RoutingAllocation.Result result = applyFailedShards(currentState, shardRoutingsToBeApplied);
                if (result.changed()) {
                    maybeUpdatedState = ClusterState.builder(currentState).routingResult(result).build();
                }
                batchResultBuilder.successes(tasksToBeApplied);
            } catch (Exception e) {
                logger.warn("failed to apply failed shards {}", e, shardRoutingsToBeApplied);
                // failures are communicated back to the requester
                // cluster state will not be updated in this case
                batchResultBuilder.failures(tasksToBeApplied, e);
            }

            return batchResultBuilder.build(maybeUpdatedState);
        }

        // visible for testing
        RoutingAllocation.Result applyFailedShards(ClusterState currentState, List<FailedRerouteAllocation.FailedShard> failedShards) {
            return allocationService.applyFailedShards(currentState, failedShards);
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
        ClusterStateObserver observer = new ClusterStateObserver(clusterService, null, logger, threadPool.getThreadContext());
        ShardEntry shardEntry = new ShardEntry(shardRouting.shardId(), shardRouting.allocationId().getId(), 0L, message, null);
        sendShardAction(SHARD_STARTED_ACTION_NAME, observer, shardEntry, listener);
    }

    private static class ShardStartedTransportHandler implements TransportRequestHandler<ShardEntry> {
        private final ClusterService clusterService;
        private final ShardStartedClusterStateTaskExecutor shardStartedClusterStateTaskExecutor;
        private final ESLogger logger;

        public ShardStartedTransportHandler(ClusterService clusterService, ShardStartedClusterStateTaskExecutor shardStartedClusterStateTaskExecutor, ESLogger logger) {
            this.clusterService = clusterService;
            this.shardStartedClusterStateTaskExecutor = shardStartedClusterStateTaskExecutor;
            this.logger = logger;
        }

        @Override
        public void messageReceived(ShardEntry request, TransportChannel channel) throws Exception {
            logger.debug("{} received shard started for [{}]", request.shardId, request);
            clusterService.submitStateUpdateTask(
                "shard-started",
                request,
                ClusterStateTaskConfig.build(Priority.URGENT),
                shardStartedClusterStateTaskExecutor,
                shardStartedClusterStateTaskExecutor);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    public static class ShardStartedClusterStateTaskExecutor implements ClusterStateTaskExecutor<ShardEntry>, ClusterStateTaskListener {
        private final AllocationService allocationService;
        private final ESLogger logger;

        public ShardStartedClusterStateTaskExecutor(AllocationService allocationService, ESLogger logger) {
            this.allocationService = allocationService;
            this.logger = logger;
        }

        @Override
        public BatchResult<ShardEntry> execute(ClusterState currentState, List<ShardEntry> tasks) throws Exception {
            BatchResult.Builder<ShardEntry> builder = BatchResult.builder();
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
                RoutingAllocation.Result result =
                    allocationService.applyStartedShards(currentState, shardRoutingsToBeApplied, true);
                if (result.changed()) {
                    maybeUpdatedState = ClusterState.builder(currentState).routingResult(result).build();
                }
                builder.successes(tasksToBeApplied);
            } catch (Exception e) {
                logger.warn("failed to apply started shards {}", e, shardRoutingsToBeApplied);
                builder.failures(tasksToBeApplied, e);
            }

            return builder.build(maybeUpdatedState);
        }

        @Override
        public void onFailure(String source, Exception e) {
            logger.error("unexpected failure during [{}]", e, source);
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
