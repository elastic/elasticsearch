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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
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
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

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

        transportService.registerRequestHandler(SHARD_STARTED_ACTION_NAME, ShardRoutingEntry::new, ThreadPool.Names.SAME, new ShardStartedTransportHandler(clusterService, new ShardStartedClusterStateTaskExecutor(allocationService, logger), logger));
        transportService.registerRequestHandler(SHARD_FAILED_ACTION_NAME, ShardRoutingEntry::new, ThreadPool.Names.SAME, new ShardFailedTransportHandler(clusterService, new ShardFailedClusterStateTaskExecutor(allocationService, routingService, logger), logger));
    }

    private void sendShardAction(final String actionName, final ClusterStateObserver observer, final ShardRoutingEntry shardRoutingEntry, final Listener listener) {
        DiscoveryNode masterNode = observer.observedState().nodes().getMasterNode();
        if (masterNode == null) {
            logger.warn("{} no master known for action [{}] for shard [{}]", shardRoutingEntry.getShardRouting().shardId(), actionName, shardRoutingEntry.getShardRouting());
            waitForNewMasterAndRetry(actionName, observer, shardRoutingEntry, listener);
        } else {
            logger.debug("{} sending [{}] to [{}] for shard [{}]", shardRoutingEntry.getShardRouting().shardId(), actionName, masterNode.getId(), shardRoutingEntry);
            transportService.sendRequest(masterNode,
                actionName, shardRoutingEntry, new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                    @Override
                    public void handleResponse(TransportResponse.Empty response) {
                        listener.onSuccess();
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        if (isMasterChannelException(exp)) {
                            waitForNewMasterAndRetry(actionName, observer, shardRoutingEntry, listener);
                        } else {
                            logger.warn("{} unexpected failure while sending request [{}] to [{}] for shard [{}]", exp, shardRoutingEntry.getShardRouting().shardId(), actionName, masterNode, shardRoutingEntry);
                            listener.onFailure(exp instanceof RemoteTransportException ? exp.getCause() : exp);
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
     * Send a shard failed request to the master node to update the
     * cluster state.
     *
     * @param shardRouting       the shard to fail
     * @param sourceShardRouting the source shard requesting the failure (must be the shard itself, or the primary shard)
     * @param message            the reason for the failure
     * @param failure            the underlying cause of the failure
     * @param listener           callback upon completion of the request
     */
    public void shardFailed(final ShardRouting shardRouting, ShardRouting sourceShardRouting, final String message, @Nullable final Throwable failure, Listener listener) {
        ClusterStateObserver observer = new ClusterStateObserver(clusterService, null, logger, threadPool.getThreadContext());
        ShardRoutingEntry shardRoutingEntry = new ShardRoutingEntry(shardRouting, sourceShardRouting, message, failure);
        sendShardAction(SHARD_FAILED_ACTION_NAME, observer, shardRoutingEntry, listener);
    }

    // visible for testing
    protected void waitForNewMasterAndRetry(String actionName, ClusterStateObserver observer, ShardRoutingEntry shardRoutingEntry, Listener listener) {
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                if (logger.isTraceEnabled()) {
                    logger.trace("new cluster state [{}] after waiting for master election to fail shard [{}]", state.prettyPrint(), shardRoutingEntry);
                }
                sendShardAction(actionName, observer, shardRoutingEntry, listener);
            }

            @Override
            public void onClusterServiceClose() {
                logger.warn("{} node closed while execution action [{}] for shard [{}]", shardRoutingEntry.failure, shardRoutingEntry.getShardRouting().shardId(), actionName, shardRoutingEntry.getShardRouting());
                listener.onFailure(new NodeClosedException(clusterService.localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                // we wait indefinitely for a new master
                assert false;
            }
        }, MasterNodeChangePredicate.INSTANCE);
    }

    private static class ShardFailedTransportHandler implements TransportRequestHandler<ShardRoutingEntry> {
        private final ClusterService clusterService;
        private final ShardFailedClusterStateTaskExecutor shardFailedClusterStateTaskExecutor;
        private final ESLogger logger;

        public ShardFailedTransportHandler(ClusterService clusterService, ShardFailedClusterStateTaskExecutor shardFailedClusterStateTaskExecutor, ESLogger logger) {
            this.clusterService = clusterService;
            this.shardFailedClusterStateTaskExecutor = shardFailedClusterStateTaskExecutor;
            this.logger = logger;
        }

        @Override
        public void messageReceived(ShardRoutingEntry request, TransportChannel channel) throws Exception {
            logger.warn("{} received shard failed for {}", request.failure, request.shardRouting.shardId(), request);
            clusterService.submitStateUpdateTask(
                "shard-failed (" + request.shardRouting + "), message [" + request.message + "]",
                request,
                ClusterStateTaskConfig.build(Priority.HIGH),
                shardFailedClusterStateTaskExecutor,
                new ClusterStateTaskListener() {
                    @Override
                    public void onFailure(String source, Throwable t) {
                        logger.error("{} unexpected failure while failing shard [{}]", t, request.shardRouting.shardId(), request.shardRouting);
                        try {
                            channel.sendResponse(t);
                        } catch (Throwable channelThrowable) {
                            logger.warn("{} failed to send failure [{}] while failing shard [{}]", channelThrowable, request.shardRouting.shardId(), t, request.shardRouting);
                        }
                    }

                    @Override
                    public void onNoLongerMaster(String source) {
                        logger.error("{} no longer master while failing shard [{}]", request.shardRouting.shardId(), request.shardRouting);
                        try {
                            channel.sendResponse(new NotMasterException(source));
                        } catch (Throwable channelThrowable) {
                            logger.warn("{} failed to send no longer master while failing shard [{}]", channelThrowable, request.shardRouting.shardId(), request.shardRouting);
                        }
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        try {
                            channel.sendResponse(TransportResponse.Empty.INSTANCE);
                        } catch (Throwable channelThrowable) {
                            logger.warn("{} failed to send response while failing shard [{}]", channelThrowable, request.shardRouting.shardId(), request.shardRouting);
                        }
                    }
                }
            );
        }
    }

    static class ShardFailedClusterStateTaskExecutor implements ClusterStateTaskExecutor<ShardRoutingEntry> {
        private final AllocationService allocationService;
        private final RoutingService routingService;
        private final ESLogger logger;

        ShardFailedClusterStateTaskExecutor(AllocationService allocationService, RoutingService routingService, ESLogger logger) {
            this.allocationService = allocationService;
            this.routingService = routingService;
            this.logger = logger;
        }

        @Override
        public BatchResult<ShardRoutingEntry> execute(ClusterState currentState, List<ShardRoutingEntry> tasks) throws Exception {
            BatchResult.Builder<ShardRoutingEntry> batchResultBuilder = BatchResult.builder();

            // partition tasks into those that correspond to shards
            // that exist versus do not exist
            Map<ValidationResult, List<ShardRoutingEntry>> partition =
                tasks.stream().collect(Collectors.groupingBy(task -> validateTask(currentState, task)));

            // tasks that correspond to non-existent shards are marked
            // as successful
            batchResultBuilder.successes(partition.getOrDefault(ValidationResult.SHARD_MISSING, Collections.emptyList()));

            ClusterState maybeUpdatedState = currentState;
            List<ShardRoutingEntry> tasksToFail = partition.getOrDefault(ValidationResult.VALID, Collections.emptyList());
            try {
                List<FailedRerouteAllocation.FailedShard> failedShards =
                    tasksToFail
                        .stream()
                        .map(task -> new FailedRerouteAllocation.FailedShard(task.shardRouting, task.message, task.failure))
                        .collect(Collectors.toList());
                RoutingAllocation.Result result = applyFailedShards(currentState, failedShards);
                if (result.changed()) {
                    maybeUpdatedState = ClusterState.builder(currentState).routingResult(result).build();
                }
                batchResultBuilder.successes(tasksToFail);
            } catch (Throwable t) {
                // failures are communicated back to the requester
                // cluster state will not be updated in this case
                batchResultBuilder.failures(tasksToFail, t);
            }

            partition
                .getOrDefault(ValidationResult.SOURCE_INVALID, Collections.emptyList())
                .forEach(task -> batchResultBuilder.failure(
                    task,
                    new NoLongerPrimaryShardException(
                        task.getShardRouting().shardId(),
                        "source shard [" + task.sourceShardRouting + "] is neither the local allocation nor the primary allocation")
                ));

            return batchResultBuilder.build(maybeUpdatedState);
        }

        // visible for testing
        RoutingAllocation.Result applyFailedShards(ClusterState currentState, List<FailedRerouteAllocation.FailedShard> failedShards) {
            return allocationService.applyFailedShards(currentState, failedShards);
        }

        private enum ValidationResult {
            VALID,
            SOURCE_INVALID,
            SHARD_MISSING
        }

        private ValidationResult validateTask(ClusterState currentState, ShardRoutingEntry task) {

            // non-local requests
            if (!task.shardRouting.isSameAllocation(task.sourceShardRouting)) {
                IndexShardRoutingTable indexShard = currentState.getRoutingTable().shardRoutingTableOrNull(task.shardRouting.shardId());
                if (indexShard == null) {
                    return ValidationResult.SOURCE_INVALID;
                }
                ShardRouting primaryShard = indexShard.primaryShard();
                if (primaryShard == null || !primaryShard.isSameAllocation(task.sourceShardRouting)) {
                    return ValidationResult.SOURCE_INVALID;
                }
            }

            RoutingNode routingNode = currentState.getRoutingNodes().node(task.getShardRouting().currentNodeId());
            if (routingNode != null) {
                ShardRouting maybe = routingNode.getByShardId(task.getShardRouting().shardId());
                if (maybe != null && maybe.isSameAllocation(task.getShardRouting())) {
                    return ValidationResult.VALID;
                }
            }
            return ValidationResult.SHARD_MISSING;
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
        ShardRoutingEntry shardRoutingEntry = new ShardRoutingEntry(shardRouting, shardRouting, message, null);
        sendShardAction(SHARD_STARTED_ACTION_NAME, observer, shardRoutingEntry, listener);
    }

    private static class ShardStartedTransportHandler implements TransportRequestHandler<ShardRoutingEntry> {
        private final ClusterService clusterService;
        private final ShardStartedClusterStateTaskExecutor shardStartedClusterStateTaskExecutor;
        private final ESLogger logger;

        public ShardStartedTransportHandler(ClusterService clusterService, ShardStartedClusterStateTaskExecutor shardStartedClusterStateTaskExecutor, ESLogger logger) {
            this.clusterService = clusterService;
            this.shardStartedClusterStateTaskExecutor = shardStartedClusterStateTaskExecutor;
            this.logger = logger;
        }

        @Override
        public void messageReceived(ShardRoutingEntry request, TransportChannel channel) throws Exception {
            logger.debug("{} received shard started for [{}]", request.shardRouting.shardId(), request);
            clusterService.submitStateUpdateTask(
                "shard-started (" + request.shardRouting + "), reason [" + request.message + "]",
                request,
                ClusterStateTaskConfig.build(Priority.URGENT),
                shardStartedClusterStateTaskExecutor,
                shardStartedClusterStateTaskExecutor);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    private static class ShardStartedClusterStateTaskExecutor implements ClusterStateTaskExecutor<ShardRoutingEntry>, ClusterStateTaskListener {
        private final AllocationService allocationService;
        private final ESLogger logger;

        public ShardStartedClusterStateTaskExecutor(AllocationService allocationService, ESLogger logger) {
            this.allocationService = allocationService;
            this.logger = logger;
        }

        @Override
        public BatchResult<ShardRoutingEntry> execute(ClusterState currentState, List<ShardRoutingEntry> tasks) throws Exception {
            BatchResult.Builder<ShardRoutingEntry> builder = BatchResult.builder();
            List<ShardRouting> shardRoutingsToBeApplied = new ArrayList<>(tasks.size());
            for (ShardRoutingEntry task : tasks) {
                shardRoutingsToBeApplied.add(task.shardRouting);
            }
            ClusterState maybeUpdatedState = currentState;
            try {
                RoutingAllocation.Result result =
                    allocationService.applyStartedShards(currentState, shardRoutingsToBeApplied, true);
                if (result.changed()) {
                    maybeUpdatedState = ClusterState.builder(currentState).routingResult(result).build();
                }
                builder.successes(tasks);
            } catch (Throwable t) {
                builder.failures(tasks, t);
            }

            return builder.build(maybeUpdatedState);
        }

        @Override
        public void onFailure(String source, Throwable t) {
            logger.error("unexpected failure during [{}]", t, source);
        }
    }

    public static class ShardRoutingEntry extends TransportRequest {
        ShardRouting shardRouting;
        ShardRouting sourceShardRouting;
        String message;
        Throwable failure;

        public ShardRoutingEntry() {
        }

        ShardRoutingEntry(ShardRouting shardRouting, ShardRouting sourceShardRouting, String message, @Nullable Throwable failure) {
            this.shardRouting = shardRouting;
            this.sourceShardRouting = sourceShardRouting;
            this.message = message;
            this.failure = failure;
        }

        public ShardRouting getShardRouting() {
            return shardRouting;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardRouting = new ShardRouting(in);
            sourceShardRouting = new ShardRouting(in);
            message = in.readString();
            failure = in.readThrowable();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardRouting.writeTo(out);
            sourceShardRouting.writeTo(out);
            out.writeString(message);
            out.writeThrowable(failure);
        }

        @Override
        public String toString() {
            List<String> components = new ArrayList<>(4);
            components.add("target shard [" + shardRouting + "]");
            components.add("source shard [" + sourceShardRouting + "]");
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
         * @param t the unexpected cause of the failure on the master
         */
        default void onFailure(final Throwable t) {
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
