/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.action.shard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ResultDeduplicator;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

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
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            StartedShardEntry::new,
            new ShardStartedTransportHandler(
                clusterService,
                new ShardStartedTaskExecutor(clusterService.getClusterSettings(), allocationService, rerouteService)
            )
        );
        transportService.registerRequestHandler(
            SHARD_FAILED_ACTION_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            FailedShardEntry::new,
            new ShardFailedTransportHandler(clusterService, new ShardFailedTaskExecutor(allocationService, rerouteService))
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
        if (masterNode == null) {
            logger.warn("no master known for action [{}] for shard entry [{}]", actionName, request);
            waitForNewMasterAndRetry(actionName, observer, request, listener);
        } else {
            logger.debug("sending [{}] to [{}] for shard entry [{}]", actionName, masterNode.getId(), request);
            transportService.sendRequest(
                masterNode,
                actionName,
                request,
                TransportResponseHandler.empty(TransportResponseHandler.TRANSPORT_WORKER, listener.delegateResponse((l, exp) -> {
                    if (isMasterChannelException(exp)) {
                        waitForNewMasterAndRetry(actionName, observer, request, listener);
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
                                ? (exp.getCause() instanceof Exception cause ? cause : new ElasticsearchException(exp.getCause()))
                                : exp
                        );
                    }
                }))
            );
        }
    }

    private static final Class<?>[] MASTER_CHANNEL_EXCEPTIONS = new Class<?>[] {
        NotMasterException.class,
        ConnectTransportException.class,
        FailedToCommitClusterStateException.class };

    private static boolean isMasterChannelException(Throwable exp) {
        return ExceptionsHelper.unwrap(exp, MASTER_CHANNEL_EXCEPTIONS) != null;
    }

    /**
     * Send a shard failed request to the master node to update the cluster state with the failure of a shard on another node. This means
     * that the shard should be failed because a write made it into the primary but was not replicated to this shard copy. If the shard
     * does not exist anymore but still has an entry in the in-sync set, remove its allocation id from the in-sync set.
     *
     * @param shardId      shard id of the shard to fail
     * @param allocationId allocation id of the shard to fail
     * @param primaryTerm  the primary term associated with the primary shard that is failing the shard. Must be strictly positive.
     * @param markAsStale  whether or not to mark a failing shard as stale (eg. removing from in-sync set) when failing the shard.
     * @param message      the reason for the failure
     * @param failure      the underlying cause of the failure
     * @param listener     callback upon completion of the request
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
     * {@link IndicesClusterStateService} in case of a master failover to enable sending fresh requests
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
        ActionListener<Void> listener
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
        }, ClusterStateObserver.NON_NULL_MASTER_PREDICATE);
    }

    // TODO: Make this a TransportMasterNodeAction and remove duplication of master failover retrying from upstream code
    private static class ShardFailedTransportHandler implements TransportRequestHandler<FailedShardEntry> {
        private final MasterServiceTaskQueue<ShardFailedTaskExecutor.Task> taskQueue;

        ShardFailedTransportHandler(ClusterService clusterService, ShardFailedTaskExecutor shardFailedTaskExecutor) {
            taskQueue = clusterService.createTaskQueue("shard-failed", Priority.HIGH, shardFailedTaskExecutor);
        }

        @Override
        public void messageReceived(FailedShardEntry request, TransportChannel channel, Task task) {
            logger.debug(() -> format("%s received shard failed for [%s]", request.getShardId(), request), request.failure);
            taskQueue.submitTask(
                "shard-failed " + request.toStringNoFailureStackTrace(),
                new ShardFailedTaskExecutor.Task(
                    request,
                    new ChannelActionListener<>(channel).map(ignored -> ActionResponse.Empty.INSTANCE)
                ),
                null
            );
        }
    }

    public void shardStarted(
        final ShardRouting shardRouting,
        final long primaryTerm,
        final String message,
        final ShardLongFieldRange timestampRange,
        final ShardLongFieldRange eventIngestedRange,
        final ActionListener<Void> listener
    ) {
        shardStarted(shardRouting, primaryTerm, message, timestampRange, eventIngestedRange, listener, clusterService.state());
    }

    public void shardStarted(
        final ShardRouting shardRouting,
        final long primaryTerm,
        final String message,
        final ShardLongFieldRange timestampRange,
        final ShardLongFieldRange eventIngestedRange,
        final ActionListener<Void> listener,
        final ClusterState currentState
    ) {
        remoteShardStateUpdateDeduplicator.executeOnce(
            new StartedShardEntry(
                shardRouting.shardId(),
                shardRouting.allocationId().getId(),
                primaryTerm,
                message,
                timestampRange,
                eventIngestedRange
            ),
            listener,
            (req, l) -> sendShardAction(SHARD_STARTED_ACTION_NAME, currentState, req, l)
        );
    }

    // TODO: Make this a TransportMasterNodeAction and remove duplication of master failover retrying from upstream code
    private static class ShardStartedTransportHandler implements TransportRequestHandler<StartedShardEntry> {
        private final MasterServiceTaskQueue<ShardStartedTaskExecutor.Task> taskQueue;

        ShardStartedTransportHandler(ClusterService clusterService, ShardStartedTaskExecutor taskExecutor) {
            taskQueue = clusterService.createTaskQueue("shard-started", Priority.URGENT, taskExecutor);
        }

        @Override
        public void messageReceived(StartedShardEntry request, TransportChannel channel, Task task) {
            logger.debug("{} received shard started for [{}]", request.shardId, request);
            taskQueue.submitTask(
                "shard-started " + request,
                new ShardStartedTaskExecutor.Task(
                    request,
                    new ChannelActionListener<>(channel).map(ignored -> ActionResponse.Empty.INSTANCE)
                ),
                null
            );
        }
    }

}
