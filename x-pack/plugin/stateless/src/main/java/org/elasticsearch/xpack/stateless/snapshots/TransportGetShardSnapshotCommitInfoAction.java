/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.Optional;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.STATELESS_SNAPSHOT_WAIT_FOR_ACTIVE_PRIMARY_TIMEOUT_SETTING;

/**
 * A transport action that retrieves commit information (metadata snapshot and blob locations)
 * from the node hosting the primary shard.
 * <p>
 * This action combines both the sender and receiver logic in a single class:
 * <ul>
 *   <li>The sender side (doExecute) locates the primary shard in the cluster state,
 *       waits for it to be active, sends the request, and retries on retryable failures.</li>
 *   <li>The receiver side (shardOperation) runs on the primary's node to read commit info locally.</li>
 * </ul>
 */
public class TransportGetShardSnapshotCommitInfoAction extends HandledTransportAction<
    GetShardSnapshotCommitInfoRequest,
    GetShardSnapshotCommitInfoResponse> {

    public static final String NAME = "internal:index/shard/snapshot:get_commit_info";
    public static final ActionType<GetShardSnapshotCommitInfoResponse> TYPE = new ActionType<>(NAME);
    static final String SHARD_ACTION_NAME = NAME + "[s]";
    private static final Logger logger = LogManager.getLogger(TransportGetShardSnapshotCommitInfoAction.class);

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final ThreadPool threadPool;
    private final SnapshotsCommitService snapshotsCommitService;
    private final ThrottledTaskRunner sendThrottle;

    private volatile TimeValue waitForActivePrimaryTimeout;

    @Inject
    public TransportGetShardSnapshotCommitInfoAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        SnapshotsCommitService snapshotsCommitService
    ) {
        this(
            clusterService,
            transportService,
            actionFilters,
            snapshotsCommitService,
            clusterService.threadPool().info(ThreadPool.Names.SNAPSHOT).getMax()
        );
    }

    // Package private for testing
    TransportGetShardSnapshotCommitInfoAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        SnapshotsCommitService snapshotsCommitService,
        int maxInFlightSendRequests
    ) {
        super(NAME, transportService, actionFilters, GetShardSnapshotCommitInfoRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        clusterService.getClusterSettings()
            .initializeAndWatch(STATELESS_SNAPSHOT_WAIT_FOR_ACTIVE_PRIMARY_TIMEOUT_SETTING, value -> waitForActivePrimaryTimeout = value);
        this.transportService = transportService;
        this.threadPool = clusterService.threadPool();
        this.snapshotsCommitService = snapshotsCommitService;
        this.sendThrottle = new ThrottledTaskRunner("get-commit-info", maxInFlightSendRequests, EsExecutors.DIRECT_EXECUTOR_SERVICE);

        transportService.registerRequestHandler(
            SHARD_ACTION_NAME,
            threadPool.executor(ThreadPool.Names.SNAPSHOT),
            GetShardSnapshotCommitInfoRequest::new,
            this::shardOperation
        );
    }

    @Override
    protected void doExecute(
        Task task,
        GetShardSnapshotCommitInfoRequest request,
        ActionListener<GetShardSnapshotCommitInfoResponse> listener
    ) {
        ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SNAPSHOT);
        final var state = clusterService.state();
        final var shardId = request.shardId();
        final var projectId = state.metadata().projectFor(shardId.getIndex()).id();
        // Observer with timeout so that it does not retry indefinitely when the shard cannot be assigned
        final var observer = new ClusterStateObserver(
            state,
            clusterService,
            waitForActivePrimaryTimeout,
            logger,
            threadPool.getThreadContext()
        );
        doGetCommitInfo(request, projectId, state, observer, listener);
    }

    private void doGetCommitInfo(
        GetShardSnapshotCommitInfoRequest request,
        ProjectId projectId,
        ClusterState state,
        ClusterStateObserver observer,
        ActionListener<GetShardSnapshotCommitInfoResponse> listener
    ) {
        final var retryingListener = listener.delegateResponse((l, e) -> {
            final var cause = ExceptionsHelper.unwrapCause(e);
            logger.debug(() -> "failed to get commit info for " + request.shardId(), cause);
            if (isRetryable(projectId, request.shardId(), cause)) {
                retry(request, projectId, observer, cause, l);
            } else {
                l.onFailure(e);
            }
        });
        try {
            final var shardId = request.shardId();
            if (projectIndexShardExists(state, projectId, shardId) == false) {
                throw new ShardNotFoundException(shardId, "project, index, or shard does not exist");
            }
            final var shardRoutingTable = state.routingTable(projectId).shardRoutingTable(shardId);
            if (shardRoutingTable.primaryShard() == null || shardRoutingTable.primaryShard().active() == false) {
                throw new ShardNotFoundException(shardId, "primary shard is not active");
            }
            final var node = state.nodes().get(shardRoutingTable.primaryShard().currentNodeId());
            assert node != null;
            logger.debug("{} sending get shard snapshot commit info request to [{}]", shardId, node);
            sendThrottle.enqueueTask(retryingListener.delegateFailure((l, permit) -> {
                transportService.sendRequest(
                    node,
                    SHARD_ACTION_NAME,
                    request,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<>(
                        ActionListener.runBefore(l, permit::close),
                        GetShardSnapshotCommitInfoResponse::new,
                        EsExecutors.DIRECT_EXECUTOR_SERVICE
                    )
                );
            }));
        } catch (Exception e) {
            retryingListener.onFailure(e);
        }
    }

    // package private for testing
    ThrottledTaskRunner getSendThrottle() {
        return sendThrottle;
    }

    private void retry(
        GetShardSnapshotCommitInfoRequest request,
        ProjectId projectId,
        ClusterStateObserver observer,
        Throwable cause,
        ActionListener<GetShardSnapshotCommitInfoResponse> listener
    ) {
        final var shardId = request.shardId();
        final Predicate<ClusterState> retryPredicate = state -> {
            if (projectIndexShardExists(state, projectId, shardId) == false) {
                return true;
            }
            final var primary = state.routingTable(projectId).shardRoutingTable(shardId).primaryShard();
            if (primary == null || primary.active() == false) {
                return false;
            }
            return Optional.ofNullable(SnapshotsInProgress.get(state).snapshot(request.snapshot()))
                .map(snapshotEntry -> snapshotEntry.shards().get(shardId))
                .map(shardSnapshotStatus -> shardSnapshotStatus.state() == SnapshotsInProgress.ShardState.INIT)
                .orElse(false);
        };
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                doGetCommitInfo(request, projectId, state, observer, listener);
            }

            @Override
            public void onClusterServiceClose() {
                listener.onFailure(new NodeClosedException(clusterService.localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                listener.onFailure(
                    new ElasticsearchException("timed out waiting for primary shard " + shardId + " to become active", cause)
                );
            }
        }, retryPredicate);
    }

    private void shardOperation(GetShardSnapshotCommitInfoRequest request, TransportChannel channel, Task task) {
        ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SNAPSHOT);
        final var channelListener = new ChannelActionListener<GetShardSnapshotCommitInfoResponse>(channel);
        ActionListener.run(channelListener, l -> {
            logger.debug("{} acquiring commit info remotely for snapshot [{}]", request.shardId(), request.snapshot());
            final var commitInfo = snapshotsCommitService.acquireAndMaybeRegisterCommitForSnapshot(
                request.shardId(),
                request.snapshot(),
                true,
                null
            );
            l.onResponse(new GetShardSnapshotCommitInfoResponse(commitInfo.blobLocations(), commitInfo.shardStateId()));
        });
    }

    private static boolean projectIndexShardExists(ClusterState state, ProjectId projectId, ShardId shardId) {
        if (state.metadata().hasProject(projectId) == false
            || state.metadata().getProject(projectId).hasIndex(shardId.getIndex()) == false) {
            return false;
        }
        // Also check the shard exists in the routing table to be future-proof against reshard operations
        // that may reduce the number of shards.
        final var indexRoutingTable = state.routingTable(projectId).index(shardId.getIndex());
        return indexRoutingTable != null && indexRoutingTable.shard(shardId.id()) != null;
    }

    private boolean isRetryable(ProjectId projectId, ShardId shardId, Throwable e) {
        if (TransportActions.isShardNotAvailableException(e)
            || e instanceof ConnectTransportException
            || e instanceof NodeClosedException) {
            return transportService.lifecycleState() == Lifecycle.State.STARTED
                && projectIndexShardExists(clusterService.state(), projectId, shardId);
        }
        return false;
    }
}
