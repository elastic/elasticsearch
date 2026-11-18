/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryCommitTooNewException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

public class TransportSendRecoveryCommitRegistrationAction extends HandledTransportAction<RegisterCommitRequest, RegisterCommitResponse> {

    public static final String NAME = "internal:index/shard/recovery:send_recovery_commit_registration";
    public static final ActionType<RegisterCommitResponse> TYPE = new ActionType<>(NAME);
    private static final Logger logger = LogManager.getLogger(TransportSendRecoveryCommitRegistrationAction.class);

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final IndicesService indicesService;
    private final ThreadPool threadPool;

    @Inject
    public TransportSendRecoveryCommitRegistrationAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters
    ) {
        super(NAME, transportService, actionFilters, RegisterCommitRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.threadPool = clusterService.threadPool();
    }

    @Override
    protected void doExecute(Task task, RegisterCommitRequest request, ActionListener<RegisterCommitResponse> listener) {
        var state = clusterService.state();
        // The timeout is intentionally kept low to prevent blocking other recoveries since this registration
        // happens during unpromotable recovery).
        var observer = new ClusterStateObserver(state, clusterService, new TimeValue(10000), logger, threadPool.getThreadContext());
        register(task, request, state, observer, listener);
    }

    private void register(
        Task task,
        RegisterCommitRequest request,
        ClusterState state,
        ClusterStateObserver observer,
        ActionListener<RegisterCommitResponse> listener
    ) {
        tryRegistration(task, request, state, listener.delegateResponse((l, e) -> {
            var cause = ExceptionsHelper.unwrapCause(e);
            logger.debug("recovery commit registration failed", cause);
            if (isRetryable(cause, request)) {
                observer.waitForNextChange(new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        register(task, request, state, observer, l);
                    }

                    @Override
                    public void onClusterServiceClose() {
                        l.onFailure(new ElasticsearchException("cs observer closed"));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        l.onFailure(new ElasticsearchException("cs observer timed out", cause));
                    }
                });
            } else {
                l.onFailure(e);
            }
        }));
    }

    private boolean isRetryable(Throwable e, RegisterCommitRequest request) {
        if (e instanceof ShardNotFoundException || e instanceof RecoveryCommitTooNewException) {
            return true;
        } else if (e instanceof IndexNotFoundException) {
            var state = clusterService.state();
            final ShardId shardId = request.getShardId();
            // lookupProject checks whether index is still in the project metadata
            return state.metadata().lookupProject(shardId.getIndex()).isPresent();
        } else {
            return false;
        }
    }

    private void tryRegistration(
        Task task,
        RegisterCommitRequest request,
        ClusterState state,
        ActionListener<RegisterCommitResponse> listener
    ) {
        try {
            var shardId = request.getShardId();
            assert localShardIsUnpromotableOrNull(shardId)
                : "TransportSendRecoveryCommitRegistrationAction can only be executed on a search shard";
            // Forward the request to the indexing shard
            final ProjectMetadata projectMetadata = state.metadata().projectFor(shardId.getIndex()); // can throw IndexNotFoundException
            var shardRoutingTable = state.routingTable(projectMetadata.id()).shardRoutingTable(shardId);

            // Because recovery is forked from `IndicesClusterStateService` it is possible to arrive here
            // while `IndicesClusterStateService` or a different cluster state applier is still processing
            // the state which assigned this shard to this node.
            // In this case `state` read from `ClusterService` is a state _before_ this search shard is assigned.
            // It doesn't make sense to send `TransportRegisterCommitForRecoveryAction` to the index shard
            // with this cluster state version since the action checks routing of the search shard and we know it is not present.
            //
            // TODO ideally we should use `RecoveryTarget#clusterStateVersion()` when creating `RegisterCommitRequest`
            // and that would fix this problem without retries.
            // But we need to plumb this information through somehow.
            if (isSearchShardAssigned(shardRoutingTable, request.getNodeId()) == false) {
                throw new ShardNotFoundException(
                    shardId,
                    "search shard {} is not assigned in the cluster state version",
                    shardId,
                    state.version()
                );
            }
            if (shardRoutingTable.primaryShard() == null || shardRoutingTable.primaryShard().active() == false) {
                // TODO: A search shard should be able to continue using the found commit despite not being able to register it.
                // For now, we retry the request to register the commit.
                throw new ShardNotFoundException(shardId, "cannot route request to the indexing shard");
            }
            DiscoveryNode node = state.nodes().get(shardRoutingTable.primaryShard().currentNodeId());
            logger.debug("{} sending recovery commit registration to {}", shardId, node);
            assert node != null;
            final var requestWithClusterStateVersion = request.withClusterStateVersion(state.version());
            transportService.sendChildRequest(
                node,
                TransportRegisterCommitForRecoveryAction.NAME,
                requestWithClusterStateVersion,
                task,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(listener.safeMap(r -> {
                    logger.debug("{} received registration response {} for {} ", shardId, r, requestWithClusterStateVersion);
                    return r;
                }), in -> new RegisterCommitResponse(in, node.getId()), transportService.getThreadPool().generic())
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private boolean localShardIsUnpromotableOrNull(ShardId shardId) {
        // It is OK if the index or the shard is concurrently removed when performing this check.
        // We do _not_ want to retry on this ShardNotFoundException.
        // The commit registration will proceed and ultimately fail inside PeerRecoveryTargetService
        // which in turn allows a new allocation to happen.
        IndexService indexService = indicesService.indexService(shardId.getIndex());
        if (indexService == null) {
            return true;
        }
        final IndexShard indexShard = indexService.getShardOrNull(shardId.id());
        if (indexShard == null) {
            return true;
        }
        return indexShard.routingEntry().isPromotableToPrimary() == false;
    }

    /// The same idea but slightly different structure from [TransportRegisterCommitForRecoveryAction#isSearchShardInRoutingTable].
    private boolean isSearchShardAssigned(IndexShardRoutingTable shardRoutingTable, String localNodeId) {
        for (var shardRouting : shardRoutingTable.assignedUnpromotableShards()) {
            if (shardRouting.currentNodeId().equals(localNodeId)) {
                return true;
            }
        }
        return false;
    }
}
