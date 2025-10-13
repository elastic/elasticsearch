/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.replication;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class ReplicationSplitHelper<
    Request extends ReplicationRequest<Request>,
    ReplicaRequest extends ReplicationRequest<ReplicaRequest>,
    Response extends ReplicationResponse> {

    private final Logger logger;
    private final ClusterService clusterService;
    private final TimeValue retryTimeout;
    private final TimeValue initialRetryBackoffBound;
    private final TriConsumer<
        DiscoveryNode,
        TransportReplicationAction.ConcreteShardRequest<Request>,
        ActionListener<Response>> primaryRequestSender;

    public ReplicationSplitHelper(
        Logger logger,
        ClusterService clusterService,
        TimeValue initialRetryBackoffBound,
        TimeValue retryTimeout,
        TriConsumer<DiscoveryNode, TransportReplicationAction.ConcreteShardRequest<Request>, ActionListener<Response>> primaryRequestSender
    ) {
        this.clusterService = clusterService;
        this.logger = logger;
        this.retryTimeout = retryTimeout;
        this.initialRetryBackoffBound = initialRetryBackoffBound;
        this.primaryRequestSender = primaryRequestSender;
    }

    public static <Request extends ReplicationRequest<Request>> boolean needsSplitCoordination(
        final TransportReplicationAction.ConcreteShardRequest<Request> primaryRequest,
        final IndexMetadata indexMetadata
    ) {
        SplitShardCountSummary requestSplitSummary = primaryRequest.getRequest().reshardSplitShardCountSummary();
        // TODO: We currently only set the request split summary transport shard bulk. Only evaluate this at the moment or else every
        // request would say it needs a split.
        return requestSplitSummary.isUnset() == false
            && requestSplitSummary.equals(
                SplitShardCountSummary.forIndexing(indexMetadata, primaryRequest.getRequest().shardId().getId())
            ) == false;
    }

    public SplitCoordinator newSplitRequest(
        TransportReplicationAction<Request, ReplicaRequest, Response> action,
        ReplicationTask task,
        ProjectMetadata project,
        TransportReplicationAction<Request, ReplicaRequest, Response>.PrimaryShardReference primaryShardReference,
        TransportReplicationAction.ConcreteShardRequest<Request> primaryRequest,
        CheckedBiConsumer<
            TransportReplicationAction<Request, ReplicaRequest, Response>.PrimaryShardReference,
            ActionListener<Response>,
            Exception> executePrimaryRequest,
        ActionListener<Response> onCompletionListener
    ) {
        return new SplitCoordinator(
            action,
            task,
            project,
            primaryShardReference,
            primaryRequest,
            executePrimaryRequest,
            onCompletionListener
        );
    }

    public class SplitCoordinator {

        private final TransportReplicationAction<Request, ReplicaRequest, Response> action;
        private final ReplicationTask task;
        private final ProjectMetadata project;
        private final TransportReplicationAction<Request, ReplicaRequest, Response>.PrimaryShardReference primaryShardReference;
        private final TransportReplicationAction.ConcreteShardRequest<Request> originalRequest;
        private final CheckedBiConsumer<
            TransportReplicationAction<Request, ReplicaRequest, Response>.PrimaryShardReference,
            ActionListener<Response>,
            Exception> doPrimaryRequest;
        private final ActionListener<Response> onCompletionListener;

        public SplitCoordinator(
            TransportReplicationAction<Request, ReplicaRequest, Response> action,
            ReplicationTask task,
            ProjectMetadata project,
            TransportReplicationAction<Request, ReplicaRequest, Response>.PrimaryShardReference primaryShardReference,
            TransportReplicationAction.ConcreteShardRequest<Request> originalRequest,
            CheckedBiConsumer<
                TransportReplicationAction<Request, ReplicaRequest, Response>.PrimaryShardReference,
                ActionListener<Response>,
                Exception> doPrimaryRequest,
            ActionListener<Response> onCompletionListener
        ) {
            this.action = action;
            this.task = task;
            this.project = project;
            this.primaryShardReference = primaryShardReference;
            this.originalRequest = originalRequest;
            this.doPrimaryRequest = doPrimaryRequest;
            this.onCompletionListener = onCompletionListener;
        }

        public void coordinate() throws Exception {
            Map<ShardId, Request> splitRequests = action.splitRequestOnPrimary(originalRequest.getRequest());

            int numSplitRequests = splitRequests.size();

            // splitRequestOnPrimary must handle the case when the request has no items
            assert numSplitRequests > 0 : "expected at-least 1 split request";
            assert numSplitRequests <= 2 : "number of split requests too many";

            if (numSplitRequests == 1) {
                // If the request is for source, same behavior as before
                if (splitRequests.containsKey(originalRequest.getRequest().shardId())) {
                    doPrimaryRequest.accept(primaryShardReference, onCompletionListener);
                } else {
                    // If the request is for target, forward request to target.
                    // TODO: Note that the request still contains the original shardId. We need to test if this will be a
                    // problem.
                    primaryShardReference.close(); // release shard operation lock as soon as possible
                    TransportReplicationAction.setPhase(task, "primary_reshard_target_delegation");

                    Map.Entry<ShardId, Request> next = splitRequests.entrySet().iterator().next();
                    Request request = next.getValue();
                    ShardId targetShardId = next.getKey();
                    delegateToTarget(targetShardId, request, clusterService::state, project, new ActionListener<>() {

                        @Override
                        public void onResponse(Response response) {
                            TransportReplicationAction.setPhase(task, "finished");
                            onCompletionListener.onResponse(response);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            TransportReplicationAction.setPhase(task, "finished");
                            onCompletionListener.onFailure(e);
                        }
                    });
                }
            } else {
                coordinateMultipleRequests(splitRequests);
            }

        }

        private void coordinateMultipleRequests(Map<ShardId, Request> splitRequests) throws Exception {
            TransportReplicationAction.setPhase(task, "primary_with_reshard_target_delegation");
            Map<ShardId, Tuple<Response, Exception>> results = new ConcurrentHashMap<>(splitRequests.size());
            CountDown countDown = new CountDown(splitRequests.size());
            for (Map.Entry<ShardId, Request> splitRequest : splitRequests.entrySet()) {
                ActionListener<Response> listener = new ActionListener<>() {
                    @Override
                    public void onResponse(Response response) {
                        results.put(splitRequest.getKey(), new Tuple<>(response, null));
                        if (countDown.countDown()) {
                            finish();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        results.put(splitRequest.getKey(), new Tuple<>(null, e));
                        if (countDown.countDown()) {
                            finish();
                        }
                    }

                    private void finish() {
                        Tuple<Response, Exception> finalResponse = action.combineSplitResponses(
                            originalRequest.getRequest(),
                            splitRequests,
                            results
                        );
                        TransportReplicationAction.setPhase(task, "finished");
                        if (finalResponse.v1() != null) {
                            onCompletionListener.onResponse(finalResponse.v1());
                        } else {
                            onCompletionListener.onFailure(finalResponse.v2());
                        }
                    }
                };
                if (splitRequest.getKey().equals(originalRequest.getRequest().shardId())) {
                    doPrimaryRequest.accept(primaryShardReference, listener);
                } else {
                    delegateToTarget(splitRequest.getKey(), splitRequest.getValue(), clusterService::state, project, listener);
                }
            }
        }

        private void delegateToTarget(
            ShardId targetShardId,
            Request splitRequest,
            Supplier<ClusterState> clusterStateSupplier,
            final ProjectMetadata project,
            ActionListener<Response> finalLister
        ) {
            new RetryableAction<>(
                logger,
                clusterService.threadPool(),
                retryTimeout,
                initialRetryBackoffBound,
                finalLister,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            ) {

                @Override
                public void tryAction(ActionListener<Response> listener) {
                    final ClusterState clusterState = clusterStateSupplier.get();
                    final ProjectId projectId = project.id();
                    final ShardRouting target = clusterState.routingTable(projectId).shardRoutingTable(targetShardId).primaryShard();
                    final IndexMetadata indexMetadata = project.index(targetShardId.getIndex());
                    final DiscoveryNode targetNode = clusterState.nodes().get(target.currentNodeId());
                    final String allocationID = target.allocationId().getId();
                    final long expectedPrimaryTerm = indexMetadata.primaryTerm(targetShardId.id());

                    TransportReplicationAction.ConcreteShardRequest<Request> concreteShardRequest =
                        new TransportReplicationAction.ConcreteShardRequest<>(splitRequest, allocationID, expectedPrimaryTerm);
                    primaryRequestSender.apply(targetNode, concreteShardRequest, listener);
                }

                @Override
                public boolean shouldRetry(Exception e) {
                    // TODO: Consider if we should just route a coordinating version of the request which will automatically implement
                    // retries.
                    return TransportReplicationAction.retryPrimaryException(ExceptionsHelper.unwrapCause(e));
                }
            }.run();
        }
    }
}
