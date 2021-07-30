/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.MasterNodeChangePredicate;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAllocationAction;
import org.elasticsearch.xpack.core.ml.action.DeleteTrainedModelAllocationAction;
import org.elasticsearch.xpack.core.ml.action.StopTrainedModelAllocationAction;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelAllocationStateAction;
import org.elasticsearch.xpack.core.ml.inference.allocation.TrainedModelAllocation;

import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class TrainedModelAllocationService {

    private static final Logger logger = LogManager.getLogger(TrainedModelAllocationService.class);

    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    public TrainedModelAllocationService(Client client, ClusterService clusterService, ThreadPool threadPool) {
        this.client = new OriginSettingClient(client, ML_ORIGIN);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.threadPool = Objects.requireNonNull(threadPool);
    }

    public void updateModelAllocationState(
        UpdateTrainedModelAllocationStateAction.Request request,
        ActionListener<AcknowledgedResponse> listener
    ) {
        ClusterState currentState = clusterService.state();
        ClusterStateObserver observer = new ClusterStateObserver(currentState, clusterService, null, logger, threadPool.getThreadContext());
        Predicate<ClusterState> changePredicate = MasterNodeChangePredicate.build(currentState);
        DiscoveryNode masterNode = currentState.nodes().getMasterNode();
        if (masterNode == null) {
            logger.warn(
                "[{}] no master known for allocation state update [{}]",
                request.getModelId(),
                request.getRoutingState().getState()
            );
            waitForNewMasterAndRetry(observer, UpdateTrainedModelAllocationStateAction.INSTANCE, request, listener, changePredicate);
            return;
        }
        client.execute(UpdateTrainedModelAllocationStateAction.INSTANCE, request, ActionListener.wrap(listener::onResponse, failure -> {
            if (isMasterChannelException(failure)) {
                logger.info(
                    "[{}] master channel exception will retry on new master node for allocation state update [{}]",
                    request.getModelId(),
                    request.getRoutingState().getState()
                );
                waitForNewMasterAndRetry(observer, UpdateTrainedModelAllocationStateAction.INSTANCE, request, listener, changePredicate);
                return;
            }
            listener.onFailure(failure);
        }));
    }

    public void createNewModelAllocation(
        StartTrainedModelDeploymentAction.TaskParams taskParams,
        ActionListener<CreateTrainedModelAllocationAction.Response> listener
    ) {
        client.execute(CreateTrainedModelAllocationAction.INSTANCE, new CreateTrainedModelAllocationAction.Request(taskParams), listener);
    }

    public void stopModelAllocation(String modelId, ActionListener<AcknowledgedResponse> listener) {
        client.execute(StopTrainedModelAllocationAction.INSTANCE, new StopTrainedModelAllocationAction.Request(modelId), listener);
    }

    public void deleteModelAllocation(String modelId, ActionListener<AcknowledgedResponse> listener) {
        client.execute(DeleteTrainedModelAllocationAction.INSTANCE, new DeleteTrainedModelAllocationAction.Request(modelId), listener);
    }

    public void waitForAllocationCondition(
        final String modelId,
        final Predicate<TrainedModelAllocation> predicate,
        final @Nullable TimeValue timeout,
        final WaitForAllocationListener listener
    ) {
        final Predicate<ClusterState> clusterStatePredicate = clusterState -> predicate.test(
            TrainedModelAllocationMetadata.allocationForModelId(clusterState, modelId).orElse(null)
        );

        final ClusterStateObserver observer = new ClusterStateObserver(clusterService, timeout, logger, threadPool.getThreadContext());
        final ClusterState clusterState = observer.setAndGetObservedState();
        if (clusterStatePredicate.test(clusterState)) {
            listener.onResponse(TrainedModelAllocationMetadata.allocationForModelId(clusterState, modelId).orElse(null));
        } else {
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    listener.onResponse(TrainedModelAllocationMetadata.allocationForModelId(clusterState, modelId).orElse(null));
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onTimeout(timeout);
                }
            }, clusterStatePredicate);
        }
    }

    public interface WaitForAllocationListener extends ActionListener<TrainedModelAllocation> {
        default void onTimeout(TimeValue timeout) {
            onFailure(new IllegalStateException("Timed out when waiting for trained model allocation after " + timeout));
        }
    }

    protected void waitForNewMasterAndRetry(
        ClusterStateObserver observer,
        ActionType<AcknowledgedResponse> action,
        ActionRequest request,
        ActionListener<AcknowledgedResponse> listener,
        Predicate<ClusterState> changePredicate
    ) {
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                client.execute(action, request, listener);
            }

            @Override
            public void onClusterServiceClose() {
                logger.warn("node closed while execution action [{}] for request [{}]", action.name(), request);
                listener.onFailure(new NodeClosedException(clusterService.localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                // we wait indefinitely for a new master
                assert false;
            }
        }, changePredicate);
    }

    private static final Class<?>[] MASTER_CHANNEL_EXCEPTIONS = new Class<?>[] {
        NotMasterException.class,
        ConnectTransportException.class,
        FailedToCommitClusterStateException.class };

    private static boolean isMasterChannelException(Exception exp) {
        return org.elasticsearch.ExceptionsHelper.unwrap(exp, MASTER_CHANNEL_EXCEPTIONS) != null;
    }

}
