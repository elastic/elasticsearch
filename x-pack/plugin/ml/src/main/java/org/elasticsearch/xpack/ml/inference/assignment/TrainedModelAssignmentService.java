/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.DeleteTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelAssignmentRoutingInfoAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;

import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class TrainedModelAssignmentService {

    private static final Logger logger = LogManager.getLogger(TrainedModelAssignmentService.class);

    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    public TrainedModelAssignmentService(Client client, ClusterService clusterService, ThreadPool threadPool) {
        this.client = new OriginSettingClient(client, ML_ORIGIN);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.threadPool = Objects.requireNonNull(threadPool);
    }

    public void updateModelAssignmentState(
        UpdateTrainedModelAssignmentRoutingInfoAction.Request request,
        ActionListener<AcknowledgedResponse> listener
    ) {
        ClusterState currentState = clusterService.state();
        ClusterStateObserver observer = new ClusterStateObserver(currentState, clusterService, null, logger, threadPool.getThreadContext());
        Predicate<ClusterState> changePredicate = MasterNodeChangePredicate.build(currentState);
        DiscoveryNode masterNode = currentState.nodes().getMasterNode();
        if (masterNode == null) {
            logger.warn("[{}] no master known for assignment update [{}]", request.getModelId(), request.getUpdate());
            waitForNewMasterAndRetry(observer, UpdateTrainedModelAssignmentRoutingInfoAction.INSTANCE, request, listener, changePredicate);
            return;
        }
        client.execute(
            UpdateTrainedModelAssignmentRoutingInfoAction.INSTANCE,
            request,
            ActionListener.wrap(listener::onResponse, failure -> {
                if (isMasterChannelException(failure)) {
                    logger.info(
                        "[{}] master channel exception will retry on new master node for assignment update [{}]",
                        request.getModelId(),
                        request.getUpdate()
                    );
                    waitForNewMasterAndRetry(
                        observer,
                        UpdateTrainedModelAssignmentRoutingInfoAction.INSTANCE,
                        request,
                        listener,
                        changePredicate
                    );
                    return;
                }
                listener.onFailure(failure);
            })
        );
    }

    public void createNewModelAssignment(
        StartTrainedModelDeploymentAction.TaskParams taskParams,
        ActionListener<CreateTrainedModelAssignmentAction.Response> listener
    ) {
        client.execute(CreateTrainedModelAssignmentAction.INSTANCE, new CreateTrainedModelAssignmentAction.Request(taskParams), listener);
    }

    public void deleteModelAssignment(String modelId, ActionListener<AcknowledgedResponse> listener) {
        client.execute(DeleteTrainedModelAssignmentAction.INSTANCE, new DeleteTrainedModelAssignmentAction.Request(modelId), listener);
    }

    public void waitForAssignmentCondition(
        final String modelId,
        final Predicate<ClusterState> predicate,
        final @Nullable TimeValue timeout,
        final WaitForAssignmentListener listener
    ) {

        final ClusterStateObserver observer = new ClusterStateObserver(clusterService, timeout, logger, threadPool.getThreadContext());
        final ClusterState clusterState = observer.setAndGetObservedState();
        if (predicate.test(clusterState)) {
            listener.onResponse(TrainedModelAssignmentMetadata.assignmentForModelId(clusterState, modelId).orElse(null));
        } else {
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    listener.onResponse(TrainedModelAssignmentMetadata.assignmentForModelId(state, modelId).orElse(null));
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onTimeout(timeout);
                }
            }, predicate);
        }
    }

    public interface WaitForAssignmentListener extends ActionListener<TrainedModelAssignment> {
        default void onTimeout(TimeValue timeout) {
            onFailure(new ElasticsearchStatusException("Starting deployment timed out after [{}]", RestStatus.REQUEST_TIMEOUT, timeout));
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
