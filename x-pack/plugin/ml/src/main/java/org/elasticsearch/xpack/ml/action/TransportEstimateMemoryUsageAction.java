/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.EstimateMemoryUsageAction;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractorFactory;
import org.elasticsearch.xpack.ml.dataframe.process.MemoryUsageEstimationProcessManager;

import java.util.Objects;
import java.util.Optional;

/**
 * Estimates memory usage for the given data frame analytics spec.
 * Redirects to a different node if the current node is *not* an ML node.
 */
public class TransportEstimateMemoryUsageAction
    extends HandledTransportAction<PutDataFrameAnalyticsAction.Request, EstimateMemoryUsageAction.Response> {

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final NodeClient client;
    private final MemoryUsageEstimationProcessManager processManager;

    @Inject
    public TransportEstimateMemoryUsageAction(TransportService transportService,
                                              ActionFilters actionFilters,
                                              ClusterService clusterService,
                                              NodeClient client,
                                              MemoryUsageEstimationProcessManager processManager) {
        super(EstimateMemoryUsageAction.NAME, transportService, actionFilters, PutDataFrameAnalyticsAction.Request::new);
        this.transportService = transportService;
        this.clusterService = Objects.requireNonNull(clusterService);
        this.client = Objects.requireNonNull(client);
        this.processManager = Objects.requireNonNull(processManager);
    }

    @Override
    protected void doExecute(Task task,
                             PutDataFrameAnalyticsAction.Request request,
                             ActionListener<EstimateMemoryUsageAction.Response> listener) {
        DiscoveryNode localNode = clusterService.localNode();
        if (MachineLearning.isMlNode(localNode)) {
            doEstimateMemoryUsage(createTaskIdForMemoryEstimation(task), request, listener);
        } else {
            redirectToMlNode(request, listener);
        }
    }

    /**
     * Creates unique task id for the memory estimation process. This id is useful when logging.
     */
    private static String createTaskIdForMemoryEstimation(Task task) {
        return "memory_usage_estimation_" + task.getId();
    }

    /**
     * Performs memory usage estimation.
     * Memory usage estimation spawns an ML C++ process which is only available on ML nodes. That's why this method can only be called on
     * the ML node.
     */
    private void doEstimateMemoryUsage(String taskId,
                                       PutDataFrameAnalyticsAction.Request request,
                                       ActionListener<EstimateMemoryUsageAction.Response> listener) {
        DataFrameDataExtractorFactory.createForSourceIndices(
            client,
            taskId,
            request.getConfig(),
            ActionListener.wrap(
                dataExtractorFactory -> {
                    processManager.runJobAsync(
                        taskId,
                        request.getConfig(),
                        dataExtractorFactory,
                        ActionListener.wrap(
                            result -> listener.onResponse(
                                new EstimateMemoryUsageAction.Response(
                                    result.getExpectedMemoryWithoutDisk(), result.getExpectedMemoryWithDisk())),
                            listener::onFailure
                        )
                    );
                },
                listener::onFailure
            )
        );
    }

    /**
     * Finds the first available ML node in the cluster and redirects the request to this node.
     */
    private void redirectToMlNode(PutDataFrameAnalyticsAction.Request request,
                                  ActionListener<EstimateMemoryUsageAction.Response> listener) {
        Optional<DiscoveryNode> node = findMlNode(clusterService.state());
        if (node.isPresent()) {
            transportService.sendRequest(
                node.get(), actionName, request, new ActionListenerResponseHandler<>(listener, EstimateMemoryUsageAction.Response::new));
        } else {
            listener.onFailure(ExceptionsHelper.badRequestException("No ML node to run on"));
        }
    }

    /**
     * Finds the first available ML node in the cluster state.
     */
    private static Optional<DiscoveryNode> findMlNode(ClusterState clusterState) {
        for (DiscoveryNode node : clusterState.getNodes()) {
            if (MachineLearning.isMlNode(node)) {
                return Optional.of(node);
            }
        }
        return Optional.empty();
    }
}
