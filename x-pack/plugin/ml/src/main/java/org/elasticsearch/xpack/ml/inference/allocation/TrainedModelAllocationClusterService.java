/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelAllocationStateAction;
import org.elasticsearch.xpack.core.ml.inference.allocation.AllocationState;
import org.elasticsearch.xpack.core.ml.inference.allocation.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.allocation.TrainedModelAllocation;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.NodeLoad;
import org.elasticsearch.xpack.ml.job.NodeLoadDetector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class TrainedModelAllocationClusterService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(TrainedModelAllocationClusterService.class);

    private final ClusterService clusterService;
    private final NodeLoadDetector nodeLoadDetector;
    private volatile int maxMemoryPercentage;
    private volatile boolean useAuto;

    public TrainedModelAllocationClusterService(Settings settings, ClusterService clusterService, NodeLoadDetector nodeLoadDetector) {
        this.clusterService = clusterService;
        this.nodeLoadDetector = nodeLoadDetector;
        this.maxMemoryPercentage = MachineLearning.MAX_MACHINE_MEMORY_PERCENT.get(settings);
        this.useAuto = MachineLearning.USE_AUTO_MACHINE_MEMORY_PERCENT.get(settings);
        // Only nodes that can possibly be master nodes really need this service running
        if (DiscoveryNode.isMasterNode(settings)) {
            clusterService.addListener(this);
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(MachineLearning.MAX_MACHINE_MEMORY_PERCENT, this::setMaxMemoryPercentage);
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(MachineLearning.USE_AUTO_MACHINE_MEMORY_PERCENT, this::setUseAuto);
        }
    }

    private void setMaxMemoryPercentage(int maxMemoryPercentage) {
        this.maxMemoryPercentage = maxMemoryPercentage;
    }

    private void setUseAuto(boolean useAuto) {
        this.useAuto = useAuto;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }
        if (event.localNodeMaster() && shouldAllocateModels(event)) {
            clusterService.submitStateUpdateTask("allocating models to nodes", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    // TODO this has a weird side-effect for allocating to nodes
                    // If the event indicates there were nodes added/removed, this method only looks at the current state and has
                    // no previous knowledge of existing nodes. Consequently, if a model was manually removed (task-kill) from a node
                    // it may get re-allocated to that node when another node is added/removed...
                    return addRemoveAllocationNodes(currentState);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn("failed to allocate models", e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    logger.trace(
                        () -> new ParameterizedMessage(
                            "updated model allocations based on node changes in the cluster; new metadata [{}]",
                            Strings.toString(TrainedModelAllocationMetadata.fromState(newState), false, true)
                        )
                    );
                }
            });
        }
    }

    public void updateModelRoutingTable(
        UpdateTrainedModelAllocationStateAction.Request request,
        ActionListener<AcknowledgedResponse> listener
    ) {
        clusterService.submitStateUpdateTask("updating model routing for node allocation", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return updateModelRoutingTable(currentState, request);
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(AcknowledgedResponse.TRUE);
            }
        });
    }

    public void createNewModelAllocation(
        StartTrainedModelDeploymentAction.TaskParams params,
        ActionListener<TrainedModelAllocation> listener
    ) {
        clusterService.submitStateUpdateTask("create model allocation", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return createModelAllocation(currentState, params);
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(TrainedModelAllocationMetadata.fromState(newState).getModelAllocation(params.getModelId()));
            }
        });
    }

    public void setModelAllocationToStopping(String modelId, ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask("set model allocation stopping", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return setToStopping(currentState, modelId);
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(AcknowledgedResponse.TRUE);
            }
        });
    }

    public void removeModelAllocation(String modelId, ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask("delete model allocation", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return removeAllocation(currentState, modelId);
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(AcknowledgedResponse.TRUE);
            }
        });
    }

    private static ClusterState update(ClusterState currentState, TrainedModelAllocationMetadata.Builder modelAllocations) {
        if (modelAllocations.isChanged()) {
            return ClusterState.builder(currentState)
                .metadata(
                    Metadata.builder(currentState.metadata()).putCustom(TrainedModelAllocationMetadata.NAME, modelAllocations.build())
                )
                .build();
        } else {
            return currentState;
        }
    }

    ClusterState createModelAllocation(ClusterState currentState, StartTrainedModelDeploymentAction.TaskParams params) {
        TrainedModelAllocationMetadata.Builder builder = TrainedModelAllocationMetadata.builder(currentState);
        if (builder.hasModel(params.getModelId())) {
            throw new ResourceAlreadyExistsException("allocation for model with id [" + params.getModelId() + "] already exist");
        }

        Set<String> shuttingDownNodes = nodesShuttingDown(currentState);
        builder.addNewAllocation(params);
        for (DiscoveryNode node : currentState.getNodes().getAllNodes()) {
            if (StartTrainedModelDeploymentAction.TaskParams.mayAllocateToNode(node)
                && shuttingDownNodes.contains(node.getId()) == false) {
                Optional<String> maybeError = nodeHasCapacity(currentState, params, node);
                if (maybeError.isPresent()) {
                    builder.addFailedNode(params.getModelId(), node.getId(), maybeError.get());
                } else {
                    builder.addNode(params.getModelId(), node.getId());
                }
            }
        }
        return update(currentState, builder);
    }

    static ClusterState setToStopping(ClusterState clusterState,  String modelId) {
        TrainedModelAllocationMetadata metadata = TrainedModelAllocationMetadata.fromState(clusterState);
        final TrainedModelAllocation existingAllocation = metadata.getModelAllocation(modelId);
        if (existingAllocation == null) {
            throw new ResourceNotFoundException("allocation for model with id [{}] not found", modelId);
        }
        // If we are stopping, don't update anything
        if (existingAllocation.getAllocationState().equals(AllocationState.STOPPING)) {
            return clusterState;
        }

        TrainedModelAllocationMetadata.Builder builder = TrainedModelAllocationMetadata.builder(clusterState);
        builder.setAllocationToStopping(modelId);
        return update(clusterState, builder);
    }

    static ClusterState updateModelRoutingTable(ClusterState currentState, UpdateTrainedModelAllocationStateAction.Request request) {
        final String modelId = request.getModelId();
        final String nodeId = request.getNodeId();
        TrainedModelAllocationMetadata metadata = TrainedModelAllocationMetadata.fromState(currentState);
        logger.trace(
            () -> new ParameterizedMessage("[{}] [{}] current metadata before update {}", modelId, nodeId, Strings.toString(metadata))
        );
        final TrainedModelAllocation existingAllocation = metadata.getModelAllocation(modelId);

        // If state is stopped, this indicates the node process is closed, remove the node from the allocation
        if (request.getRoutingState().getState().equals(RoutingState.STOPPED)) {
            if (existingAllocation == null || existingAllocation.isRoutedToNode(nodeId) == false) {
                return currentState;
            }
            return update(currentState, TrainedModelAllocationMetadata.builder(currentState).removeNode(modelId, nodeId));
        }

        if (existingAllocation == null) {
            throw new ResourceNotFoundException("allocation for model with id [{}] not found", modelId);
        }
        // If we are stopping, don't update anything
        if (existingAllocation.getAllocationState().equals(AllocationState.STOPPING)) {
            logger.debug(() -> new ParameterizedMessage(
                "[{}] requested update from node [{}] to update route state to [{}]",
                modelId,
                nodeId,
                request.getRoutingState()
            ));
            return currentState;
        }
        if (existingAllocation.isRoutedToNode(nodeId) == false) {
            throw new ResourceNotFoundException("allocation for model with id [{}]] is not routed to node [{}]", modelId, nodeId);
        }
        TrainedModelAllocationMetadata.Builder builder = TrainedModelAllocationMetadata.builder(currentState);
        builder.updateAllocation(modelId, nodeId, request.getRoutingState());
        return update(currentState, builder);
    }

    static ClusterState removeAllocation(ClusterState currentState, String modelId) {
        TrainedModelAllocationMetadata.Builder builder = TrainedModelAllocationMetadata.builder(currentState);
        if (builder.hasModel(modelId) == false) {
            throw new ResourceNotFoundException("allocation for model with id [{}] not found", modelId);
        }
        return update(currentState, builder.removeAllocation(modelId));
    }

    ClusterState addRemoveAllocationNodes(ClusterState currentState) {
        TrainedModelAllocationMetadata previousState = TrainedModelAllocationMetadata.fromState(currentState);
        TrainedModelAllocationMetadata.Builder builder = TrainedModelAllocationMetadata.builder(currentState);
        Map<String, List<String>> removedNodeModelLookUp = new HashMap<>();
        Set<String> shuttingDownNodes = nodesShuttingDown(currentState);
        // TODO: make more efficient, right now this is O(nm) where n = sizeof(models) and m = sizeof(nodes)
        // It could probably be O(max(n, m))
        // Add nodes and keep track of currently routed nodes
        // Should we indicate a partial allocation somehow if some nodes don't have space?
        for (Map.Entry<String, TrainedModelAllocation> modelAllocationEntry : previousState.modelAllocations().entrySet()) {
            // Don't bother adding/removing nodes if this allocation is stopping
            if (modelAllocationEntry.getValue().getAllocationState().equals(AllocationState.STOPPING)) {
                continue;
            }
            for (DiscoveryNode node : currentState.getNodes()) {
                // Only add the route if the node is NOT shutting down, this would be a weird case of the node
                // just being added to the cluster and immediately shutting down...
                if (shuttingDownNodes.contains(node.getId()) == false
                    && StartTrainedModelDeploymentAction.TaskParams.mayAllocateToNode(node)
                    && modelAllocationEntry.getValue().isRoutedToNode(node.getId()) == false) {
                    nodeHasCapacity(currentState, modelAllocationEntry.getValue().getTaskParams(), node).ifPresentOrElse(
                        (error) -> builder.addFailedNode(modelAllocationEntry.getKey(), node.getId(), error),
                        () -> builder.addNode(modelAllocationEntry.getKey(), node.getId())
                    );
                }
            }
            for (String nodeId : modelAllocationEntry.getValue().getNodeRoutingTable().keySet()) {
                removedNodeModelLookUp.computeIfAbsent(nodeId, k -> new ArrayList<>()).add(modelAllocationEntry.getKey());
            }
        }

        // Remove nodes
        currentState.getNodes()
            .forEach(
                d -> {
                    // If a node is referenced in the current state, we shouldn't remove the node
                    // But, if that node that is referenced is shutting down, we should remove the node
                    if (shuttingDownNodes.contains(d.getId()) == false) {
                        removedNodeModelLookUp.remove(d.getId());
                    }
                }
            );
        for (Map.Entry<String, List<String>> nodeToModels : removedNodeModelLookUp.entrySet()) {
            final String nodeId = nodeToModels.getKey();
            for (String modelId : nodeToModels.getValue()) {
                builder.removeNode(modelId, nodeId);
            }
        }
        return update(currentState, builder);
    }

    static boolean shouldAllocateModels(final ClusterChangedEvent event) {
        // If there are no allocations created at all, there is nothing to update
        final TrainedModelAllocationMetadata newMetadata = event.state().getMetadata().custom(TrainedModelAllocationMetadata.NAME);
        if (newMetadata == null) {
            return false;
        }
        if (event.nodesChanged()) {
            Set<String> shuttingDownNodes = nodesShuttingDown(event.state());
            DiscoveryNodes.Delta nodesDelta = event.nodesDelta();
            for (TrainedModelAllocation trainedModelAllocation : newMetadata.modelAllocations().values()) {
                if (trainedModelAllocation.getAllocationState().equals(AllocationState.STOPPING)) {
                    continue;
                }
                for (DiscoveryNode removed : nodesDelta.removedNodes()) {
                    if (trainedModelAllocation.isRoutedToNode(removed.getId())) {
                        return true;
                    }
                }
                for (DiscoveryNode added : nodesDelta.addedNodes()) {
                    if (StartTrainedModelDeploymentAction.TaskParams.mayAllocateToNode(added)
                        && shuttingDownNodes.contains(added.getId()) == false) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    Optional<String> nodeHasCapacity(ClusterState state, StartTrainedModelDeploymentAction.TaskParams params, DiscoveryNode node) {
        NodeLoad load = nodeLoadDetector.detectNodeLoad(state, true, node, Integer.MAX_VALUE, maxMemoryPercentage, useAuto);
        if (Strings.isNullOrEmpty(load.getError()) == false) {
            logger.warn("[{}] failed to calculate current node load with error [{}]", params.getModelId(), node.getId());
            return Optional.of(load.getError());
        }
        if (load.getFreeMemory() < params.estimateMemoryUsageBytes()) {
            return Optional.of(
                ParameterizedMessage.format(
                    "This node has insufficient available memory. Available memory for ML [{} ({})], "
                        + "memory required by existing jobs and models [{} ({})], "
                        + "estimated memory required for this model [{} ({})].",
                    new Object[] {
                        load.getMaxMlMemory(),
                        ByteSizeValue.ofBytes(load.getMaxMlMemory()).toString(),
                        load.getAssignedJobMemory(),
                        ByteSizeValue.ofBytes(load.getAssignedJobMemory()).toString(),
                        params.estimateMemoryUsageBytes(),
                        ByteSizeValue.ofBytes(params.estimateMemoryUsageBytes()).toString() }
                )
            );
        }
        return Optional.empty();
    }

    /**
     * Returns true if the given node is marked as shutting down with any
     * shutdown type.
     */
    static Set<String> nodesShuttingDown(final ClusterState state) {
        return NodesShutdownMetadata.getShutdowns(state)
            .map(NodesShutdownMetadata::getAllNodeMetadataMap)
            .map(Map::keySet)
            .orElse(Collections.emptySet());
    }

}
