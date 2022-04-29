/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchStatusException;
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
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelAssignmentStateAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentState;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.NodeLoad;
import org.elasticsearch.xpack.ml.job.NodeLoadDetector;

import java.util.Collections;
import java.util.Comparator;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TrainedModelAssignmentClusterService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(TrainedModelAssignmentClusterService.class);

    private final ClusterService clusterService;
    private final NodeLoadDetector nodeLoadDetector;
    private volatile int maxMemoryPercentage;
    private volatile boolean useAuto;
    private volatile int maxOpenJobs;

    public TrainedModelAssignmentClusterService(Settings settings, ClusterService clusterService, NodeLoadDetector nodeLoadDetector) {
        this.clusterService = clusterService;
        this.nodeLoadDetector = nodeLoadDetector;
        this.maxMemoryPercentage = MachineLearning.MAX_MACHINE_MEMORY_PERCENT.get(settings);
        this.useAuto = MachineLearning.USE_AUTO_MACHINE_MEMORY_PERCENT.get(settings);
        this.maxOpenJobs = MachineLearning.MAX_OPEN_JOBS_PER_NODE.get(settings);
        // Only nodes that can possibly be master nodes really need this service running
        if (DiscoveryNode.isMasterNode(settings)) {
            clusterService.addListener(this);
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(MachineLearning.MAX_MACHINE_MEMORY_PERCENT, this::setMaxMemoryPercentage);
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(MachineLearning.USE_AUTO_MACHINE_MEMORY_PERCENT, this::setUseAuto);
            clusterService.getClusterSettings().addSettingsUpdateConsumer(MachineLearning.MAX_OPEN_JOBS_PER_NODE, this::setMaxOpenJobs);
        }
    }

    private void setMaxMemoryPercentage(int maxMemoryPercentage) {
        this.maxMemoryPercentage = maxMemoryPercentage;
    }

    private void setUseAuto(boolean useAuto) {
        this.useAuto = useAuto;
    }

    private void setMaxOpenJobs(int maxOpenJobs) {
        this.maxOpenJobs = maxOpenJobs;
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }
        if (event.localNodeMaster() && shouldAllocateModels(event)) {
            submitUnbatchedTask("allocating models to nodes", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    // TODO this has a weird side-effect for allocating to nodes
                    // If the event indicates there were nodes added/removed, this method only looks at the current state and has
                    // no previous knowledge of existing nodes. Consequently, if a model was manually removed (task-kill) from a node
                    // it may get re-allocated to that node when another node is added/removed...

                    // As this produces a cluster state update task, we are certain that if the persistent
                    // task framework results in assigning some ML tasks on that same cluster state change
                    // we do not end up over-allocating a node. Both this service and the persistant task service
                    // will produce a cluster state update but the one that gets applied first wins. The other
                    // update will be rejected and we will retry to assign getting a correct update on available memory
                    // on each node.
                    // Also, note that as this service is a returned as a component of the ML plugin,
                    // and components are created before persistent task executors, we will try to allocate
                    // trained models before we try to assign ML persistent tasks.
                    return addRemoveAssignmentNodes(currentState);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn("failed to allocate models", e);
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    logger.trace(
                        () -> new ParameterizedMessage(
                            "updated model assignments based on node changes in the cluster; new metadata [{}]",
                            Strings.toString(TrainedModelAssignmentMetadata.fromState(newState), false, true)
                        )
                    );
                }
            });
        }
    }

    public void updateModelRoutingTable(
        UpdateTrainedModelAssignmentStateAction.Request request,
        ActionListener<AcknowledgedResponse> listener
    ) {
        submitUnbatchedTask("updating model routing for node assignment", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return updateModelRoutingTable(currentState, request);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                listener.onResponse(AcknowledgedResponse.TRUE);
            }
        });
    }

    public void createNewModelAssignment(
        StartTrainedModelDeploymentAction.TaskParams params,
        ActionListener<TrainedModelAssignment> listener
    ) {
        submitUnbatchedTask("create model assignment", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return createModelAssignment(currentState, params);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                listener.onResponse(TrainedModelAssignmentMetadata.fromState(newState).getModelAssignment(params.getModelId()));
            }
        });
    }

    public void setModelAssignmentToStopping(String modelId, ActionListener<AcknowledgedResponse> listener) {
        submitUnbatchedTask("set model assignment stopping", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return setToStopping(currentState, modelId, "client API call");
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                listener.onResponse(AcknowledgedResponse.TRUE);
            }
        });
    }

    public void removeModelAssignment(String modelId, ActionListener<AcknowledgedResponse> listener) {
        submitUnbatchedTask("delete model assignment", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return removeAssignment(currentState, modelId);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                listener.onResponse(AcknowledgedResponse.TRUE);
            }
        });
    }

    // Used by the reset action directly
    public void removeAllModelAssignments(ActionListener<AcknowledgedResponse> listener) {
        submitUnbatchedTask("delete all model assignments", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return removeAllAssignments(currentState);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                listener.onResponse(AcknowledgedResponse.TRUE);
            }
        });
    }

    private static ClusterState update(ClusterState currentState, TrainedModelAssignmentMetadata.Builder modelAssignments) {
        if (modelAssignments.isChanged()) {
            return ClusterState.builder(currentState)
                .metadata(
                    Metadata.builder(currentState.metadata())
                        .putCustom(TrainedModelAssignmentMetadata.NAME, modelAssignments.build())
                        .removeCustom(TrainedModelAssignmentMetadata.DEPRECATED_NAME)
                )
                .build();
        } else {
            return currentState;
        }
    }

    ClusterState createModelAssignment(ClusterState currentState, StartTrainedModelDeploymentAction.TaskParams params) {
        if (MlMetadata.getMlMetadata(currentState).isResetMode()) {
            throw new ElasticsearchStatusException(
                "cannot create new assignment for model [{}] while feature reset is in progress.",
                RestStatus.CONFLICT,
                params.getModelId()
            );
        }
        TrainedModelAssignmentMetadata.Builder builder = TrainedModelAssignmentMetadata.builder(currentState);
        if (builder.hasModel(params.getModelId())) {
            throw new ResourceAlreadyExistsException("assignment for model with id [{}] already exist", params.getModelId());
        }
        TrainedModelAssignment.Builder assignmentBuilder = TrainedModelAssignment.Builder.empty(params);

        Set<String> shuttingDownNodes = nodesShuttingDown(currentState);
        Map<String, String> nodeToReason = new TreeMap<>();
        for (DiscoveryNode node : currentState.getNodes()) {
            if (StartTrainedModelDeploymentAction.TaskParams.mayAssignToNode(node) && shuttingDownNodes.contains(node.getId()) == false) {
                Optional<String> maybeError = nodeHasCapacity(currentState, params, node);
                if (maybeError.isPresent()) {
                    nodeToReason.put(node.getName(), maybeError.get());
                } else {
                    assignmentBuilder.addNewRoutingEntry(node.getId());
                }
            }
        }
        if (nodeToReason.isEmpty() == false) {
            assignmentBuilder.setReason(
                nodeToReason.entrySet()
                    .stream()
                    .map(entry -> String.format(Locale.ROOT, "Not allocating on node [%s]. Reason: %s", entry.getKey(), entry.getValue()))
                    .collect(Collectors.joining("|"))
            );
        }
        builder.addNewAssignment(params.getModelId(), assignmentBuilder);
        return update(currentState, builder);
    }

    static ClusterState setToStopping(ClusterState clusterState, String modelId, String reason) {
        TrainedModelAssignmentMetadata metadata = TrainedModelAssignmentMetadata.fromState(clusterState);
        final TrainedModelAssignment existingAssignment = metadata.getModelAssignment(modelId);
        if (existingAssignment == null) {
            throw new ResourceNotFoundException("assignment for model with id [{}] not found", modelId);
        }
        // If we are stopping, don't update anything
        if (existingAssignment.getAssignmentState().equals(AssignmentState.STOPPING)) {
            return clusterState;
        }
        TrainedModelAssignmentMetadata.Builder builder = TrainedModelAssignmentMetadata.builder(clusterState);
        builder.getAssignment(modelId).stopAssignment(reason);
        return update(clusterState, builder);
    }

    static ClusterState updateModelRoutingTable(ClusterState currentState, UpdateTrainedModelAssignmentStateAction.Request request) {
        final String modelId = request.getModelId();
        final String nodeId = request.getNodeId();
        TrainedModelAssignmentMetadata metadata = TrainedModelAssignmentMetadata.fromState(currentState);
        logger.trace(
            () -> new ParameterizedMessage("[{}] [{}] current metadata before update {}", modelId, nodeId, Strings.toString(metadata))
        );
        final TrainedModelAssignment existingAssignment = metadata.getModelAssignment(modelId);
        final TrainedModelAssignmentMetadata.Builder builder = TrainedModelAssignmentMetadata.builder(currentState);
        // If state is stopped, this indicates the node process is closed, remove the node from the assignment
        if (request.getRoutingState().getState().equals(RoutingState.STOPPED)) {
            if (existingAssignment == null || existingAssignment.isRoutedToNode(nodeId) == false) {
                return currentState;
            }
            builder.getAssignment(modelId).removeRoutingEntry(nodeId).calculateAndSetAssignmentState();
            return update(currentState, builder);
        }

        if (existingAssignment == null) {
            throw new ResourceNotFoundException("assignment for model with id [{}] not found", modelId);
        }
        // If we are stopping, don't update anything
        if (existingAssignment.getAssignmentState().equals(AssignmentState.STOPPING)) {
            logger.debug(
                () -> new ParameterizedMessage(
                    "[{}] requested update from node [{}] to update route state to [{}]",
                    modelId,
                    nodeId,
                    request.getRoutingState()
                )
            );
            return currentState;
        }
        if (existingAssignment.isRoutedToNode(nodeId) == false) {
            throw new ResourceNotFoundException("assignment for model with id [{}]] is not routed to node [{}]", modelId, nodeId);
        }
        builder.getAssignment(modelId).updateExistingRoutingEntry(nodeId, request.getRoutingState()).calculateAndSetAssignmentState();

        return update(currentState, builder);
    }

    static ClusterState removeAssignment(ClusterState currentState, String modelId) {
        TrainedModelAssignmentMetadata.Builder builder = TrainedModelAssignmentMetadata.builder(currentState);
        if (builder.hasModel(modelId) == false) {
            throw new ResourceNotFoundException("assignment for model with id [{}] not found", modelId);
        }
        return update(currentState, builder.removeAssignment(modelId));
    }

    static ClusterState removeAllAssignments(ClusterState currentState) {
        if (TrainedModelAssignmentMetadata.fromState(currentState).modelAssignments().isEmpty()) {
            return currentState;
        }
        return ClusterState.builder(currentState)
            .metadata(
                Metadata.builder(currentState.metadata())
                    .putCustom(TrainedModelAssignmentMetadata.NAME, TrainedModelAssignmentMetadata.Builder.empty().build())
                    .removeCustom(TrainedModelAssignmentMetadata.DEPRECATED_NAME)
                    .build()
            )
            .build();
    }

    ClusterState addRemoveAssignmentNodes(ClusterState currentState) {
        final TrainedModelAssignmentMetadata previousState = TrainedModelAssignmentMetadata.fromState(currentState);
        final TrainedModelAssignmentMetadata.Builder builder = TrainedModelAssignmentMetadata.builder(currentState);
        Set<String> shuttingDownNodes = nodesShuttingDown(currentState);
        Map<String, DiscoveryNode> currentEligibleNodes = currentState.getNodes()
            .stream()
            // TODO: Change when we update `mayAllocateToNode`
            .filter(
                node -> shuttingDownNodes.contains(node.getId()) == false
                    && StartTrainedModelDeploymentAction.TaskParams.mayAssignToNode(node)
            )
            .collect(Collectors.toMap(DiscoveryNode::getId, Function.identity()));
        // TODO: make more efficient, we iterate every entry, sorting by nodes routed (fewest to most)
        previousState.modelAssignments()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().getAssignmentState().equals(AssignmentState.STOPPING) == false)
            .sorted(Comparator.comparing(e -> e.getValue().getNodeRoutingTable().size()))
            .forEach(modelAssignmentEntry -> {
                final String modelId = modelAssignmentEntry.getKey();
                Map<String, String> nodeToReason = new TreeMap<>();
                for (DiscoveryNode node : currentEligibleNodes.values()) {
                    if (modelAssignmentEntry.getValue().isRoutedToNode(node.getId()) == false) {
                        Optional<String> failure = builder.isChanged() ?
                        // We use the builder only if we have changed, there is no point in creating a new object if we haven't changed
                        nodeHasCapacity(currentState, builder, modelAssignmentEntry.getValue().getTaskParams(), node)
                            : nodeHasCapacity(currentState, modelAssignmentEntry.getValue().getTaskParams(), node);
                        if (failure.isPresent()) {
                            nodeToReason.put(node.getName(), failure.get());
                        } else {
                            builder.getAssignment(modelId).addNewRoutingEntry(node.getId());
                        }
                    }
                }
                if (nodeToReason.isEmpty() == false) {
                    builder.getAssignment(modelId)
                        .setReason(
                            nodeToReason.entrySet()
                                .stream()
                                .map(
                                    entry -> String.format(
                                        Locale.ROOT,
                                        "Not allocating on node [%s]. Reason: %s",
                                        entry.getKey(),
                                        entry.getValue()
                                    )
                                )
                                .collect(Collectors.joining("|"))
                        );
                } else {
                    builder.getAssignment(modelId).clearReason();
                }
                for (String nodeId : modelAssignmentEntry.getValue().getNodeRoutingTable().keySet()) {
                    if (currentEligibleNodes.containsKey(nodeId) == false) {
                        builder.getAssignment(modelId).removeRoutingEntry(nodeId);
                    }
                }
                // It may be we moved from STARTED to PARTIALLY_STARTED with the addition of new nodes
                // Or moved from PARTIALLY_STARTED to STARTED if a node was removed
                builder.getAssignment(modelId).calculateAndSetAssignmentState();
            });
        return update(currentState, builder);
    }

    static boolean shouldAllocateModels(final ClusterChangedEvent event) {
        // If there are no assignments created at all, there is nothing to update
        final TrainedModelAssignmentMetadata newMetadata = event.state().getMetadata().custom(TrainedModelAssignmentMetadata.NAME);
        if (newMetadata == null) {
            return false;
        }

        // Reallocate in reaction to either node change events or
        // changes triggered by the node shutdown API.
        // When the shutdown API is used the metadata is modified
        // before the node is removed and then once again after
        // the node has returned. In this situation the node change
        // events become a no-op due to the checks against shutting
        // down nodes and because reassignment has already been
        // triggered by the node shutdown metadata changes.
        //
        // If the shutdown API is not used the node change events
        // are sufficient to cause a reassignment.
        //
        // Shutdowns should be respected so that the service does not
        // allocate models to a node that is about to leave the cluster
        boolean nodesShutdownChanged = event.changedCustomMetadataSet().contains(NodesShutdownMetadata.TYPE);
        if (event.nodesChanged() || nodesShutdownChanged) {
            Set<String> shuttingDownNodes = nodesShuttingDown(event.state());
            DiscoveryNodes.Delta nodesDelta = event.nodesDelta();

            Set<String> removedNodes = nodesDelta.removedNodes().stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
            Set<String> addedNodes = nodesDelta.addedNodes().stream().map(DiscoveryNode::getId).collect(Collectors.toSet());

            Set<String> exitingShutDownNodes;
            if (nodesShutdownChanged) {
                Set<String> previousShuttingDownNodes = nodesShuttingDown(event.previousState());

                // Add nodes that where marked for shutdown in the previous state
                // but are no longer marked as shutdown in the current state.
                Set<String> returningShutDownNodes = Sets.difference(previousShuttingDownNodes, shuttingDownNodes);
                addedNodes.addAll(returningShutDownNodes);

                // and nodes that are marked for shutdown in this event only
                exitingShutDownNodes = Sets.difference(shuttingDownNodes, previousShuttingDownNodes);
                removedNodes.addAll(exitingShutDownNodes);
            } else {
                exitingShutDownNodes = Collections.emptySet();
            }

            for (TrainedModelAssignment trainedModelAssignment : newMetadata.modelAssignments().values()) {
                if (trainedModelAssignment.getAssignmentState().equals(AssignmentState.STOPPING)) {
                    continue;
                }
                for (var nodeId : exitingShutDownNodes) {
                    if (trainedModelAssignment.isRoutedToNode(nodeId)) {
                        return true;
                    }
                }

                for (var nodeId : removedNodes) {
                    if (trainedModelAssignment.isRoutedToNode(nodeId) && shuttingDownNodes.contains(nodeId) == false) {
                        return true;
                    }
                }
                for (var nodeId : addedNodes) {
                    if (StartTrainedModelDeploymentAction.TaskParams.mayAssignToNode(event.state().nodes().get(nodeId))
                        && shuttingDownNodes.contains(nodeId) == false) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    Optional<String> nodeHasCapacity(ClusterState state, StartTrainedModelDeploymentAction.TaskParams params, DiscoveryNode node) {
        NodeLoad load = nodeLoadDetector.detectNodeLoad(state, node, maxOpenJobs, maxMemoryPercentage, useAuto);
        return handleNodeLoad(load, node.getId(), params);
    }

    /**
     *  Gather current node capacity taking the passed assignment metadata into account instead of the one stored in cluster state.
     */
    Optional<String> nodeHasCapacity(
        ClusterState state,
        TrainedModelAssignmentMetadata.Builder builder,
        StartTrainedModelDeploymentAction.TaskParams params,
        DiscoveryNode node
    ) {
        NodeLoad load = nodeLoadDetector.detectNodeLoad(state, builder.build(), node, maxOpenJobs, maxMemoryPercentage, useAuto);
        return handleNodeLoad(load, node.getId(), params);
    }

    Optional<String> handleNodeLoad(NodeLoad load, String nodeId, StartTrainedModelDeploymentAction.TaskParams params) {
        if (Strings.isNullOrEmpty(load.getError()) == false) {
            logger.warn("[{}] failed to calculate current node load with error [{}]", params.getModelId(), nodeId);
            return Optional.of(load.getError());
        }
        if (load.remainingJobs() == 0) {
            return Optional.of(
                ParameterizedMessage.format(
                    "This node is full. Number of opened jobs and allocated native inference processes [{}], {} [{}].",
                    new Object[] { load.getNumAssignedJobs(), MachineLearning.MAX_OPEN_JOBS_PER_NODE.getKey(), maxOpenJobs }
                )
            );
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
     * Returns the set of nodes that are currently shutting down
     */
    static Set<String> nodesShuttingDown(final ClusterState state) {
        return NodesShutdownMetadata.getShutdowns(state)
            .map(NodesShutdownMetadata::getAllNodeMetadataMap)
            .map(Map::keySet)
            .orElse(Collections.emptySet());
    }

}
