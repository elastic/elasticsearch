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
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
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
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelAllocationStateAction;
import org.elasticsearch.xpack.core.ml.inference.allocation.AllocationState;
import org.elasticsearch.xpack.core.ml.inference.allocation.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.allocation.TrainedModelAllocation;
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

public class TrainedModelAllocationClusterService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(TrainedModelAllocationClusterService.class);

    private final ClusterService clusterService;
    private final NodeLoadDetector nodeLoadDetector;
    private volatile int maxMemoryPercentage;
    private volatile boolean useAuto;
    private volatile int maxOpenJobs;

    public TrainedModelAllocationClusterService(Settings settings, ClusterService clusterService, NodeLoadDetector nodeLoadDetector) {
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
    private static <T extends ClusterStateUpdateTask> ClusterStateTaskExecutor<T> newExecutor() {
        return ClusterStateTaskExecutor.unbatched();
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

                    // As this produces a cluster state update task, we are certain that if the persistent
                    // task framework results in assigning some ML tasks on that same cluster state change
                    // we do not end up over-allocating a node. Both this service and the persistant task service
                    // will produce a cluster state update but the one that gets applied first wins. The other
                    // update will be rejected and we will retry to assign getting a correct update on available memory
                    // on each node.
                    // Also, note that as this service is a returned as a component of the ML plugin,
                    // and components are created before persistent task executors, we will try to allocate
                    // trained models before we try to assign ML persistent tasks.
                    return addRemoveAllocationNodes(currentState);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn("failed to allocate models", e);
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    logger.trace(
                        () -> new ParameterizedMessage(
                            "updated model allocations based on node changes in the cluster; new metadata [{}]",
                            Strings.toString(TrainedModelAllocationMetadata.fromState(newState), false, true)
                        )
                    );
                }
            }, newExecutor());
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
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                listener.onResponse(AcknowledgedResponse.TRUE);
            }
        }, newExecutor());
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
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                listener.onResponse(TrainedModelAllocationMetadata.fromState(newState).getModelAllocation(params.getModelId()));
            }
        }, newExecutor());
    }

    public void setModelAllocationToStopping(String modelId, ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask("set model allocation stopping", new ClusterStateUpdateTask() {
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
        }, newExecutor());
    }

    public void removeModelAllocation(String modelId, ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask("delete model allocation", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return removeAllocation(currentState, modelId);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                listener.onResponse(AcknowledgedResponse.TRUE);
            }
        }, newExecutor());
    }

    // Used by the reset action directly
    public void removeAllModelAllocations(ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask("delete all model allocations", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return removeAllAllocations(currentState);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                listener.onResponse(AcknowledgedResponse.TRUE);
            }
        }, newExecutor());
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
        if (MlMetadata.getMlMetadata(currentState).isResetMode()) {
            throw new ElasticsearchStatusException(
                "cannot create new allocation for model [{}] while feature reset is in progress.",
                RestStatus.CONFLICT,
                params.getModelId()
            );
        }
        TrainedModelAllocationMetadata.Builder builder = TrainedModelAllocationMetadata.builder(currentState);
        if (builder.hasModel(params.getModelId())) {
            throw new ResourceAlreadyExistsException("allocation for model with id [{}] already exist", params.getModelId());
        }
        TrainedModelAllocation.Builder allocationBuilder = TrainedModelAllocation.Builder.empty(params);

        Set<String> shuttingDownNodes = nodesShuttingDown(currentState);
        Map<String, String> nodeToReason = new TreeMap<>();
        for (DiscoveryNode node : currentState.getNodes()) {
            if (StartTrainedModelDeploymentAction.TaskParams.mayAllocateToNode(node) && shuttingDownNodes.contains(node.getId()) == false) {
                Optional<String> maybeError = nodeHasCapacity(currentState, params, node);
                if (maybeError.isPresent()) {
                    nodeToReason.put(node.getName(), maybeError.get());
                } else {
                    allocationBuilder.addNewRoutingEntry(node.getId());
                }
            }
        }
        if (nodeToReason.isEmpty() == false) {
            allocationBuilder.setReason(
                nodeToReason.entrySet()
                    .stream()
                    .map(entry -> String.format(Locale.ROOT, "Not allocating on node [%s]. Reason: %s", entry.getKey(), entry.getValue()))
                    .collect(Collectors.joining("|"))
            );
        }
        builder.addNewAllocation(params.getModelId(), allocationBuilder);
        return update(currentState, builder);
    }

    static ClusterState setToStopping(ClusterState clusterState, String modelId, String reason) {
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
        builder.getAllocation(modelId).stopAllocation(reason);
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
        final TrainedModelAllocationMetadata.Builder builder = TrainedModelAllocationMetadata.builder(currentState);
        // If state is stopped, this indicates the node process is closed, remove the node from the allocation
        if (request.getRoutingState().getState().equals(RoutingState.STOPPED)) {
            if (existingAllocation == null || existingAllocation.isRoutedToNode(nodeId) == false) {
                return currentState;
            }
            builder.getAllocation(modelId).removeRoutingEntry(nodeId).calculateAndSetAllocationState();
            return update(currentState, builder);
        }

        if (existingAllocation == null) {
            throw new ResourceNotFoundException("allocation for model with id [{}] not found", modelId);
        }
        // If we are stopping, don't update anything
        if (existingAllocation.getAllocationState().equals(AllocationState.STOPPING)) {
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
        if (existingAllocation.isRoutedToNode(nodeId) == false) {
            throw new ResourceNotFoundException("allocation for model with id [{}]] is not routed to node [{}]", modelId, nodeId);
        }
        builder.getAllocation(modelId).updateExistingRoutingEntry(nodeId, request.getRoutingState()).calculateAndSetAllocationState();

        return update(currentState, builder);
    }

    static ClusterState removeAllocation(ClusterState currentState, String modelId) {
        TrainedModelAllocationMetadata.Builder builder = TrainedModelAllocationMetadata.builder(currentState);
        if (builder.hasModel(modelId) == false) {
            throw new ResourceNotFoundException("allocation for model with id [{}] not found", modelId);
        }
        return update(currentState, builder.removeAllocation(modelId));
    }

    static ClusterState removeAllAllocations(ClusterState currentState) {
        if (TrainedModelAllocationMetadata.fromState(currentState).modelAllocations().isEmpty()) {
            return currentState;
        }
        return ClusterState.builder(currentState)
            .metadata(
                Metadata.builder(currentState.metadata())
                    .putCustom(TrainedModelAllocationMetadata.NAME, TrainedModelAllocationMetadata.Builder.empty().build())
                    .build()
            )
            .build();
    }

    ClusterState addRemoveAllocationNodes(ClusterState currentState) {
        final TrainedModelAllocationMetadata previousState = TrainedModelAllocationMetadata.fromState(currentState);
        final TrainedModelAllocationMetadata.Builder builder = TrainedModelAllocationMetadata.builder(currentState);
        Set<String> shuttingDownNodes = nodesShuttingDown(currentState);
        Map<String, DiscoveryNode> currentEligibleNodes = currentState.getNodes()
            .stream()
            // TODO: Change when we update `mayAllocateToNode`
            .filter(
                node -> shuttingDownNodes.contains(node.getId()) == false
                    && StartTrainedModelDeploymentAction.TaskParams.mayAllocateToNode(node)
            )
            .collect(Collectors.toMap(DiscoveryNode::getId, Function.identity()));
        // TODO: make more efficient, we iterate every entry, sorting by nodes routed (fewest to most)
        previousState.modelAllocations()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().getAllocationState().equals(AllocationState.STOPPING) == false)
            .sorted(Comparator.comparing(e -> e.getValue().getNodeRoutingTable().size()))
            .forEach(modelAllocationEntry -> {
                final String modelId = modelAllocationEntry.getKey();
                Map<String, String> nodeToReason = new TreeMap<>();
                for (DiscoveryNode node : currentEligibleNodes.values()) {
                    if (modelAllocationEntry.getValue().isRoutedToNode(node.getId()) == false) {
                        Optional<String> failure = builder.isChanged() ?
                        // We use the builder only if we have changed, there is no point in creating a new object if we haven't changed
                        nodeHasCapacity(currentState, builder, modelAllocationEntry.getValue().getTaskParams(), node)
                            : nodeHasCapacity(currentState, modelAllocationEntry.getValue().getTaskParams(), node);
                        if (failure.isPresent()) {
                            nodeToReason.put(node.getName(), failure.get());
                        } else {
                            builder.getAllocation(modelId).addNewRoutingEntry(node.getId());
                        }
                    }
                }
                if (nodeToReason.isEmpty() == false) {
                    builder.getAllocation(modelId)
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
                    builder.getAllocation(modelId).clearReason();
                }
                for (String nodeId : modelAllocationEntry.getValue().getNodeRoutingTable().keySet()) {
                    if (currentEligibleNodes.containsKey(nodeId) == false) {
                        builder.getAllocation(modelId).removeRoutingEntry(nodeId);
                    }
                }
                // It may be we moved from STARTED to PARTIALLY_STARTED with the addition of new nodes
                // Or moved from PARTIALLY_STARTED to STARTED if a node was removed
                builder.getAllocation(modelId).calculateAndSetAllocationState();
            });
        return update(currentState, builder);
    }

    static boolean shouldAllocateModels(final ClusterChangedEvent event) {
        // If there are no allocations created at all, there is nothing to update
        final TrainedModelAllocationMetadata newMetadata = event.state().getMetadata().custom(TrainedModelAllocationMetadata.NAME);
        if (newMetadata == null) {
            return false;
        }

        // Reallocate in reaction to either node change events or
        // changes triggered by the node shutdown API.
        // When the shutdown API is used the metadata is modified
        // before the node is removed and then once again after
        // the node has returned. In this situation the node change
        // events become a no-op due to the checks against shutting
        // down nodes and because reallocation has already been
        // triggered by the node shutdown metadata changes.
        //
        // If the shutdown API is not used the node change events
        // are sufficient to cause a reallocation.
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

            for (TrainedModelAllocation trainedModelAllocation : newMetadata.modelAllocations().values()) {
                if (trainedModelAllocation.getAllocationState().equals(AllocationState.STOPPING)) {
                    continue;
                }
                for (var nodeId : exitingShutDownNodes) {
                    if (trainedModelAllocation.isRoutedToNode(nodeId)) {
                        return true;
                    }
                }

                for (var nodeId : removedNodes) {
                    if (trainedModelAllocation.isRoutedToNode(nodeId) && shuttingDownNodes.contains(nodeId) == false) {
                        return true;
                    }
                }
                for (var nodeId : addedNodes) {
                    if (StartTrainedModelDeploymentAction.TaskParams.mayAllocateToNode(event.state().nodes().get(nodeId))
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
     *  Gather current node capacity taking the passed allocation metadata into account instead of the one stored in cluster state.
     */
    Optional<String> nodeHasCapacity(
        ClusterState state,
        TrainedModelAllocationMetadata.Builder builder,
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
        // If any ML processes are running on a node we require some space to load the shared libraries.
        // So if none are currently running then this per-node overhead must be added to the requirement.
        long requiredMemory = params.estimateMemoryUsageBytes() + ((load.getNumAssignedJobs() == 0)
            ? MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
            : 0);
        if (load.getFreeMemory() < requiredMemory) {
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
                        requiredMemory,
                        ByteSizeValue.ofBytes(requiredMemory).toString() }
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
