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
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
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
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelAssignmentRoutingInfoAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentState;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlPlatformArchitecturesUtil;
import org.elasticsearch.xpack.core.ml.utils.TransportVersionUtils;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.autoscaling.NodeAvailabilityZoneMapper;
import org.elasticsearch.xpack.ml.inference.assignment.planning.AllocationReducer;
import org.elasticsearch.xpack.ml.job.NodeLoad;
import org.elasticsearch.xpack.ml.job.NodeLoadDetector;
import org.elasticsearch.xpack.ml.notifications.SystemAuditor;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.Request.NUMBER_OF_ALLOCATIONS;
import static org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentUtils.NODES_CHANGED_REASON;
import static org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentUtils.createShuttingDownRoute;

public class TrainedModelAssignmentClusterService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(TrainedModelAssignmentClusterService.class);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final NodeLoadDetector nodeLoadDetector;
    private final SystemAuditor systemAuditor;
    private final NodeAvailabilityZoneMapper nodeAvailabilityZoneMapper;
    private final Client client;
    private volatile int maxMemoryPercentage;
    private volatile boolean useAuto;
    private volatile int maxOpenJobs;
    protected volatile int maxLazyMLNodes;
    protected volatile long maxMLNodeSize;
    protected volatile int allocatedProcessorsScale;

    public TrainedModelAssignmentClusterService(
        Settings settings,
        ClusterService clusterService,
        ThreadPool threadPool,
        NodeLoadDetector nodeLoadDetector,
        SystemAuditor systemAuditor,
        NodeAvailabilityZoneMapper nodeAvailabilityZoneMapper,
        Client client
    ) {
        this.clusterService = Objects.requireNonNull(clusterService);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.nodeLoadDetector = Objects.requireNonNull(nodeLoadDetector);
        this.systemAuditor = Objects.requireNonNull(systemAuditor);
        this.nodeAvailabilityZoneMapper = Objects.requireNonNull(nodeAvailabilityZoneMapper);
        this.maxMemoryPercentage = MachineLearning.MAX_MACHINE_MEMORY_PERCENT.get(settings);
        this.useAuto = MachineLearningField.USE_AUTO_MACHINE_MEMORY_PERCENT.get(settings);
        this.maxOpenJobs = MachineLearning.MAX_OPEN_JOBS_PER_NODE.get(settings);
        this.maxLazyMLNodes = MachineLearningField.MAX_LAZY_ML_NODES.get(settings);
        this.maxMLNodeSize = MachineLearning.MAX_ML_NODE_SIZE.get(settings).getBytes();
        this.allocatedProcessorsScale = MachineLearning.ALLOCATED_PROCESSORS_SCALE.get(settings);
        this.client = client;
        // Only nodes that can possibly be master nodes really need this service running
        if (DiscoveryNode.isMasterNode(settings)) {
            clusterService.addListener(this);
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(MachineLearning.MAX_MACHINE_MEMORY_PERCENT, this::setMaxMemoryPercentage);
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(MachineLearningField.USE_AUTO_MACHINE_MEMORY_PERCENT, this::setUseAuto);
            clusterService.getClusterSettings().addSettingsUpdateConsumer(MachineLearning.MAX_OPEN_JOBS_PER_NODE, this::setMaxOpenJobs);
            clusterService.getClusterSettings().addSettingsUpdateConsumer(MachineLearningField.MAX_LAZY_ML_NODES, this::setMaxLazyMLNodes);
            clusterService.getClusterSettings().addSettingsUpdateConsumer(MachineLearning.MAX_ML_NODE_SIZE, this::setMaxMLNodeSize);
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(MachineLearning.ALLOCATED_PROCESSORS_SCALE, this::setAllocatedProcessorsScale);
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

    private void setMaxLazyMLNodes(int value) {
        this.maxLazyMLNodes = value;
    }

    private void setMaxMLNodeSize(ByteSizeValue value) {
        this.maxMLNodeSize = value.getBytes();
    }

    private void setAllocatedProcessorsScale(int scale) {
        this.allocatedProcessorsScale = scale;
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (eventStateHasGlobalBlockStateNotRecoveredBlock(event)) {
            return;
        }
        if (event.localNodeMaster() == false) {
            return;
        }

        if (event.nodesAdded()) {
            logMlNodeHeterogeneity();
        }

        Optional<String> rebalanceReason = detectReasonToRebalanceModels(event);
        if (rebalanceReason.isPresent()) {
            // As this produces a cluster state update task, we are certain that if the persistent
            // task framework results in assigning some ML tasks on that same cluster state change
            // we do not end up over-allocating a node. Both this service and the persistent task service
            // will produce a cluster state update but the one that gets applied first wins. The other
            // update will be rejected and we will retry to assign getting a correct update on available memory
            // on each node.
            rebalanceAssignments(
                event.state(),
                Optional.empty(),
                rebalanceReason.get(),
                ActionListener.wrap(
                    newMetadata -> logger.debug(
                        () -> format("rebalanced model assignments [%s]", Strings.toString(newMetadata, false, true))
                    ),
                    e -> logger.warn("failed to rebalance models", e)
                )
            );
        }
    }

    boolean eventStateHasGlobalBlockStateNotRecoveredBlock(ClusterChangedEvent event) {
        return event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK);
    }

    void logMlNodeHeterogeneity() {
        ActionListener<Set<String>> architecturesListener = getArchitecturesSetActionListener();
        MlPlatformArchitecturesUtil.getMlNodesArchitecturesSet(
            architecturesListener,
            client,
            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)
        );
    }

    static ActionListener<Set<String>> getArchitecturesSetActionListener() {
        ActionListener<Set<String>> architecturesListener = new ActionListener<Set<String>>() {
            @Override
            public void onResponse(Set<String> architectures) {
                if (architectures.size() > 1) {
                    logger.warn(
                        format(
                            "Heterogeneous platform architectures were detected among ML nodes. "
                                + "This will prevent the deployment of some trained models. Distinct platform architectures detected: %s",
                            architectures
                        )
                    );
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to detect heterogeneity among ML nodes with exception: ", e);
            }
        };
        return architecturesListener;
    }

    private void removeRoutingToRemovedOrShuttingDownNodes(ClusterChangedEvent event) {
        if (areAssignedNodesRemoved(event)) {
            submitUnbatchedTask("removing routing entries for removed or shutting down nodes", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return removeRoutingToUnassignableNodes(currentState);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("could not remove routing entries for removed or shutting down nodes", e);
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    logger.debug(
                        () -> format(
                            "updated model assignments based on node changes in the cluster; new metadata [%s]",
                            Strings.toString(TrainedModelAssignmentMetadata.fromState(newState), false, true)
                        )
                    );
                }
            });
        }
    }

    // Visible for testing
    static boolean areAssignedNodesRemoved(ClusterChangedEvent event) {
        boolean nodesShutdownChanged = event.changedCustomClusterMetadataSet().contains(NodesShutdownMetadata.TYPE);
        if (event.nodesRemoved() || nodesShutdownChanged) {
            Set<String> removedOrShuttingDownNodeIds = new HashSet<>(nodesShuttingDown(event.state()));
            event.nodesDelta().removedNodes().stream().map(DiscoveryNode::getId).forEach(removedOrShuttingDownNodeIds::add);

            TrainedModelAssignmentMetadata metadata = TrainedModelAssignmentMetadata.fromState(event.state());
            for (TrainedModelAssignment assignment : metadata.allAssignments().values()) {
                if (Sets.intersection(removedOrShuttingDownNodeIds, assignment.getNodeRoutingTable().keySet()).isEmpty() == false) {
                    return true;
                }
            }
        }
        return false;
    }

    // Visible for testing
    static ClusterState removeRoutingToUnassignableNodes(ClusterState currentState) {
        Set<String> assignableNodes = getAssignableNodes(currentState).stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
        TrainedModelAssignmentMetadata metadata = TrainedModelAssignmentMetadata.fromState(currentState);
        TrainedModelAssignmentMetadata.Builder builder = TrainedModelAssignmentMetadata.builder(currentState);
        Set<String> shuttingDownNodes = currentState.metadata().nodeShutdowns().getAllNodeIds();

        for (TrainedModelAssignment assignment : metadata.allAssignments().values()) {
            Set<String> routedNodeIdsToRemove = Sets.difference(assignment.getNodeRoutingTable().keySet(), assignableNodes);
            if (routedNodeIdsToRemove.isEmpty() == false) {
                logger.debug(
                    () -> format(
                        "[%s] removing routing entries to nodes %s because they have been removed or are shutting down",
                        assignment.getDeploymentId(),
                        routedNodeIdsToRemove
                    )
                );

                /*
                 * This code is to handle the following edge case.
                 *
                 * This code was added in 8.11.0. This code path is hit if the version of a node in the cluster is less than 8.4.0.
                 * - A rolling upgrade is performed from a version less than 8.4.0
                 * - The versions of the nodes in the cluster is mixed between 8.11.0 and less than 8.4.0
                 * - The master node upgrades to 8.11.0 (which will have this code change)
                 * - An ML node upgrades to 8.11.0 and begins shutting down
                 * - A data node exists on a version less than 8.4.0
                 *
                 * The ML node that is shutting down will go through the graceful shutdown by having any routes referencing it, set to
                 * stopping. The TrainedModelAssignmentNodeService will be notified of the change and see that the route is in stopping
                 * and complete any remaining work in the processes before stopping them.
                 *
                 * If in the future we can simplify and remove this edge case code that'd be ideal.
                 */
                TrainedModelAssignment.Builder assignmentBuilder = removeRoutingBuilder(
                    routedNodeIdsToRemove,
                    shuttingDownNodes,
                    assignment
                );

                builder.updateAssignment(assignment.getDeploymentId(), assignmentBuilder.calculateAndSetAssignmentState());
            }
        }
        return update(currentState, builder);
    }

    private static TrainedModelAssignment.Builder removeRoutingBuilder(
        Set<String> nodeIds,
        Set<String> shuttingDownNodes,
        TrainedModelAssignment assignment
    ) {
        TrainedModelAssignment.Builder assignmentBuilder = TrainedModelAssignment.Builder.fromAssignment(assignment);

        for (String nodeIdToRemove : nodeIds) {
            RoutingInfo routingInfoToRemove = assignment.getNodeRoutingTable().get(nodeIdToRemove);

            if (shuttingDownNodes.contains(nodeIdToRemove) == false) {
                logger.debug(
                    () -> format("[%s] Removing route for unassignable node id [%s]", assignment.getDeploymentId(), nodeIdToRemove)
                );

                assignmentBuilder.removeRoutingEntry(nodeIdToRemove);
            } else if (routingInfoToRemove != null && routingInfoToRemove.getState().isAnyOf(RoutingState.STARTED, RoutingState.STARTING)) {
                logger.debug(
                    () -> format(
                        "[%s] Found assignment with route to shutting down node id [%s], adding stopping route",
                        assignment.getDeploymentId(),
                        nodeIdToRemove
                    )
                );

                RoutingInfo stoppingRouteInfo = createShuttingDownRoute(assignment.getNodeRoutingTable().get(nodeIdToRemove));
                assignmentBuilder.addOrOverwriteRoutingEntry(nodeIdToRemove, stoppingRouteInfo);
            }

        }

        return assignmentBuilder;
    }

    public void updateModelRoutingTable(
        UpdateTrainedModelAssignmentRoutingInfoAction.Request request,
        ActionListener<AcknowledgedResponse> listener
    ) {
        logger.debug(
            () -> format(
                "[%s] updating routing table entry for node [%s], update [%s]",
                request.getDeploymentId(),
                request.getNodeId(),
                request.getUpdate()
            )
        );
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
        CreateTrainedModelAssignmentAction.Request request,
        ActionListener<TrainedModelAssignment> listener
    ) {
        if (MlMetadata.getMlMetadata(clusterService.state()).isResetMode()) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "cannot create new assignment [{}] for model [{}] while feature reset is in progress.",
                    RestStatus.CONFLICT,
                    request.getTaskParams().getDeploymentId(),
                    request.getTaskParams().getModelId()
                )
            );
            return;
        }

        rebalanceAssignments(clusterService.state(), Optional.of(request), "model deployment started", ActionListener.wrap(newMetadata -> {
            TrainedModelAssignment assignment = newMetadata.getDeploymentAssignment(request.getTaskParams().getDeploymentId());
            if (assignment == null) {
                // If we could not allocate the model anywhere then it is possible the assignment
                // here is null. We should notify the listener of an empty assignment as the
                // handling of this is done elsewhere with the wait-to-start predicate.
                assignment = TrainedModelAssignment.Builder.empty(request).build();
            }
            listener.onResponse(assignment);
        }, listener::onFailure));
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

    public void removeModelAssignment(String deploymentId, ActionListener<AcknowledgedResponse> listener) {
        submitUnbatchedTask("delete model deployment assignment", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return removeAssignment(currentState, deploymentId);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                // As a model deployment has been stopped we should rebalance as we might now
                // be able to satisfy more allocations for the rest of the deployments.
                rebalanceAssignments(
                    newState,
                    Optional.empty(),
                    "model deployment stopped",
                    ActionListener.wrap(
                        metadataAfterRebalance -> logger.debug(
                            () -> format("Successfully rebalanced model deployments after deployment [%s] was stopped", deploymentId)
                        ),
                        e -> logger.error(
                            format("Failed to rebalance model deployments after deployment [%s] was stopped", deploymentId),
                            e
                        )
                    )
                );
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
        TrainedModelAssignmentMetadata previousMetadata = TrainedModelAssignmentMetadata.fromState(currentState);
        TrainedModelAssignmentMetadata updatedMetadata = modelAssignments.build();
        if (updatedMetadata.equals(previousMetadata)) {
            return currentState;
        } else {
            return forceUpdate(currentState, modelAssignments);
        }
    }

    private static ClusterState forceUpdate(ClusterState currentState, TrainedModelAssignmentMetadata.Builder modelAssignments) {
        logger.debug(() -> format("updated assignments: %s", modelAssignments.build()));
        Metadata.Builder metadata = Metadata.builder(currentState.metadata());
        metadata.putCustom(TrainedModelAssignmentMetadata.NAME, modelAssignments.build())
            .removeProjectCustom(TrainedModelAssignmentMetadata.DEPRECATED_NAME);
        return ClusterState.builder(currentState).metadata(metadata).build();
    }

    ClusterState createModelAssignment(ClusterState currentState, CreateTrainedModelAssignmentAction.Request request) throws Exception {
        return update(currentState, rebalanceAssignments(currentState, Optional.of(request)));
    }

    private void rebalanceAssignments(
        ClusterState clusterState,
        Optional<CreateTrainedModelAssignmentAction.Request> createAssignmentRequest,
        String reason,
        ActionListener<TrainedModelAssignmentMetadata> listener
    ) {
        ActionListener<Set<String>> architecturesListener = ActionListener.wrap((mlNodesArchitectures) -> {
            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {
                logger.debug(() -> format("Rebalancing model allocations because [%s]", reason));

                TrainedModelAssignmentMetadata.Builder rebalancedMetadata;
                try {
                    rebalancedMetadata = rebalanceAssignments(clusterState, createAssignmentRequest);
                } catch (Exception e) {
                    listener.onFailure(e);
                    return;
                }

                submitUnbatchedTask(reason, new ClusterStateUpdateTask() {

                    private volatile boolean isUpdated;
                    private volatile boolean isChanged;

                    @Override
                    public ClusterState execute(ClusterState currentState) {

                        currentState = stopPlatformSpecificModelsInHeterogeneousClusters(
                            currentState,
                            mlNodesArchitectures,
                            createAssignmentRequest.map(CreateTrainedModelAssignmentAction.Request::getTaskParams),
                            clusterState
                        );

                        if (areClusterStatesCompatibleForRebalance(clusterState, currentState)) {
                            isUpdated = true;
                            ClusterState updatedState = update(currentState, rebalancedMetadata);
                            isChanged = updatedState != currentState;
                            return updatedState;
                        }

                        rebalanceAssignments(currentState, createAssignmentRequest, reason, listener);
                        return currentState;
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }

                    @Override
                    public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                        if (isUpdated) {
                            if (isChanged) {
                                threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)
                                    .execute(
                                        () -> systemAuditor.info(Messages.getMessage(Messages.INFERENCE_DEPLOYMENT_REBALANCED, reason))
                                    );
                            }
                            listener.onResponse(TrainedModelAssignmentMetadata.fromState(newState));
                        }
                    }
                });
            });
        }, listener::onFailure);

        MlPlatformArchitecturesUtil.getMlNodesArchitecturesSet(
            architecturesListener,
            client,
            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)
        );
    }

    ClusterState stopPlatformSpecificModelsInHeterogeneousClusters(
        ClusterState updatedState,
        Set<String> mlNodesArchitectures,
        Optional<StartTrainedModelDeploymentAction.TaskParams> modelToAdd,
        ClusterState clusterState
    ) {
        if (mlNodesArchitectures.size() > 1 && modelToAdd.isPresent()) {
            String reasonToStop = format(
                "ML nodes in this cluster have multiple platform architectures, "
                    + "but can only have one for this model ([%s]); "
                    + "detected architectures: %s",
                modelToAdd.get().getModelId(),
                mlNodesArchitectures
            );
            updatedState = callSetToStopping(reasonToStop, modelToAdd.get().getDeploymentId(), clusterState);
        }
        return updatedState;
    }

    ClusterState callSetToStopping(String reasonToStop, String deploymentId, ClusterState clusterState) {
        return setToStopping(clusterState, deploymentId, reasonToStop);
    }

    private boolean areClusterStatesCompatibleForRebalance(ClusterState source, ClusterState target) {
        List<DiscoveryNode> sourceNodes = getAssignableNodes(source);
        List<DiscoveryNode> targetNodes = getAssignableNodes(target);
        // We also compare node loads as it could be that another ML job has been started meanwhile
        return sourceNodes.equals(targetNodes)
            && detectNodeLoads(sourceNodes, source).equals(detectNodeLoads(targetNodes, target))
            && MlMetadata.getMlMetadata(source).equals(MlMetadata.getMlMetadata(target))
            && TrainedModelAssignmentMetadata.fromState(source).equals(TrainedModelAssignmentMetadata.fromState(target));
    }

    private TrainedModelAssignmentMetadata.Builder rebalanceAssignments(
        ClusterState currentState,
        Optional<CreateTrainedModelAssignmentAction.Request> createAssignmentRequest
    ) throws Exception {
        List<DiscoveryNode> nodes = getAssignableNodes(currentState);
        logger.debug(() -> format("assignable nodes are %s", nodes.stream().map(DiscoveryNode::getId).toList()));
        Map<DiscoveryNode, NodeLoad> nodeLoads = detectNodeLoads(nodes, currentState);
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.fromState(currentState);

        boolean useNewMemoryFields = TrainedModelAssignment.useNewMemoryFields(TransportVersionUtils.getMinTransportVersion(currentState));
        TrainedModelAssignmentRebalancer rebalancer = new TrainedModelAssignmentRebalancer(
            currentMetadata,
            nodeLoads,
            nodeAvailabilityZoneMapper.buildMlNodesByAvailabilityZone(currentState),
            createAssignmentRequest,
            allocatedProcessorsScale,
            useNewMemoryFields
        );

        Set<String> shuttingDownNodeIds = currentState.metadata().nodeShutdowns().getAllNodeIds();
        /*
         * To signal that we should gracefully stop the deployments routed to a particular node we set the routing state to stopping.
         * The TrainedModelAssignmentNodeService will see that the route is in stopping for a shutting down node and gracefully shut down
         * the native process after draining the queues.
         */
        TrainedModelAssignmentMetadata.Builder rebalanced = setShuttingDownNodeRoutesToStopping(
            currentMetadata,
            shuttingDownNodeIds,
            rebalancer.rebalance()
        );

        if (createAssignmentRequest.isPresent()) {
            checkModelIsFullyAllocatedIfScalingIsNotPossible(
                createAssignmentRequest.get().getTaskParams().getDeploymentId(),
                rebalanced,
                nodes
            );
        }

        return rebalanced;
    }

    // Default for testing
    static TrainedModelAssignmentMetadata.Builder setShuttingDownNodeRoutesToStopping(
        TrainedModelAssignmentMetadata currentMetadata,
        Set<String> shuttingDownNodeIds,
        TrainedModelAssignmentMetadata.Builder builder
    ) {
        if (shuttingDownNodeIds.isEmpty()) {
            return builder;
        }

        for (TrainedModelAssignment existingAssignment : currentMetadata.allAssignments().values()) {
            boolean foundShuttingDownNodeForAssignment = false;

            String existingDeploymentId = existingAssignment.getDeploymentId();
            TrainedModelAssignment.Builder assignmentBuilder = builder.hasModelDeployment(existingAssignment.getDeploymentId())
                ? builder.getAssignment(existingDeploymentId)
                : TrainedModelAssignment.Builder.fromAssignment(existingAssignment)
                    /*
                     * If this code path happens that means that the assignment originally existed prior to the rebalance and then
                     * disappeared. This would be an anomaly so we'll set the assignment to stopping and attempt to gracefully shut down
                     * the native process.
                     */
                    .stopAssignment(NODES_CHANGED_REASON)
                    // If there are other routes that are now outdated after the rebalance we don't want to include them, so let's start
                    // with a fresh table
                    .clearNodeRoutingTable();

            for (String nodeId : shuttingDownNodeIds) {
                if (existingAssignment.isRoutedToNode(nodeId)
                    && existingAssignment.getNodeRoutingTable()
                        .get(nodeId)
                        .getState()
                        .isAnyOf(RoutingState.STARTED, RoutingState.STARTING)) {
                    logger.debug(
                        () -> format(
                            "Found assignment deployment id: [%s] with route to shutting down node id: [%s], adding stopping route",
                            existingDeploymentId,
                            nodeId
                        )
                    );

                    foundShuttingDownNodeForAssignment = true;
                    RoutingInfo stoppingRouteInfo = createShuttingDownRoute(existingAssignment.getNodeRoutingTable().get(nodeId));

                    assignmentBuilder.addOrOverwriteRoutingEntry(nodeId, stoppingRouteInfo);
                }
            }

            // if we didn't find a shutting down routing info then we don't want to add an empty assignment here
            if (foundShuttingDownNodeForAssignment) {
                builder.addOrOverwriteAssignment(existingDeploymentId, assignmentBuilder);
            }
        }

        return builder;
    }

    private void checkModelIsFullyAllocatedIfScalingIsNotPossible(
        String modelId,
        TrainedModelAssignmentMetadata.Builder assignments,
        List<DiscoveryNode> nodes
    ) {
        TrainedModelAssignment assignment = assignments.getAssignment(modelId).build();
        if (isScalingPossible(nodes) || assignment.isSatisfied(nodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet()))) {
            return;
        }

        if (assignment.getNodeRoutingTable().isEmpty()) {
            String msg = "Could not start deployment because no suitable nodes were found, allocation explanation ["
                + assignment.getReason().orElse("none")
                + "]";
            logger.warn("[{}] {}", modelId, msg);
            Exception detail = new IllegalStateException(msg);
            throw new ElasticsearchStatusException(
                "Could not start deployment because no ML nodes with sufficient capacity were found",
                RestStatus.TOO_MANY_REQUESTS,
                detail
            );
        }

        String msg = "Could not start deployment because there are not enough resources to provide all requested allocations";
        logger.debug(() -> format("[%s] %s", modelId, msg));
        throw new ElasticsearchStatusException(msg, RestStatus.TOO_MANY_REQUESTS);
    }

    private static List<DiscoveryNode> getAssignableNodes(ClusterState clusterState) {
        final Set<String> shuttingDownNodes = nodesShuttingDown(clusterState);
        return clusterState.getNodes()
            .getNodes()
            .values()
            .stream()
            .filter(StartTrainedModelDeploymentAction.TaskParams::mayAssignToNode)
            .filter(n -> shuttingDownNodes.contains(n.getId()) == false)
            .toList();
    }

    private Map<DiscoveryNode, NodeLoad> detectNodeLoads(List<DiscoveryNode> nodes, ClusterState clusterState) {
        return nodes.stream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    n -> nodeLoadDetector.detectNodeLoad(clusterState, null, n, maxOpenJobs, maxMemoryPercentage, useAuto)
                )
            );
    }

    private boolean isScalingPossible(List<DiscoveryNode> nodes) {
        OptionalLong smallestMLNode = nodes.stream().map(NodeLoadDetector::getNodeSize).flatMapToLong(OptionalLong::stream).min();

        // We can scale horizontally
        return maxLazyMLNodes > nodes.size()
            // We can scale vertically

            // TODO This checks if there is more space we could vertically scale to but
            // not if it will be enough for the model to actually fit in. For example,
            // we might be 32GB off of the maximum ML tier size and someone wants to start a 45GB model.
            // As this code stands we'll scale up to maximum size then find we still cannot start that model.
            || (smallestMLNode.isPresent() && smallestMLNode.getAsLong() < maxMLNodeSize);
    }

    public void updateDeployment(
        String deploymentId,
        Integer numberOfAllocations,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings,
        boolean isInternal,
        ActionListener<TrainedModelAssignment> listener
    ) {
        updateDeployment(clusterService.state(), deploymentId, numberOfAllocations, adaptiveAllocationsSettings, isInternal, listener);
    }

    private void updateDeployment(
        ClusterState clusterState,
        String deploymentId,
        Integer numberOfAllocations,
        AdaptiveAllocationsSettings adaptiveAllocationsSettingsUpdates,
        boolean isInternal,
        ActionListener<TrainedModelAssignment> listener
    ) {
        TrainedModelAssignmentMetadata metadata = TrainedModelAssignmentMetadata.fromState(clusterState);
        final TrainedModelAssignment existingAssignment = metadata.getDeploymentAssignment(deploymentId);
        if (existingAssignment == null) {
            listener.onFailure(ExceptionsHelper.missingModelDeployment(deploymentId));
            return;
        }
        AdaptiveAllocationsSettings adaptiveAllocationsSettings = getAdaptiveAllocationsSettings(
            existingAssignment.getAdaptiveAllocationsSettings(),
            adaptiveAllocationsSettingsUpdates
        );
        if (adaptiveAllocationsSettings != null) {
            if (isInternal == false && adaptiveAllocationsSettings.getEnabled() == Boolean.TRUE && numberOfAllocations != null) {
                ValidationException validationException = new ValidationException();
                validationException.addValidationError("[" + NUMBER_OF_ALLOCATIONS + "] cannot be set if adaptive allocations is enabled");
                listener.onFailure(validationException);
                return;
            }
            ActionRequestValidationException validationException = adaptiveAllocationsSettings.validate();
            if (validationException != null) {
                listener.onFailure(validationException);
                return;
            }
        }
        boolean hasUpdates = hasUpdates(numberOfAllocations, adaptiveAllocationsSettingsUpdates, existingAssignment);
        if (hasUpdates == false) {
            logger.debug("no updates to be made for deployment [{}]", deploymentId);
            listener.onResponse(existingAssignment);
            return;
        }
        if (existingAssignment.getAssignmentState() != AssignmentState.STARTED) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "cannot update deployment that is not in [{}] state",
                    RestStatus.CONFLICT,
                    AssignmentState.STARTED
                )
            );
            return;
        }

        ActionListener<TrainedModelAssignmentMetadata.Builder> updatedAssignmentListener = ActionListener.wrap(
            updatedAssignment -> submitUnbatchedTask("update model deployment", new ClusterStateUpdateTask() {

                private volatile boolean isUpdated;

                @Override
                public ClusterState execute(ClusterState currentState) {
                    if (areClusterStatesCompatibleForRebalance(clusterState, currentState)) {
                        isUpdated = true;
                        return update(currentState, updatedAssignment);
                    }
                    logger.debug(() -> format("[%s] Retrying update as cluster state has been modified", deploymentId));
                    updateDeployment(currentState, deploymentId, numberOfAllocations, adaptiveAllocationsSettings, isInternal, listener);
                    return currentState;
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    if (isUpdated) {
                        TrainedModelAssignment updatedAssignment = TrainedModelAssignmentMetadata.fromState(newState)
                            .getDeploymentAssignment(deploymentId);
                        if (updatedAssignment.totalTargetAllocations() > existingAssignment.totalTargetAllocations()) {
                            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)
                                .execute(
                                    () -> systemAuditor.info(
                                        Messages.getMessage(Messages.INFERENCE_DEPLOYMENT_REBALANCED, "model deployment updated")
                                    )
                                );
                        }
                        listener.onResponse(updatedAssignment);
                    }
                }
            }),
            listener::onFailure
        );

        updateAssignment(clusterState, existingAssignment, numberOfAllocations, adaptiveAllocationsSettings, updatedAssignmentListener);
    }

    static boolean hasUpdates(
        Integer proposedNumberOfAllocations,
        AdaptiveAllocationsSettings proposedAdaptiveSettings,
        TrainedModelAssignment existingAssignment
    ) {
        return (proposedNumberOfAllocations != null
            && Objects.equals(proposedNumberOfAllocations, existingAssignment.getTaskParams().getNumberOfAllocations()) == false)
            || (proposedAdaptiveSettings != null
                && Objects.equals(proposedAdaptiveSettings, existingAssignment.getAdaptiveAllocationsSettings()) == false);
    }

    private AdaptiveAllocationsSettings getAdaptiveAllocationsSettings(
        AdaptiveAllocationsSettings original,
        AdaptiveAllocationsSettings updates
    ) {
        if (updates == null) {
            return original;
        } else if (updates == AdaptiveAllocationsSettings.RESET_PLACEHOLDER) {
            return null;
        } else if (original == null) {
            return updates;
        } else {
            return original.merge(updates);
        }
    }

    private void updateAssignment(
        ClusterState clusterState,
        TrainedModelAssignment assignment,
        Integer numberOfAllocations,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings,
        ActionListener<TrainedModelAssignmentMetadata.Builder> listener
    ) {
        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {
            if (numberOfAllocations == null || numberOfAllocations == assignment.getTaskParams().getNumberOfAllocations()) {
                updateAndKeepNumberOfAllocations(clusterState, assignment, adaptiveAllocationsSettings, listener);
            } else if (numberOfAllocations > assignment.getTaskParams().getNumberOfAllocations()) {
                increaseNumberOfAllocations(clusterState, assignment, numberOfAllocations, adaptiveAllocationsSettings, listener);
            } else {
                decreaseNumberOfAllocations(clusterState, assignment, numberOfAllocations, adaptiveAllocationsSettings, listener);
            }
        });
    }

    private void updateAndKeepNumberOfAllocations(
        ClusterState clusterState,
        TrainedModelAssignment assignment,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings,
        ActionListener<TrainedModelAssignmentMetadata.Builder> listener
    ) {
        TrainedModelAssignment.Builder updatedAssignment = TrainedModelAssignment.Builder.fromAssignment(assignment)
            .setAdaptiveAllocationsSettings(adaptiveAllocationsSettings);
        TrainedModelAssignmentMetadata.Builder builder = TrainedModelAssignmentMetadata.builder(clusterState);
        builder.updateAssignment(assignment.getDeploymentId(), updatedAssignment);
        listener.onResponse(builder);
    }

    private void increaseNumberOfAllocations(
        ClusterState clusterState,
        TrainedModelAssignment assignment,
        int numberOfAllocations,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings,
        ActionListener<TrainedModelAssignmentMetadata.Builder> listener
    ) {
        try {
            TrainedModelAssignment.Builder updatedAssignment = TrainedModelAssignment.Builder.fromAssignment(assignment)
                .setNumberOfAllocations(numberOfAllocations)
                .setAdaptiveAllocationsSettings(adaptiveAllocationsSettings);
            final ClusterState updatedClusterState = update(
                clusterState,
                TrainedModelAssignmentMetadata.builder(clusterState).updateAssignment(assignment.getDeploymentId(), updatedAssignment)
            );
            TrainedModelAssignmentMetadata.Builder rebalancedMetadata = rebalanceAssignments(updatedClusterState, Optional.empty());
            if (isScalingPossible(getAssignableNodes(clusterState)) == false
                && rebalancedMetadata.getAssignment(assignment.getDeploymentId()).build().totalTargetAllocations() < numberOfAllocations) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "Could not update deployment because there are not enough resources to provide all requested allocations",
                        RestStatus.TOO_MANY_REQUESTS
                    )
                );
            } else {
                listener.onResponse(rebalancedMetadata);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void decreaseNumberOfAllocations(
        ClusterState clusterState,
        TrainedModelAssignment assignment,
        int numberOfAllocations,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings,
        ActionListener<TrainedModelAssignmentMetadata.Builder> listener
    ) {
        TrainedModelAssignment.Builder updatedAssignment = numberOfAllocations < assignment.totalTargetAllocations()
            ? new AllocationReducer(assignment, nodeAvailabilityZoneMapper.buildMlNodesByAvailabilityZone(clusterState)).reduceTo(
                numberOfAllocations
            )
            : TrainedModelAssignment.Builder.fromAssignment(assignment).setNumberOfAllocations(numberOfAllocations);
        updatedAssignment.setAdaptiveAllocationsSettings(adaptiveAllocationsSettings);
        // We have now reduced allocations to a number we can be sure it is satisfied
        // and thus we should clear the assignment reason.
        if (numberOfAllocations <= assignment.totalTargetAllocations()) {
            updatedAssignment.setReason(null);
        }
        TrainedModelAssignmentMetadata.Builder builder = TrainedModelAssignmentMetadata.builder(clusterState);
        builder.updateAssignment(assignment.getDeploymentId(), updatedAssignment);
        listener.onResponse(builder);
    }

    static ClusterState setToStopping(ClusterState clusterState, String deploymentId, String reason) {
        TrainedModelAssignmentMetadata metadata = TrainedModelAssignmentMetadata.fromState(clusterState);
        final TrainedModelAssignment existingAssignment = metadata.getDeploymentAssignment(deploymentId);
        if (existingAssignment == null) {
            throw new ResourceNotFoundException("assignment with id [{}] not found", deploymentId);
        }
        // If we are stopping, don't update anything
        if (existingAssignment.getAssignmentState().equals(AssignmentState.STOPPING)) {
            return clusterState;
        }
        TrainedModelAssignmentMetadata.Builder builder = TrainedModelAssignmentMetadata.builder(clusterState);
        builder.getAssignment(deploymentId).stopAssignment(reason);
        return update(clusterState, builder);
    }

    static ClusterState updateModelRoutingTable(ClusterState currentState, UpdateTrainedModelAssignmentRoutingInfoAction.Request request) {
        final String deploymentId = request.getDeploymentId();
        final String nodeId = request.getNodeId();
        TrainedModelAssignmentMetadata metadata = TrainedModelAssignmentMetadata.fromState(currentState);
        logger.trace(() -> format("[%s] [%s] current metadata before update %s", deploymentId, nodeId, Strings.toString(metadata)));
        final TrainedModelAssignment existingAssignment = metadata.getDeploymentAssignment(deploymentId);
        final TrainedModelAssignmentMetadata.Builder builder = TrainedModelAssignmentMetadata.builder(currentState);
        // If state is stopped, this indicates the node process is closed, remove the node from the assignment
        if (request.getUpdate().getStateAndReason().isPresent()
            && request.getUpdate().getStateAndReason().get().getState().equals(RoutingState.STOPPED)) {
            if (existingAssignment == null || existingAssignment.isRoutedToNode(nodeId) == false) {
                return currentState;
            }
            builder.getAssignment(deploymentId).removeRoutingEntry(nodeId).calculateAndSetAssignmentState();
            return update(currentState, builder);
        }

        if (existingAssignment == null) {
            throw new ResourceNotFoundException("assignment with id [{}] not found", deploymentId);
        }
        // If we are stopping, don't update anything
        if (existingAssignment.getAssignmentState().equals(AssignmentState.STOPPING)) {
            logger.debug(
                () -> format(
                    "[%s] requested update from node [%s] while stopping; update was [%s]",
                    deploymentId,
                    nodeId,
                    request.getUpdate()
                )
            );
            return currentState;
        }
        if (existingAssignment.isRoutedToNode(nodeId) == false) {
            throw new ResourceNotFoundException("assignment with id [{}]] is not routed to node [{}]", deploymentId, nodeId);
        }
        RoutingInfo routingInfo = existingAssignment.getNodeRoutingTable().get(nodeId);
        builder.getAssignment(deploymentId)
            .updateExistingRoutingEntry(nodeId, request.getUpdate().apply(routingInfo))
            .calculateAndSetAssignmentState();

        return update(currentState, builder);
    }

    static ClusterState removeAssignment(ClusterState currentState, String deploymentId) {
        TrainedModelAssignmentMetadata.Builder builder = TrainedModelAssignmentMetadata.builder(currentState);
        if (builder.hasModelDeployment(deploymentId) == false) {
            throw new ResourceNotFoundException("assignment for deployment with id [{}] not found", deploymentId);
        }
        logger.debug(() -> format("[%s] removing assignment", deploymentId));
        return update(currentState, builder.removeAssignment(deploymentId));
    }

    static ClusterState removeAllAssignments(ClusterState currentState) {
        if (TrainedModelAssignmentMetadata.fromState(currentState).allAssignments().isEmpty()) {
            return currentState;
        }
        return forceUpdate(currentState, TrainedModelAssignmentMetadata.Builder.empty());
    }

    static Optional<String> detectReasonToRebalanceModels(final ClusterChangedEvent event) {
        // If there are no assignments created at all, there is nothing to update
        final TrainedModelAssignmentMetadata newMetadata = TrainedModelAssignmentMetadata.fromState(event.state());
        if (newMetadata == null || newMetadata.allAssignments().isEmpty()) {
            return Optional.empty();
        }

        // If an ML persistent task with process stopped we should rebalance as we could have
        // available memory that we did not have before.
        return detectReasonIfMlJobsStopped(event).or(() -> {
            String reason = null;
            if (haveMlNodesChanged(event, newMetadata)) {
                reason = NODES_CHANGED_REASON;
            } else if (newMetadata.hasOutdatedAssignments()) {
                reason = "outdated assignments detected";
            }
            return Optional.ofNullable(reason);
        });
    }

    static Optional<String> detectReasonIfMlJobsStopped(ClusterChangedEvent event) {
        if (event.changedCustomProjectMetadataSet().contains(PersistentTasksCustomMetadata.TYPE) == false) {
            return Optional.empty();
        }

        PersistentTasksCustomMetadata previousPersistentTasks = PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(
            event.previousState()
        );
        if (previousPersistentTasks == null) { // no previous jobs so nothing has stopped
            return Optional.empty();
        }

        PersistentTasksCustomMetadata currentPersistentTasks = PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(
            event.state()
        );
        Set<String> currentMlTaskIds = findMlProcessTaskIds(currentPersistentTasks);

        Set<PersistentTasksCustomMetadata.PersistentTask<?>> previousMlTasks = MlTasks.findMlProcessTasks(previousPersistentTasks);
        Set<String> stoppedTaskTypes = previousMlTasks.stream()
            .filter(task -> currentMlTaskIds.contains(task.getId()) == false) // remove the tasks that are still present. Stopped Ids only.
            .map(PersistentTasksCustomMetadata.PersistentTask::getTaskName)
            .map(MlTasks::prettyPrintTaskName)
            .collect(Collectors.toSet());
        if (stoppedTaskTypes.size() == 1) {
            return Optional.of("ML [" + stoppedTaskTypes.iterator().next() + "] job stopped");
        } else if (stoppedTaskTypes.size() > 1) {
            return Optional.of("ML " + stoppedTaskTypes + " jobs stopped");
        }
        return Optional.empty();
    }

    private static Set<String> findMlProcessTaskIds(@Nullable PersistentTasksCustomMetadata metadata) {
        return metadata == null
            ? Set.of()
            : MlTasks.findMlProcessTasks(metadata)
                .stream()
                .map(PersistentTasksCustomMetadata.PersistentTask::getId)
                .collect(Collectors.toSet());
    }

    static boolean haveMlNodesChanged(ClusterChangedEvent event, TrainedModelAssignmentMetadata newMetadata) {
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
        //
        // TODO this has a weird side-effect for allocating to nodes
        // If the event indicates there were nodes added/removed, this method only looks at the current state and has
        // no previous knowledge of existing nodes. Consequently, if a model was manually removed (task-kill) from a node
        // it may get re-allocated to that node when another node is added/removed...
        boolean nodesShutdownChanged = event.changedCustomClusterMetadataSet().contains(NodesShutdownMetadata.TYPE);
        if (event.nodesChanged() || nodesShutdownChanged) {
            // This is just to track the various log messages that happen in this function to help with debugging in the future
            // so that we can reasonably assume they're all related
            // If the log messages printed from this method get interlaced across nodes it can make debugging difficult
            var eventIdentity = Long.toHexString(System.nanoTime());

            Set<String> shuttingDownNodes = nodesShuttingDown(event.state());
            DiscoveryNodes.Delta nodesDelta = event.nodesDelta();

            Set<String> removedNodes = nodesDelta.removedNodes().stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
            Set<String> addedNodes = nodesDelta.addedNodes().stream().map(DiscoveryNode::getId).collect(Collectors.toSet());

            logger.debug(
                () -> format(
                    "Initial node change info; identity: %s; removed nodes: %s; added nodes: %s; shutting down nodes: %s",
                    eventIdentity,
                    removedNodes,
                    addedNodes,
                    shuttingDownNodes
                )
            );

            Set<String> exitingShutDownNodes;
            if (nodesShutdownChanged) {
                Set<String> previousShuttingDownNodes = nodesShuttingDown(event.previousState());
                Set<String> presentNodes = event.state().nodes().stream().map(DiscoveryNode::getId).collect(Collectors.toSet());

                // Add nodes that where marked for shutdown in the previous state
                // but are no longer marked as shutdown in the current state.
                // The intersection is to only include the nodes that actually exist
                Set<String> returningShutDownNodes = Sets.intersection(
                    presentNodes,
                    Sets.difference(previousShuttingDownNodes, shuttingDownNodes)
                );
                addedNodes.addAll(returningShutDownNodes);

                // and nodes that are marked for shutdown in this event only
                exitingShutDownNodes = Sets.difference(shuttingDownNodes, previousShuttingDownNodes);
                removedNodes.addAll(exitingShutDownNodes);

                logger.debug(
                    () -> format(
                        "Shutting down nodes were changed; identity: %s; previous shutting down nodes: %s; returning nodes: %s",
                        eventIdentity,
                        previousShuttingDownNodes,
                        returningShutDownNodes
                    )
                );
            } else {
                exitingShutDownNodes = Collections.emptySet();
            }

            logger.debug(
                () -> format(
                    "identity: %s; added nodes %s; removed nodes %s; shutting down nodes %s; exiting shutdown nodes %s",
                    eventIdentity,
                    addedNodes,
                    removedNodes,
                    shuttingDownNodes,
                    exitingShutDownNodes
                )
            );
            for (TrainedModelAssignment trainedModelAssignment : newMetadata.allAssignments().values()) {
                if (trainedModelAssignment.getAssignmentState().equals(AssignmentState.STOPPING)) {
                    continue;
                }
                for (var nodeId : exitingShutDownNodes) {
                    if (trainedModelAssignment.isRoutedToNode(nodeId)
                        // If the route is stopping then it's draining its queue or being forced to stop so let that continue
                        // and don't try to rebalance until it has completely finished
                        && trainedModelAssignment.getNodeRoutingTable().get(nodeId).getState() != RoutingState.STOPPING) {
                        logger.debug(
                            () -> format(
                                "should rebalance because model deployment [%s] has allocations on shutting down node [%s]",
                                trainedModelAssignment.getDeploymentId(),
                                nodeId
                            )
                        );
                        return true;
                    }
                }

                for (var nodeId : removedNodes) {
                    if (trainedModelAssignment.isRoutedToNode(nodeId) && shuttingDownNodes.contains(nodeId) == false) {
                        logger.debug(
                            () -> format(
                                "should rebalance because model deployment [%s] has allocations on removed node [%s]",
                                trainedModelAssignment.getDeploymentId(),
                                nodeId
                            )
                        );
                        return true;
                    }
                }
                for (var nodeId : addedNodes) {
                    if (StartTrainedModelDeploymentAction.TaskParams.mayAssignToNode(event.state().nodes().get(nodeId))
                        && shuttingDownNodes.contains(nodeId) == false) {
                        logger.debug(() -> format("should rebalance because ML eligible node [%s] was added", nodeId));
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Returns the set of nodes that are currently shutting down
     */
    static Set<String> nodesShuttingDown(final ClusterState state) {
        return state.metadata().nodeShutdowns().getAllNodeIds();
    }
}
