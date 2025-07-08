/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest;
import org.elasticsearch.xpack.inference.action.BaseTransportInferenceAction;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Note: {@link InferenceAPIClusterAwareRateLimitingFeature} needs to be enabled for this class to get
 * instantiated inside {@link org.elasticsearch.xpack.inference.InferencePlugin} and be available via dependency injection.
 *
 * Calculates and manages node-local rate limits for inference services based on changes in the cluster topology.
 * This calculator calculates a "node-local" rate-limit, which essentially divides the rate limit for a service/task type
 * through the number of nodes, which got assigned to this service/task type pair. Without this calculator the rate limit stored
 * in the inference endpoint configuration would get effectively multiplied by the number of nodes in a cluster (assuming a ~ uniform
 * distribution of requests to the nodes in the cluster).
 *
 * The calculator works in conjunction with several other components:
 * - {@link BaseTransportInferenceAction} - Uses the calculator to determine, whether to reroute a request or not
 * - {@link BaseInferenceActionRequest} - Tracks, if the request (an instance of a subclass of {@link BaseInferenceActionRequest})
 *   already got re-routed at least once
 * - {@link HttpRequestSender} - Provides original rate limits that this calculator divides through the number of nodes
 *   responsible for a service/task type
 */
public class InferenceServiceNodeLocalRateLimitCalculator implements InferenceServiceRateLimitCalculator {

    public static final Integer DEFAULT_MAX_NODES_PER_GROUPING = 3;

    /**
     * Configuration mapping services to their task type rate limiting settings.
     * Each service can have multiple configs defining:
     * - Which task types support request re-routing and "node-local" rate limit calculation
     * - How many nodes should handle requests for each task type, based on cluster size (dynamically calculated or statically provided)
     **/
    static final Map<String, Collection<NodeLocalRateLimitConfig>> SERVICE_NODE_LOCAL_RATE_LIMIT_CONFIGS = Map.of(
        ElasticInferenceService.NAME,
        // TODO: should probably be a map/set
        List.of(new NodeLocalRateLimitConfig(TaskType.SPARSE_EMBEDDING, (numNodesInCluster) -> DEFAULT_MAX_NODES_PER_GROUPING))
    );

    record NodeLocalRateLimitConfig(TaskType taskType, MaxNodesPerGroupingStrategy maxNodesPerGroupingStrategy) {}

    @FunctionalInterface
    private interface MaxNodesPerGroupingStrategy {

        Integer calculate(Integer numberOfNodesInCluster);

    }

    private static final Logger logger = LogManager.getLogger(InferenceServiceNodeLocalRateLimitCalculator.class);

    private final InferenceServiceRegistry serviceRegistry;

    private final ConcurrentHashMap<String, Map<TaskType, RateLimitAssignment>> serviceAssignments;

    @Inject
    public InferenceServiceNodeLocalRateLimitCalculator(ClusterService clusterService, InferenceServiceRegistry serviceRegistry) {
        clusterService.addListener(this);
        this.serviceRegistry = serviceRegistry;
        this.serviceAssignments = new ConcurrentHashMap<>();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        boolean clusterTopologyChanged = event.nodesChanged();

        // TODO: feature flag per node? We should not reroute to nodes not having eis and/or the inference plugin enabled
        // Every node should land on the same grouping by calculation, so no need to put anything into the cluster state
        if (clusterTopologyChanged) {
            updateAssignments(event);
        }
    }

    public boolean isTaskTypeReroutingSupported(String serviceName, TaskType taskType) {
        return SERVICE_NODE_LOCAL_RATE_LIMIT_CONFIGS.getOrDefault(serviceName, Collections.emptyList())
            .stream()
            .anyMatch(rateLimitConfig -> taskType.equals(rateLimitConfig.taskType));
    }

    public RateLimitAssignment getRateLimitAssignment(String service, TaskType taskType) {
        var assignmentsPerTaskType = serviceAssignments.get(service);

        if (assignmentsPerTaskType == null) {
            return null;
        }

        return assignmentsPerTaskType.get(taskType);
    }

    /**
     * Updates instances of {@link RateLimitAssignment} for each service and task type when the cluster topology changes.
     * For each service and supported task type, calculates which nodes should handle requests
     * and what their local rate limits should be per inference endpoint.
     */
    private void updateAssignments(ClusterChangedEvent event) {
        var newClusterState = event.state();
        var nodes = newClusterState.nodes().getAllNodes();

        // Sort nodes by id (every node lands on the same result)
        var sortedNodes = nodes.stream().sorted(Comparator.comparing(DiscoveryNode::getId)).toList();

        // Sort inference services by name (every node lands on the same result)
        var sortedServices = new ArrayList<>(serviceRegistry.getServices().values());
        sortedServices.sort(Comparator.comparing(InferenceService::name));

        for (String serviceName : SERVICE_NODE_LOCAL_RATE_LIMIT_CONFIGS.keySet()) {
            Optional<InferenceService> service = serviceRegistry.getService(serviceName);

            if (service.isPresent()) {
                var inferenceService = service.get();

                for (NodeLocalRateLimitConfig rateLimitConfig : SERVICE_NODE_LOCAL_RATE_LIMIT_CONFIGS.get(serviceName)) {
                    Map<TaskType, RateLimitAssignment> perTaskTypeAssignments = new HashMap<>();
                    TaskType taskType = rateLimitConfig.taskType();

                    // Calculate node assignments needed for re-routing
                    var assignedNodes = calculateServiceAssignment(rateLimitConfig.maxNodesPerGroupingStrategy(), sortedNodes);

                    // Update rate limits to be "node-local"
                    var numAssignedNodes = assignedNodes.size();
                    updateRateLimits(inferenceService, numAssignedNodes);

                    perTaskTypeAssignments.put(taskType, new RateLimitAssignment(assignedNodes));
                    serviceAssignments.put(serviceName, perTaskTypeAssignments);
                }
            } else {
                logger.warn(
                    "Service [{}] is configured for node-local rate limiting but was not found in the service registry",
                    serviceName
                );
            }
        }
    }

    private List<DiscoveryNode> calculateServiceAssignment(
        MaxNodesPerGroupingStrategy maxNodesPerGroupingStrategy,
        List<DiscoveryNode> sortedNodes
    ) {
        int numberOfNodes = sortedNodes.size();
        int nodesPerGrouping = Math.min(numberOfNodes, maxNodesPerGroupingStrategy.calculate(numberOfNodes));

        List<DiscoveryNode> assignedNodes = new ArrayList<>();

        // TODO: here we can probably be smarter: if |num nodes in cluster| > |num nodes per task types|
        // -> make sure a service provider is not assigned the same nodes for all task types; only relevant as soon as we support more task
        // types
        for (int j = 0; j < nodesPerGrouping; j++) {
            var assignedNode = sortedNodes.get(j % numberOfNodes);
            assignedNodes.add(assignedNode);
        }

        return assignedNodes;
    }

    private void updateRateLimits(InferenceService service, int responsibleNodes) {
        if ((service instanceof SenderService) == false) {
            return;
        }

        SenderService senderService = (SenderService) service;
        Sender sender = senderService.getSender();
        // TODO: this needs to take in service and task type as soon as multiple services/task types are supported
        sender.updateRateLimitDivisor(responsibleNodes);
    }

    InferenceServiceRegistry serviceRegistry() {
        return serviceRegistry;
    }
}
