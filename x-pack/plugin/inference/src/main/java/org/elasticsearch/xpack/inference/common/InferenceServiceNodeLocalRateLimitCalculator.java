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
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchService;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockService;
import org.elasticsearch.xpack.inference.services.anthropic.AnthropicService;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioService;
import org.elasticsearch.xpack.inference.services.cohere.CohereService;
import org.elasticsearch.xpack.inference.services.deepseek.DeepSeekService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.googleaistudio.GoogleAiStudioService;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiService;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceService;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxService;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIService;
import org.elasticsearch.xpack.inference.services.mistral.MistralService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.TreeMap;
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

    @FunctionalInterface
    private interface MaxNodesPerGroupingStrategy {

        Integer calculate(Integer numberOfNodesInCluster);

    }

    private static final Logger logger = LogManager.getLogger(InferenceServiceNodeLocalRateLimitCalculator.class);

    private final InferenceServiceRegistry serviceRegistry;

    private final ConcurrentHashMap<String, Map<TaskType, RateLimitAssignment>> serviceAssignments;

    private final SortedMap<String, SortedMap<TaskType, MaxNodesPerGroupingStrategy>> serviceNodeLocalRateLimitConfigs;

    @Inject
    public InferenceServiceNodeLocalRateLimitCalculator(ClusterService clusterService, InferenceServiceRegistry serviceRegistry) {
        clusterService.addListener(this);
        this.serviceRegistry = serviceRegistry;
        this.serviceAssignments = new ConcurrentHashMap<>();
        this.serviceNodeLocalRateLimitConfigs = createServiceNodeLocalRateLimitConfigs();
    }

    private SortedMap<String, SortedMap<TaskType, MaxNodesPerGroupingStrategy>> createServiceNodeLocalRateLimitConfigs() {
        TreeMap<String, TreeMap<TaskType, MaxNodesPerGroupingStrategy>> serviceNodeLocalRateLimitConfigs = new TreeMap<>();

        MaxNodesPerGroupingStrategy defaultStrategy = (numNodesInCluster) -> DEFAULT_MAX_NODES_PER_GROUPING;

        // Alibaba Cloud Search
        TreeMap<TaskType, MaxNodesPerGroupingStrategy> alibabaCloudSearchConfigs = new TreeMap<>();
        var alibabaCloudSearchService = serviceRegistry.getService(AlibabaCloudSearchService.NAME);
        if (alibabaCloudSearchService.isPresent()) {
            var alibabaCloudSearchTaskTypes = alibabaCloudSearchService.get().supportedTaskTypes();
            for (TaskType taskType : alibabaCloudSearchTaskTypes) {
                alibabaCloudSearchConfigs.put(taskType, defaultStrategy);
            }
        }
        serviceNodeLocalRateLimitConfigs.put(AlibabaCloudSearchService.NAME, alibabaCloudSearchConfigs);

        // Amazon Bedrock
        TreeMap<TaskType, MaxNodesPerGroupingStrategy> amazonBedrockConfigs = new TreeMap<>();
        var amazonBedrockService = serviceRegistry.getService(AmazonBedrockService.NAME);
        if (amazonBedrockService.isPresent()) {
            var amazonBedrockTaskTypes = amazonBedrockService.get().supportedTaskTypes();
            for (TaskType taskType : amazonBedrockTaskTypes) {
                amazonBedrockConfigs.put(taskType, defaultStrategy);
            }
        }
        serviceNodeLocalRateLimitConfigs.put(AmazonBedrockService.NAME, amazonBedrockConfigs);

        // Anthropic
        TreeMap<TaskType, MaxNodesPerGroupingStrategy> anthropicConfigs = new TreeMap<>();
        var anthropicService = serviceRegistry.getService(AnthropicService.NAME);
        if (anthropicService.isPresent()) {
            var anthropicTaskTypes = anthropicService.get().supportedTaskTypes();
            for (TaskType taskType : anthropicTaskTypes) {
                anthropicConfigs.put(taskType, defaultStrategy);
            }
        }
        serviceNodeLocalRateLimitConfigs.put(AnthropicService.NAME, anthropicConfigs);

        // Azure AI Studio
        TreeMap<TaskType, MaxNodesPerGroupingStrategy> azureAiStudioConfigs = new TreeMap<>();
        var azureAiStudioService = serviceRegistry.getService(AzureAiStudioService.NAME);
        if (azureAiStudioService.isPresent()) {
            var azureAiStudioTaskTypes = azureAiStudioService.get().supportedTaskTypes();
            for (TaskType taskType : azureAiStudioTaskTypes) {
                azureAiStudioConfigs.put(taskType, defaultStrategy);
            }
        }
        serviceNodeLocalRateLimitConfigs.put(AzureAiStudioService.NAME, azureAiStudioConfigs);

        // Cohere
        TreeMap<TaskType, MaxNodesPerGroupingStrategy> cohereConfigs = new TreeMap<>();
        var cohereService = serviceRegistry.getService(CohereService.NAME);
        if (cohereService.isPresent()) {
            var cohereTaskTypes = cohereService.get().supportedTaskTypes();
            for (TaskType taskType : cohereTaskTypes) {
                cohereConfigs.put(taskType, defaultStrategy);
            }
        }
        serviceNodeLocalRateLimitConfigs.put(CohereService.NAME, cohereConfigs);

        // DeepSeek
        TreeMap<TaskType, MaxNodesPerGroupingStrategy> deepSeekConfigs = new TreeMap<>();
        var deepSeekService = serviceRegistry.getService(DeepSeekService.NAME);
        if (deepSeekService.isPresent()) {
            var deepSeekTaskTypes = deepSeekService.get().supportedTaskTypes();
            for (TaskType taskType : deepSeekTaskTypes) {
                deepSeekConfigs.put(taskType, defaultStrategy);
            }
        }
        serviceNodeLocalRateLimitConfigs.put(DeepSeekService.NAME, deepSeekConfigs);

        // Elastic Inference Service (EIS)
        TreeMap<TaskType, MaxNodesPerGroupingStrategy> elasticInferenceConfigs = new TreeMap<>();
        var elasticInferenceService = serviceRegistry.getService(ElasticInferenceService.NAME);
        if (elasticInferenceService.isPresent()) {
            var elasticInferenceTaskTypes = elasticInferenceService.get().supportedTaskTypes();
            for (TaskType taskType : elasticInferenceTaskTypes) {
                elasticInferenceConfigs.put(taskType, defaultStrategy);
            }
        }
        serviceNodeLocalRateLimitConfigs.put(ElasticInferenceService.NAME, elasticInferenceConfigs);

        // Google AI Studio
        TreeMap<TaskType, MaxNodesPerGroupingStrategy> googleAiStudioConfigs = new TreeMap<>();
        var googleAiStudioService = serviceRegistry.getService(GoogleAiStudioService.NAME);
        if (googleAiStudioService.isPresent()) {
            var googleAiStudioTaskTypes = googleAiStudioService.get().supportedTaskTypes();
            for (TaskType taskType : googleAiStudioTaskTypes) {
                googleAiStudioConfigs.put(taskType, defaultStrategy);
            }
        }
        serviceNodeLocalRateLimitConfigs.put(GoogleAiStudioService.NAME, googleAiStudioConfigs);

        // Google Vertex AI
        TreeMap<TaskType, MaxNodesPerGroupingStrategy> googleVertexAiConfigs = new TreeMap<>();
        var googleVertexAiService = serviceRegistry.getService(GoogleVertexAiService.NAME);
        if (googleVertexAiService.isPresent()) {
            var googleVertexAiTaskTypes = googleVertexAiService.get().supportedTaskTypes();
            for (TaskType taskType : googleVertexAiTaskTypes) {
                googleVertexAiConfigs.put(taskType, defaultStrategy);
            }
        }
        serviceNodeLocalRateLimitConfigs.put(GoogleVertexAiService.NAME, googleVertexAiConfigs);

        // HuggingFace
        TreeMap<TaskType, MaxNodesPerGroupingStrategy> huggingFaceConfigs = new TreeMap<>();
        var huggingFaceService = serviceRegistry.getService(HuggingFaceService.NAME);
        if (huggingFaceService.isPresent()) {
            var huggingFaceTaskTypes = huggingFaceService.get().supportedTaskTypes();
            for (TaskType taskType : huggingFaceTaskTypes) {
                huggingFaceConfigs.put(taskType, defaultStrategy);
            }
        }
        serviceNodeLocalRateLimitConfigs.put(HuggingFaceService.NAME, huggingFaceConfigs);

        // IBM Watson X
        TreeMap<TaskType, MaxNodesPerGroupingStrategy> ibmWatsonxConfigs = new TreeMap<>();
        var ibmWatsonxService = serviceRegistry.getService(IbmWatsonxService.NAME);
        if (ibmWatsonxService.isPresent()) {
            var ibmWatsonxTaskTypes = ibmWatsonxService.get().supportedTaskTypes();
            for (TaskType taskType : ibmWatsonxTaskTypes) {
                ibmWatsonxConfigs.put(taskType, defaultStrategy);
            }
        }
        serviceNodeLocalRateLimitConfigs.put(IbmWatsonxService.NAME, ibmWatsonxConfigs);

        // Jina AI
        TreeMap<TaskType, MaxNodesPerGroupingStrategy> jinaAiConfigs = new TreeMap<>();
        var jinaAiService = serviceRegistry.getService(JinaAIService.NAME);
        if (jinaAiService.isPresent()) {
            var jinaAiTaskTypes = jinaAiService.get().supportedTaskTypes();
            for (TaskType taskType : jinaAiTaskTypes) {
                jinaAiConfigs.put(taskType, defaultStrategy);
            }
        }
        serviceNodeLocalRateLimitConfigs.put(JinaAIService.NAME, jinaAiConfigs);

        // Mistral
        TreeMap<TaskType, MaxNodesPerGroupingStrategy> mistralConfigs = new TreeMap<>();
        var mistralService = serviceRegistry.getService(MistralService.NAME);
        if (mistralService.isPresent()) {
            var mistralTaskTypes = mistralService.get().supportedTaskTypes();
            for (TaskType taskType : mistralTaskTypes) {
                mistralConfigs.put(taskType, defaultStrategy);
            }
        }
        serviceNodeLocalRateLimitConfigs.put(MistralService.NAME, mistralConfigs);

        return Collections.unmodifiableSortedMap(serviceNodeLocalRateLimitConfigs);
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
        Map<TaskType, MaxNodesPerGroupingStrategy> serviceConfigs = serviceNodeLocalRateLimitConfigs.get(serviceName);
        return serviceConfigs != null && serviceConfigs.containsKey(taskType);
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
        PriorityQueue<Map.Entry<Integer, DiscoveryNode>> nodeAssignmentCounts = createNodeAssignmentPriorityQueue(sortedNodes);

        // Sort inference services by name (every node lands on the same result)
        var sortedServices = new ArrayList<>(serviceRegistry.getServices().values());
        sortedServices.sort(Comparator.comparing(InferenceService::name));

        for (String serviceName : serviceNodeLocalRateLimitConfigs.keySet()) {
            Optional<InferenceService> service = serviceRegistry.getService(serviceName);

            if (service.isPresent()) {
                var inferenceService = service.get();

                for (TaskType taskType : serviceNodeLocalRateLimitConfigs.get(serviceName).keySet()) {
                    Map<TaskType, RateLimitAssignment> perTaskTypeAssignments = new HashMap<>();
                    var maxNodesPerGroupingStrategy = serviceNodeLocalRateLimitConfigs.get(serviceName).get(taskType);

                    // Calculate node assignments needed for re-routing
                    var assignedNodes = calculateServiceTaskTypeAssignment(maxNodesPerGroupingStrategy, nodeAssignmentCounts);

                    // Update rate limits to be "node-local"
                    var numAssignedNodes = assignedNodes.size();
                    updateRateLimits(inferenceService, taskType, numAssignedNodes);

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

    private PriorityQueue<Map.Entry<Integer, DiscoveryNode>> createNodeAssignmentPriorityQueue(List<DiscoveryNode> sortedNodes) {
        PriorityQueue<Map.Entry<Integer, DiscoveryNode>> nodeAssignmentCounts = new PriorityQueue<>(
            Comparator.comparingInt((Map.Entry<Integer, DiscoveryNode> o) -> o.getKey()).thenComparing(o -> o.getValue().getId())
        );

        for (DiscoveryNode node : sortedNodes) {
            nodeAssignmentCounts.add(Map.entry(0, node));
        }
        return nodeAssignmentCounts;
    }

    private List<DiscoveryNode> calculateServiceTaskTypeAssignment(
        MaxNodesPerGroupingStrategy maxNodesPerGroupingStrategy,
        PriorityQueue<Map.Entry<Integer, DiscoveryNode>> nodeAssignmentCounts
    ) {
        // Use a priority queue to prioritize nodes with the fewest assignments
        int numberOfNodes = nodeAssignmentCounts.size();
        int nodesPerGrouping = Math.min(numberOfNodes, maxNodesPerGroupingStrategy.calculate(numberOfNodes));

        List<DiscoveryNode> assignedNodes = new ArrayList<>();

        // Assign nodes by repeatedly picking the one with the fewest assignments
        for (int j = 0; j < nodesPerGrouping; j++) {
            Map.Entry<Integer, DiscoveryNode> fewestAssignments = nodeAssignmentCounts.poll();

            if (fewestAssignments == null) {
                logger.warn("Node assignment queue is empty. Stopping node local rate limiting assignment.");
                break;
            }

            DiscoveryNode nodeToAssign = fewestAssignments.getValue();
            int currentAssignmentCount = fewestAssignments.getKey();

            assignedNodes.add(nodeToAssign);
            nodeAssignmentCounts.add(Map.entry(currentAssignmentCount + 1, nodeToAssign));
        }

        return assignedNodes;
    }

    private void updateRateLimits(InferenceService service, TaskType taskType, int responsibleNodes) {
        if ((service instanceof SenderService) == false) {
            return;
        }

        SenderService senderService = (SenderService) service;
        Sender sender = senderService.getSender();
        sender.updateRateLimitDivisor(service.name(), taskType, responsibleNodes);
    }

    InferenceServiceRegistry serviceRegistry() {
        return serviceRegistry;
    }

    SortedMap<String, SortedMap<TaskType, MaxNodesPerGroupingStrategy>> serviceNodeLocalRateLimitConfigs() {
        return serviceNodeLocalRateLimitConfigs;
    }
}
