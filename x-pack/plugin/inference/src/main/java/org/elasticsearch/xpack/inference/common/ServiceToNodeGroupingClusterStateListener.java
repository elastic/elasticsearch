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
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.injection.guice.Inject;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class ServiceToNodeGroupingClusterStateListener implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(ServiceToNodeGroupingClusterStateListener.class);

    private final InferenceServiceRegistry serviceRegistry;

    private final AtomicReference<Map<String, DiscoveryNode>> serviceToNodeGrouping = new AtomicReference<>();

    @Inject
    public ServiceToNodeGroupingClusterStateListener(ClusterService clusterService, InferenceServiceRegistry serviceRegistry) {
        clusterService.addListener(this);
        logger.info("ServiceToNodeGroupingClusterStateListener registered");
        this.serviceRegistry = serviceRegistry;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        boolean clusterTopologyChange = event.nodesChanged();

        // Every node should land on the same grouping by calculation, so no need to put anything into the cluster state
        if (clusterTopologyChange) {
            var newClusterState = event.state();
            var nodes = newClusterState.nodes().getAllNodes();
            var streamingCompletionServices = completionServices();

            // Nodes sorted by name
            var nodeList = new ArrayList<>(nodes);
            nodeList.sort(Comparator.comparing(DiscoveryNode::getName));

            // Services sorted by name
            streamingCompletionServices.sort(Comparator.comparing(InferenceService::name));

            var newServiceToNodeGrouping = calculateServiceToNodeMapping(nodeList, streamingCompletionServices);

            serviceToNodeGrouping.set(newServiceToNodeGrouping);

            logger.info("Services to node grouping: {}", serviceToNodeGrouping.get());
        }
    }

    private Map<String, DiscoveryNode> calculateServiceToNodeMapping(
        List<DiscoveryNode> sortedNodes,
        List<InferenceService> sortedServices
    ) {
        Map<String, DiscoveryNode> mapping = new HashMap<>();
        int numberOfNodes = sortedNodes.size();
        int numberOfServices = sortedServices.size();

        for (int i = 0; i < numberOfServices; i++) {
            // Wrap around nodes if services > nodes
            DiscoveryNode assignedNode = sortedNodes.get(i % numberOfNodes);
            mapping.put(sortedServices.get(i).name(), assignedNode);
        }

        return mapping;
    }

    public Map<String, DiscoveryNode> serviceToNodeGrouping() {
        return serviceToNodeGrouping.get();
    }

    public List<InferenceService> completionServices() {
        var services = serviceRegistry.getServices();
        List<InferenceService> streamingCompletionServices = new ArrayList<>();

        for (InferenceService inferenceService : services.values()) {
            // TODO: get only services with active endpoints?
            // TODO: change to streaming after POC
            if (inferenceService.supportedTaskTypes().contains(TaskType.COMPLETION)) {
                streamingCompletionServices.add(inferenceService);
            }
        }

        return streamingCompletionServices;
    }
}
