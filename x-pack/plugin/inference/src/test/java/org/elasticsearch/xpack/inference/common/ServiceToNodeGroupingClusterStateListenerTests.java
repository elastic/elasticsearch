/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.inference.InferencePlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class ServiceToNodeGroupingClusterStateListenerTests extends ESIntegTestCase {

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testInitialClusterGroupingCorrect() {
        // Start cluster with multiple nodes
        var numNodes = randomIntBetween(2, 5);
        var nodeNames = internalCluster().startNodes(numNodes);
        ensureStableCluster(numNodes);
        Map<String, DiscoveryNode> serviceToNodeGrouping = null;

        for (String nodeName : nodeNames) {
            var serviceToNodeGroupingClusterStateListener = internalCluster().getInstance(
                ServiceToNodeGroupingClusterStateListener.class,
                nodeName
            );
            var streamingCompletionServices = serviceToNodeGroupingClusterStateListener.completionServices();

            // First service to node grouping
            if (serviceToNodeGrouping == null) {
                serviceToNodeGrouping = serviceToNodeGroupingClusterStateListener.serviceToNodeGrouping();

                // Contains a service to node mapping for every streaming completion service
                assertThat(streamingCompletionServices.size(), equalTo(serviceToNodeGrouping.size()));
                for (var service : streamingCompletionServices) {
                    assertThat(serviceToNodeGrouping.containsKey(service.name()), equalTo(true));
                }
            } else {
                // Service to node grouping is the same across all nodes
                assertThat(serviceToNodeGroupingClusterStateListener.serviceToNodeGrouping(), equalTo(serviceToNodeGrouping));
            }
        }

        // Assure that the service to node grouping happened
        assertThat(serviceToNodeGrouping, notNullValue());
    }

    public void testAllServicesMapToOneNodeAfterAllExceptOneLeft() throws IOException {
        // Start cluster with multiple nodes
        var numNodes = randomIntBetween(2, 5);
        var nodeNames = internalCluster().startNodes(numNodes);
        ensureStableCluster(numNodes);

        var nodeLeftInCluster = nodeNames.getFirst();
        var currentNumberOfNodes = numNodes;

        // Stop all nodes except one
        for (String nodeName : nodeNames) {
            // One node should be left in cluster
            if (nodeName.equals(nodeLeftInCluster)) {
                continue;
            }

            internalCluster().stopNode(nodeName);
            currentNumberOfNodes--;
            ensureStableCluster(currentNumberOfNodes);
        }

        var serviceToNodeGroupingClusterStateListener = internalCluster().getInstance(
            ServiceToNodeGroupingClusterStateListener.class,
            nodeLeftInCluster
        );
        var streamingCompletionServices = serviceToNodeGroupingClusterStateListener.completionServices();
        var serviceToNodeGrouping = serviceToNodeGroupingClusterStateListener.serviceToNodeGrouping();

        // All services map to the same node
        assertThat(streamingCompletionServices.size(), equalTo(serviceToNodeGrouping.size()));
        for (var service : streamingCompletionServices) {
            assertThat(serviceToNodeGrouping.get(service.name()).getName(), equalTo(nodeLeftInCluster));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InferencePlugin.class);
    }
}
