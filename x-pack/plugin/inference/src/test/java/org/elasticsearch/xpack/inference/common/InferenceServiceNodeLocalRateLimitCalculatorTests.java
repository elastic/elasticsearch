/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import static org.elasticsearch.xpack.inference.common.InferenceServiceNodeLocalRateLimitCalculator.DEFAULT_MAX_NODES_PER_GROUPING;
import static org.elasticsearch.xpack.inference.common.InferenceServiceNodeLocalRateLimitCalculator.SERVICE_NODE_LOCAL_RATE_LIMIT_CONFIGS;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class InferenceServiceNodeLocalRateLimitCalculatorTests extends ESIntegTestCase {

    public void setUp() throws Exception {
        super.setUp();
    }

    public void testInitialClusterGrouping_Correct() {
        // Start with 2-5 nodes
        var numNodes = randomIntBetween(2, 5);
        var nodeNames = internalCluster().startNodes(numNodes);
        ensureStableCluster(numNodes);

        RateLimitAssignment firstAssignment = null;

        for (String nodeName : nodeNames) {
            var calculator = internalCluster().getInstance(InferenceServiceNodeLocalRateLimitCalculator.class, nodeName);

            // Check first node's assignments
            if (firstAssignment == null) {
                // Get assignment for a specific service (e.g., EIS)
                firstAssignment = calculator.getRateLimitAssignment(ElasticInferenceService.NAME, TaskType.SPARSE_EMBEDDING);

                assertNotNull(firstAssignment);
                // Verify there are assignments for this service
                assertFalse(firstAssignment.responsibleNodes().isEmpty());
            } else {
                // Verify other nodes see the same assignment
                var currentAssignment = calculator.getRateLimitAssignment(ElasticInferenceService.NAME, TaskType.SPARSE_EMBEDDING);
                assertEquals(firstAssignment, currentAssignment);
            }
        }
    }

    public void testNumberOfNodesPerGroup_Decreases_When_NodeLeavesCluster() throws IOException {
        // Start with 3-5 nodes
        var numNodes = randomIntBetween(3, 5);
        var nodeNames = internalCluster().startNodes(numNodes);
        ensureStableCluster(numNodes);

        var nodeLeftInCluster = nodeNames.getFirst();
        var currentNumberOfNodes = numNodes;

        // Stop all nodes except one
        for (String nodeName : nodeNames) {
            if (nodeName.equals(nodeLeftInCluster)) {
                continue;
            }
            internalCluster().stopNode(nodeName);
            currentNumberOfNodes--;
            ensureStableCluster(currentNumberOfNodes);
        }

        var calculator = internalCluster().getInstance(InferenceServiceNodeLocalRateLimitCalculator.class, nodeLeftInCluster);

        Set<String> supportedServices = SERVICE_NODE_LOCAL_RATE_LIMIT_CONFIGS.keySet();

        // Check assignments for each supported service
        for (var service : supportedServices) {
            var assignment = calculator.getRateLimitAssignment(service, TaskType.SPARSE_EMBEDDING);

            assertNotNull(assignment);
            // Should have exactly one responsible node
            assertEquals(1, assignment.responsibleNodes().size());
            // That node should be our remaining node
            assertEquals(nodeLeftInCluster, assignment.responsibleNodes().get(0).getName());
        }
    }

    public void testGrouping_RespectsMaxNodesPerGroupingLimit() {
        // Start with more nodes possible per grouping
        var numNodes = DEFAULT_MAX_NODES_PER_GROUPING + randomIntBetween(1, 3);
        var nodeNames = internalCluster().startNodes(numNodes);
        ensureStableCluster(numNodes);

        var calculator = internalCluster().getInstance(InferenceServiceNodeLocalRateLimitCalculator.class, nodeNames.getFirst());

        Set<String> supportedServices = SERVICE_NODE_LOCAL_RATE_LIMIT_CONFIGS.keySet();

        for (var service : supportedServices) {
            var assignment = calculator.getRateLimitAssignment(service, TaskType.SPARSE_EMBEDDING);

            assertNotNull(assignment);
            assertThat(DEFAULT_MAX_NODES_PER_GROUPING, equalTo(assignment.responsibleNodes().size()));
        }
    }

    public void testInitialRateLimitsCalculation_Correct() throws IOException {
        // Start with max nodes per grouping (=3)
        int numNodes = DEFAULT_MAX_NODES_PER_GROUPING;
        var nodeNames = internalCluster().startNodes(numNodes);
        ensureStableCluster(numNodes);

        var calculator = internalCluster().getInstance(InferenceServiceNodeLocalRateLimitCalculator.class, nodeNames.getFirst());

        Set<String> supportedServices = SERVICE_NODE_LOCAL_RATE_LIMIT_CONFIGS.keySet();

        for (var serviceName : supportedServices) {
            try (var serviceRegistry = calculator.serviceRegistry()) {
                var serviceOptional = serviceRegistry.getService(serviceName);
                assertTrue(serviceOptional.isPresent());
                var service = serviceOptional.get();

                if ((service instanceof SenderService senderService)) {
                    var sender = senderService.getSender();
                    if (sender instanceof HttpRequestSender httpSender) {
                        var assignment = calculator.getRateLimitAssignment(service.name(), TaskType.SPARSE_EMBEDDING);

                        assertNotNull(assignment);
                        assertThat(DEFAULT_MAX_NODES_PER_GROUPING, equalTo(assignment.responsibleNodes().size()));
                    }
                }
            }

        }
    }

    public void testRateLimits_Decrease_OnNodeJoin() {
        // Start with 2 nodes
        var initialNodes = 2;
        var nodeNames = internalCluster().startNodes(initialNodes);
        ensureStableCluster(initialNodes);

        var calculator = internalCluster().getInstance(InferenceServiceNodeLocalRateLimitCalculator.class, nodeNames.getFirst());

        for (var serviceName : SERVICE_NODE_LOCAL_RATE_LIMIT_CONFIGS.keySet()) {
            var configs = SERVICE_NODE_LOCAL_RATE_LIMIT_CONFIGS.get(serviceName);
            for (var config : configs) {
                // Get initial assignments and rate limits
                var initialAssignment = calculator.getRateLimitAssignment(serviceName, config.taskType());
                assertEquals(2, initialAssignment.responsibleNodes().size());

                // Add a new node
                internalCluster().startNode();
                ensureStableCluster(initialNodes + 1);

                // Get updated assignments
                var updatedAssignment = calculator.getRateLimitAssignment(serviceName, config.taskType());

                // Verify number of responsible nodes increased
                assertEquals(3, updatedAssignment.responsibleNodes().size());
            }
        }
    }

    public void testRateLimits_Increase_OnNodeLeave() throws IOException {
        // Start with max nodes per grouping (=3)
        int numNodes = DEFAULT_MAX_NODES_PER_GROUPING;
        var nodeNames = internalCluster().startNodes(numNodes);
        ensureStableCluster(numNodes);

        var calculator = internalCluster().getInstance(InferenceServiceNodeLocalRateLimitCalculator.class, nodeNames.getFirst());

        for (var serviceName : SERVICE_NODE_LOCAL_RATE_LIMIT_CONFIGS.keySet()) {
            var configs = SERVICE_NODE_LOCAL_RATE_LIMIT_CONFIGS.get(serviceName);
            for (var config : configs) {
                // Get initial assignments and rate limits
                var initialAssignment = calculator.getRateLimitAssignment(serviceName, config.taskType());
                assertThat(DEFAULT_MAX_NODES_PER_GROUPING, equalTo(initialAssignment.responsibleNodes().size()));

                // Remove a node
                var nodeToRemove = nodeNames.get(numNodes - 1);
                internalCluster().stopNode(nodeToRemove);
                ensureStableCluster(numNodes - 1);

                // Get updated assignments
                var updatedAssignment = calculator.getRateLimitAssignment(serviceName, config.taskType());

                // Verify number of responsible nodes decreased
                assertThat(2, equalTo(updatedAssignment.responsibleNodes().size()));
            }
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateInferencePlugin.class);
    }
}
