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
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.common.InferenceServiceNodeLocalRateLimitCalculator.DEFAULT_MAX_NODES_PER_GROUPING;
import static org.elasticsearch.xpack.inference.common.InferenceServiceNodeLocalRateLimitCalculator.SERVICE_NODE_LOCAL_RATE_LIMIT_CONFIGS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class InferenceServiceNodeLocalRateLimitCalculatorTests extends ESIntegTestCase {

    private static final Integer RATE_LIMIT_ASSIGNMENT_MAX_WAIT_TIME_IN_SECONDS = 15;

    public void setUp() throws Exception {
        super.setUp();
        assumeTrue(
            "If inference_cluster_aware_rate_limiting_feature_flag_enabled=false we'll fallback to "
                + "NoopNodeLocalRateLimitCalculator, which shouldn't be tested by this class.",
            InferenceAPIClusterAwareRateLimitingFeature.INFERENCE_API_CLUSTER_AWARE_RATE_LIMITING_FEATURE_FLAG
        );
    }

    public void testInitialClusterGrouping_Correct() throws Exception {
        // Start with 2-5 nodes
        var numNodes = randomIntBetween(2, 5);
        var nodeNames = internalCluster().startNodes(numNodes);
        ensureStableCluster(numNodes);

        var firstCalculator = getCalculatorInstance(internalCluster(), nodeNames.getFirst());
        waitForRateLimitingAssignments(firstCalculator);

        RateLimitAssignment firstAssignment = firstCalculator.getRateLimitAssignment(
            ElasticInferenceService.NAME,
            TaskType.SPARSE_EMBEDDING
        );

        // Verify that all other nodes land on the same assignment
        for (String nodeName : nodeNames.subList(1, nodeNames.size())) {
            var calculator = getCalculatorInstance(internalCluster(), nodeName);
            waitForRateLimitingAssignments(calculator);
            var currentAssignment = calculator.getRateLimitAssignment(ElasticInferenceService.NAME, TaskType.SPARSE_EMBEDDING);
            assertEquals(firstAssignment, currentAssignment);
        }
    }

    public void testNumberOfNodesPerGroup_Decreases_When_NodeLeavesCluster() throws Exception {
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

        var calculator = getCalculatorInstance(internalCluster(), nodeLeftInCluster);
        waitForRateLimitingAssignments(calculator);

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

    public void testGrouping_RespectsMaxNodesPerGroupingLimit() throws Exception {
        // Start with more nodes possible per grouping
        var numNodes = DEFAULT_MAX_NODES_PER_GROUPING + randomIntBetween(1, 3);
        var nodeNames = internalCluster().startNodes(numNodes);
        ensureStableCluster(numNodes);

        var calculator = getCalculatorInstance(internalCluster(), nodeNames.getFirst());
        waitForRateLimitingAssignments(calculator);

        Set<String> supportedServices = SERVICE_NODE_LOCAL_RATE_LIMIT_CONFIGS.keySet();

        for (var service : supportedServices) {
            var assignment = calculator.getRateLimitAssignment(service, TaskType.SPARSE_EMBEDDING);

            assertNotNull(assignment);
            assertThat(DEFAULT_MAX_NODES_PER_GROUPING, equalTo(assignment.responsibleNodes().size()));
        }
    }

    public void testInitialRateLimitsCalculation_Correct() throws Exception {
        // Start with max nodes per grouping (=3)
        int numNodes = DEFAULT_MAX_NODES_PER_GROUPING;
        var nodeNames = internalCluster().startNodes(numNodes);
        ensureStableCluster(numNodes);

        var calculator = getCalculatorInstance(internalCluster(), nodeNames.getFirst());
        waitForRateLimitingAssignments(calculator);

        Set<String> supportedServices = SERVICE_NODE_LOCAL_RATE_LIMIT_CONFIGS.keySet();

        for (var serviceName : supportedServices) {
            try (var serviceRegistry = calculator.serviceRegistry()) {
                var serviceOptional = serviceRegistry.getService(serviceName);
                assertTrue(serviceOptional.isPresent());
                var service = serviceOptional.get();

                if ((service instanceof SenderService senderService)) {
                    var sender = senderService.getSender();
                    if (sender instanceof HttpRequestSender) {
                        var assignment = calculator.getRateLimitAssignment(service.name(), TaskType.SPARSE_EMBEDDING);

                        assertNotNull(assignment);
                        assertThat(DEFAULT_MAX_NODES_PER_GROUPING, equalTo(assignment.responsibleNodes().size()));
                    }
                }
            }

        }
    }

    public void testRateLimits_Decrease_OnNodeJoin() throws Exception {
        // Start with 2 nodes
        var initialNodes = 2;
        var nodeNames = internalCluster().startNodes(initialNodes);
        ensureStableCluster(initialNodes);

        var calculator = getCalculatorInstance(internalCluster(), nodeNames.getFirst());
        waitForRateLimitingAssignments(calculator);

        for (var serviceName : SERVICE_NODE_LOCAL_RATE_LIMIT_CONFIGS.keySet()) {
            var configs = SERVICE_NODE_LOCAL_RATE_LIMIT_CONFIGS.get(serviceName);
            for (var config : configs) {
                // Get initial assignments and rate limits
                var initialAssignment = calculator.getRateLimitAssignment(serviceName, config.taskType());
                assertEquals(2, initialAssignment.responsibleNodes().size());

                // Add a new node
                internalCluster().startNode();
                ensureStableCluster(initialNodes + 1);
                waitForRateLimitingAssignments(calculator);

                // Get updated assignments
                var updatedAssignment = calculator.getRateLimitAssignment(serviceName, config.taskType());

                // Verify number of responsible nodes increased
                assertEquals(3, updatedAssignment.responsibleNodes().size());
            }
        }
    }

    public void testRateLimits_Increase_OnNodeLeave() throws Exception {
        // Start with max nodes per grouping (=3)
        int numNodes = DEFAULT_MAX_NODES_PER_GROUPING;
        var nodeNames = internalCluster().startNodes(numNodes);
        ensureStableCluster(numNodes);

        var calculator = getCalculatorInstance(internalCluster(), nodeNames.getFirst());
        waitForRateLimitingAssignments(calculator);

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
                waitForRateLimitingAssignments(calculator);

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

    private InferenceServiceNodeLocalRateLimitCalculator getCalculatorInstance(InternalTestCluster internalTestCluster, String nodeName) {
        InferenceServiceRateLimitCalculator calculatorInstance = internalTestCluster.getInstance(
            InferenceServiceRateLimitCalculator.class,
            nodeName
        );
        assertThat(
            "["
                + InferenceServiceNodeLocalRateLimitCalculatorTests.class.getName()
                + "] should use ["
                + InferenceServiceNodeLocalRateLimitCalculator.class.getName()
                + "] as implementation for ["
                + InferenceServiceRateLimitCalculator.class.getName()
                + "]. Provided implementation was ["
                + calculatorInstance.getClass().getName()
                + "].",
            calculatorInstance,
            instanceOf(InferenceServiceNodeLocalRateLimitCalculator.class)
        );
        return (InferenceServiceNodeLocalRateLimitCalculator) calculatorInstance;
    }

    private void waitForRateLimitingAssignments(InferenceServiceNodeLocalRateLimitCalculator calculator) throws Exception {
        assertBusy(() -> {
            var assignment = calculator.getRateLimitAssignment(ElasticInferenceService.NAME, TaskType.SPARSE_EMBEDDING);
            assertNotNull(assignment);
            assertFalse(assignment.responsibleNodes().isEmpty());
        }, RATE_LIMIT_ASSIGNMENT_MAX_WAIT_TIME_IN_SECONDS, TimeUnit.SECONDS);
    }
}
