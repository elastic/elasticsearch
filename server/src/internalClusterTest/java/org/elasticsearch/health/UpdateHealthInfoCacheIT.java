/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.node.DiskHealthInfo;
import org.elasticsearch.health.node.FetchHealthInfoCacheAction;
import org.elasticsearch.health.node.LocalHealthMonitor;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 4)
public class UpdateHealthInfoCacheIT extends ESIntegTestCase {

    private static final DiskHealthInfo GREEN = new DiskHealthInfo(HealthStatus.GREEN, null);

    public void testNodesReportingHealth() throws Exception {
        try (InternalTestCluster internalCluster = internalCluster()) {
            decreasePollingInterval(internalCluster);
            String[] nodeIds = getNodes(internalCluster).keySet().toArray(new String[0]);
            DiscoveryNode healthNode = waitAndGetHealthNode(internalCluster);
            assertThat(healthNode, notNullValue());
            assertBusy(() -> assertResultsCanBeFetched(internalCluster, healthNode, List.of(nodeIds), null));
        } catch (IOException e) {
            throw new RuntimeException("Failed to close internal cluster: " + e.getMessage(), e);
        }
    }

    public void testNodeLeavingCluster() throws Exception {
        try (InternalTestCluster internalCluster = internalCluster()) {
            decreasePollingInterval(internalCluster);
            Collection<DiscoveryNode> nodes = getNodes(internalCluster).values();
            DiscoveryNode healthNode = waitAndGetHealthNode(internalCluster);
            assertThat(healthNode, notNullValue());
            DiscoveryNode nodeToLeave = nodes.stream().filter(node -> {
                boolean isMaster = node.getName().equals(internalCluster.getMasterName());
                boolean isHealthNode = node.getId().equals(healthNode.getId());
                // We have dedicated tests for master and health node
                return isMaster == false && isHealthNode == false;
            }).findAny().orElseThrow();
            internalCluster.stopNode(nodeToLeave.getName());
            assertBusy(
                () -> assertResultsCanBeFetched(
                    internalCluster,
                    healthNode,
                    nodes.stream().filter(node -> node.equals(nodeToLeave) == false).map(DiscoveryNode::getId).toList(),
                    nodeToLeave.getId()
                )
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to close internal cluster: " + e.getMessage(), e);
        }
    }

    @TestLogging(value = "org.elasticsearch.health.node:DEBUG", reason = "https://github.com/elastic/elasticsearch/issues/97213")
    public void testHealthNodeFailOver() throws Exception {
        try (InternalTestCluster internalCluster = internalCluster()) {
            decreasePollingInterval(internalCluster);
            String[] nodeIds = getNodes(internalCluster).keySet().toArray(new String[0]);
            DiscoveryNode healthNodeToBeShutDown = waitAndGetHealthNode(internalCluster);
            assertThat(healthNodeToBeShutDown, notNullValue());
            internalCluster.restartNode(healthNodeToBeShutDown.getName());
            ensureStableCluster(nodeIds.length);
            DiscoveryNode newHealthNode = waitAndGetHealthNode(internalCluster);
            assertThat(newHealthNode, notNullValue());
            logger.info("Previous health node {}, new health node {}.", healthNodeToBeShutDown, newHealthNode);
            assertBusy(() -> assertResultsCanBeFetched(internalCluster, newHealthNode, List.of(nodeIds), null));
        } catch (IOException e) {
            throw new RuntimeException("Failed to close internal cluster: " + e.getMessage(), e);
        }
    }

    @TestLogging(value = "org.elasticsearch.health.node:DEBUG", reason = "https://github.com/elastic/elasticsearch/issues/97213")
    public void testMasterFailure() throws Exception {
        try (InternalTestCluster internalCluster = internalCluster()) {
            decreasePollingInterval(internalCluster);
            String[] nodeIds = getNodes(internalCluster).keySet().toArray(new String[0]);
            DiscoveryNode healthNodeBeforeIncident = waitAndGetHealthNode(internalCluster);
            assertThat(healthNodeBeforeIncident, notNullValue());
            String masterName = internalCluster.getMasterName();
            logger.info("Restarting elected master node {}.", masterName);
            internalCluster.restartNode(masterName);
            ensureStableCluster(nodeIds.length);
            DiscoveryNode newHealthNode = waitAndGetHealthNode(internalCluster);
            assertThat(newHealthNode, notNullValue());
            assertBusy(() -> assertResultsCanBeFetched(internalCluster, newHealthNode, List.of(nodeIds), null));
        } catch (IOException e) {
            throw new RuntimeException("Failed to close internal cluster: " + e.getMessage(), e);
        }
    }

    /**
     * This method fetches the health data using FetchHealthInfoCacheAction. It does this using both a random non-health-node client and
     * a health node client. It asserts that all expected nodeIds are there with GREEN status, and that the notExpectedNodeId (if not
     * null) is not in the results.
     * @param internalCluster The cluster to use to get clients
     * @param healthNode The health node
     * @param expectedNodeIds The list of nodeIds we expect to see in the results (with a GREEN status)
     * @param notExpectedNodeId A nullable nodeId that we do _not_ expect to see in the results
     * @throws Exception
     */
    private void assertResultsCanBeFetched(
        InternalTestCluster internalCluster,
        DiscoveryNode healthNode,
        Iterable<String> expectedNodeIds,
        @Nullable String notExpectedNodeId
    ) throws Exception {
        Client nonHealthNodeClient = internalCluster.client(randomValueOtherThan(healthNode.getName(), internalCluster::getRandomNodeName));
        FetchHealthInfoCacheAction.Response healthResponse = nonHealthNodeClient.execute(
            FetchHealthInfoCacheAction.INSTANCE,
            new FetchHealthInfoCacheAction.Request()
        ).get();
        for (String nodeId : expectedNodeIds) {
            assertThat("Unexpected data for " + nodeId, healthResponse.getHealthInfo().diskInfoByNode().get(nodeId), equalTo(GREEN));
        }
        if (notExpectedNodeId != null) {
            assertThat(healthResponse.getHealthInfo().diskInfoByNode().containsKey(notExpectedNodeId), equalTo(false));
        }
        Client healthNodeClient = internalCluster.client(healthNode.getName());
        healthResponse = healthNodeClient.execute(FetchHealthInfoCacheAction.INSTANCE, new FetchHealthInfoCacheAction.Request()).get();
        for (String nodeId : expectedNodeIds) {
            assertThat("Unexpected data for " + nodeId, healthResponse.getHealthInfo().diskInfoByNode().get(nodeId), equalTo(GREEN));
        }
    }

    private void decreasePollingInterval(InternalTestCluster internalCluster) {
        internalCluster.client()
            .admin()
            .cluster()
            .updateSettings(
                new ClusterUpdateSettingsRequest().persistentSettings(
                    Settings.builder().put(LocalHealthMonitor.POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(10))
                )
            );
    }

    private static Map<String, DiscoveryNode> getNodes(InternalTestCluster internalCluster) {
        return internalCluster.client().admin().cluster().prepareState().clear().setNodes(true).get().getState().getNodes().getNodes();
    }
}
