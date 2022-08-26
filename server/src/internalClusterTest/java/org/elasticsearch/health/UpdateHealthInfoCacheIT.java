/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.health.node.DiskHealthInfo;
import org.elasticsearch.health.node.HealthInfoCache;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 4)
public class UpdateHealthInfoCacheIT extends ESIntegTestCase {

    private static final DiskHealthInfo GREEN = new DiskHealthInfo(HealthStatus.GREEN, null);

    public void testHappyFlow() throws Exception {
        try (InternalTestCluster internalCluster = internalCluster()) {
            ClusterState state = internalCluster.client().admin().cluster().prepareState().clear().setNodes(true).get().getState();
            String[] nodeIds = state.getNodes().getNodes().keySet().toArray(new String[0]);
            DiscoveryNode healthNode = waitAndGetHealthNode(internalCluster);
            DiskHealthInfo green = new DiskHealthInfo(HealthStatus.GREEN, null);
            assertBusy(() -> {
                Map<String, DiskHealthInfo> healthInfoCache = internalCluster.getInstance(HealthInfoCache.class, healthNode.getName())
                    .getDiskHealthInfo();
                assertThat(healthInfoCache.size(), equalTo(nodeIds.length));
                for (String nodeId : nodeIds) {
                    assertThat(healthInfoCache.get(nodeId), equalTo(green));
                }
            });
        } catch (IOException e) {
            throw new RuntimeException("Failed to close internal cluster: " + e.getMessage(), e);
        }
    }

    public void testHealthNodeFailOver() throws Exception {
        try (InternalTestCluster internalCluster = internalCluster()) {
            ClusterState state = internalCluster.client().admin().cluster().prepareState().clear().setNodes(true).get().getState();
            String[] nodeIds = state.getNodes().getNodes().keySet().toArray(new String[0]);
            DiscoveryNode healthNodeToBeShutDown = waitAndGetHealthNode(internalCluster);
            internalCluster.restartNode(healthNodeToBeShutDown.getName());
            DiscoveryNode newHealthNode = waitAndGetHealthNode(internalCluster);
            assertBusy(() -> {
                Map<String, DiskHealthInfo> healthInfoCache = internalCluster.getInstance(HealthInfoCache.class, newHealthNode.getName())
                    .getDiskHealthInfo();
                assertThat(healthInfoCache.size(), equalTo(nodeIds.length));
                for (String nodeId : nodeIds) {
                    assertThat(healthInfoCache.get(nodeId), equalTo(GREEN));
                }
            });
        } catch (IOException e) {
            throw new RuntimeException("Failed to close internal cluster: " + e.getMessage(), e);
        }
    }

    private static DiscoveryNode waitAndGetHealthNode(InternalTestCluster internalCluster) throws InterruptedException {
        DiscoveryNode[] healthNode = new DiscoveryNode[1];
        waitUntil(() -> {
            try (Client client = internalCluster.client()) {
                ClusterState state = client.admin().cluster().prepareState().clear().setMetadata(true).setNodes(true).get().getState();
                PersistentTasksCustomMetadata taskMetadata = state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
                PersistentTasksCustomMetadata.PersistentTask<?> task = taskMetadata.getTask("health-node");
                healthNode[0] = task != null && task.isAssigned()
                    ? state.nodes().getDataNodes().get(task.getAssignment().getExecutorNode())
                    : null;
            } catch (Exception e) {
                throw new RuntimeException("Can't get the health node " + e.getMessage(), e);
            }
            return healthNode[0] != null;
        }, 2, TimeUnit.SECONDS);
        assertThat(healthNode[0], notNullValue());
        return healthNode[0];
    }
}
