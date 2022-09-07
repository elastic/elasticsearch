/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.node.DiskHealthInfo;
import org.elasticsearch.health.node.HealthInfoCache;
import org.elasticsearch.health.node.LocalHealthMonitor;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
            assertBusy(() -> {
                Map<String, DiskHealthInfo> healthInfoCache = internalCluster.getInstance(HealthInfoCache.class, healthNode.getName())
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
            assertBusy(() -> {
                Map<String, DiskHealthInfo> healthInfoCache = internalCluster.getInstance(HealthInfoCache.class, healthNode.getName())
                    .getDiskHealthInfo();
                assertThat(healthInfoCache.size(), equalTo(nodes.size() - 1));
                for (DiscoveryNode node : nodes) {
                    if (node.getId().equals(nodeToLeave.getId())) {
                        assertThat(healthInfoCache.containsKey(node.getId()), equalTo(false));
                    } else {
                        assertThat(healthInfoCache.get(node.getId()), equalTo(GREEN));
                    }
                }
            });
        } catch (IOException e) {
            throw new RuntimeException("Failed to close internal cluster: " + e.getMessage(), e);
        }
    }

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

    @Nullable
    private DiscoveryNode waitAndGetHealthNode(InternalTestCluster internalCluster) throws InterruptedException {
        DiscoveryNode[] healthNode = new DiscoveryNode[1];
        waitUntil(() -> {
            ClusterState state = internalCluster.client()
                .admin()
                .cluster()
                .prepareState()
                .clear()
                .setMetadata(true)
                .setNodes(true)
                .get()
                .getState();
            healthNode[0] = HealthNode.findHealthNode(state);
            return healthNode[0] != null;
        }, 2, TimeUnit.SECONDS);
        return healthNode[0];
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
