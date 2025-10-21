/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nodesinfo;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class SimpleNodesInfoIT extends ESIntegTestCase {

    public void testNodesInfos() {
        List<String> nodesNames = internalCluster().startNodes(2);
        final String node_1 = nodesNames.get(0);
        final String node_2 = nodesNames.get(1);

        ClusterHealthResponse clusterHealth = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
            .setWaitForGreenStatus()
            .setWaitForNodes("2")
            .get();
        logger.info("--> done cluster_health, status {}", clusterHealth.getStatus());

        String server1NodeId = getNodeId(node_1);
        String server2NodeId = getNodeId(node_2);
        logger.info("--> started nodes: {} and {}", server1NodeId, server2NodeId);

        NodesInfoResponse response = clusterAdmin().prepareNodesInfo().get();
        assertThat(response.getNodes(), hasSize(2));
        assertThat(response.getNodesMap().get(server1NodeId), notNullValue());
        assertThat(response.getNodesMap().get(server2NodeId), notNullValue());

        response = clusterAdmin().nodesInfo(new NodesInfoRequest()).actionGet();
        assertThat(response.getNodes(), hasSize(2));
        assertThat(response.getNodesMap().get(server1NodeId), notNullValue());
        assertThat(response.getNodesMap().get(server2NodeId), notNullValue());

        response = clusterAdmin().nodesInfo(new NodesInfoRequest(server1NodeId)).actionGet();
        assertThat(response.getNodes(), hasSize(1));
        assertThat(response.getNodesMap().get(server1NodeId), notNullValue());

        response = clusterAdmin().nodesInfo(new NodesInfoRequest(server1NodeId)).actionGet();
        assertThat(response.getNodes(), hasSize(1));
        assertThat(response.getNodesMap().get(server1NodeId), notNullValue());

        response = clusterAdmin().nodesInfo(new NodesInfoRequest(server2NodeId)).actionGet();
        assertThat(response.getNodes(), hasSize(1));
        assertThat(response.getNodesMap().get(server2NodeId), notNullValue());

        response = clusterAdmin().nodesInfo(new NodesInfoRequest(server2NodeId)).actionGet();
        assertThat(response.getNodes(), hasSize(1));
        assertThat(response.getNodesMap().get(server2NodeId), notNullValue());
    }

    public void testNodesInfosTotalIndexingBuffer() {
        List<String> nodesNames = internalCluster().startNodes(2);
        final String node_1 = nodesNames.get(0);
        final String node_2 = nodesNames.get(1);

        ClusterHealthResponse clusterHealth = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
            .setWaitForGreenStatus()
            .setWaitForNodes("2")
            .get();
        logger.info("--> done cluster_health, status {}", clusterHealth.getStatus());

        String server1NodeId = getNodeId(node_1);
        String server2NodeId = getNodeId(node_2);
        logger.info("--> started nodes: {} and {}", server1NodeId, server2NodeId);

        NodesInfoResponse response = clusterAdmin().prepareNodesInfo().get();
        assertThat(response.getNodes(), hasSize(2));
        assertThat(response.getNodesMap().get(server1NodeId), notNullValue());
        assertNotNull(response.getNodesMap().get(server1NodeId).getTotalIndexingBuffer());
        assertThat(response.getNodesMap().get(server1NodeId).getTotalIndexingBuffer().getBytes(), greaterThan(0L));

        assertThat(response.getNodesMap().get(server2NodeId), notNullValue());
        assertNotNull(response.getNodesMap().get(server2NodeId).getTotalIndexingBuffer());
        assertThat(response.getNodesMap().get(server2NodeId).getTotalIndexingBuffer().getBytes(), greaterThan(0L));

        // again, using only the indices flag
        response = clusterAdmin().prepareNodesInfo().clear().setIndices(true).get();
        assertThat(response.getNodes(), hasSize(2));
        assertThat(response.getNodesMap().get(server1NodeId), notNullValue());
        assertNotNull(response.getNodesMap().get(server1NodeId).getTotalIndexingBuffer());
        assertThat(response.getNodesMap().get(server1NodeId).getTotalIndexingBuffer().getBytes(), greaterThan(0L));

        assertThat(response.getNodesMap().get(server2NodeId), notNullValue());
        assertNotNull(response.getNodesMap().get(server2NodeId).getTotalIndexingBuffer());
        assertThat(response.getNodesMap().get(server2NodeId).getTotalIndexingBuffer().getBytes(), greaterThan(0L));
    }

    public void testAllocatedProcessors() throws Exception {
        List<String> nodeNames = internalCluster().startNodes(
            Settings.builder().put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), 2.9).build(),
            Settings.builder().put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), 5.9).build()
        );

        final String node_1 = nodeNames.get(0);
        final String node_2 = nodeNames.get(1);

        ClusterHealthResponse clusterHealth = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
            .setWaitForGreenStatus()
            .setWaitForNodes("2")
            .get();
        logger.info("--> done cluster_health, status {}", clusterHealth.getStatus());

        String server1NodeId = getNodeId(node_1);
        String server2NodeId = getNodeId(node_2);
        logger.info("--> started nodes: {} and {}", server1NodeId, server2NodeId);

        NodesInfoResponse response = clusterAdmin().prepareNodesInfo().get();

        assertThat(response.getNodes(), hasSize(2));
        assertThat(response.getNodesMap().get(server1NodeId), notNullValue());
        assertThat(response.getNodesMap().get(server2NodeId), notNullValue());

        assertThat(
            response.getNodesMap().get(server1NodeId).getInfo(OsInfo.class).getAvailableProcessors(),
            equalTo(Runtime.getRuntime().availableProcessors())
        );
        assertThat(
            response.getNodesMap().get(server2NodeId).getInfo(OsInfo.class).getAvailableProcessors(),
            equalTo(Runtime.getRuntime().availableProcessors())
        );

        assertThat(response.getNodesMap().get(server1NodeId).getInfo(OsInfo.class).getAllocatedProcessors(), equalTo(3));
        assertThat(response.getNodesMap().get(server1NodeId).getInfo(OsInfo.class).getFractionalAllocatedProcessors(), equalTo(2.9));
        assertThat(response.getNodesMap().get(server2NodeId).getInfo(OsInfo.class).getAllocatedProcessors(), equalTo(6));
        assertThat(response.getNodesMap().get(server2NodeId).getInfo(OsInfo.class).getFractionalAllocatedProcessors(), equalTo(5.9));
    }
}
