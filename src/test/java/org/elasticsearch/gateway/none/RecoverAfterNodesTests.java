/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gateway.none;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

/**
 *
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class RecoverAfterNodesTests extends ElasticsearchIntegrationTest {

    private final static TimeValue BLOCK_WAIT_TIMEOUT = TimeValue.timeValueSeconds(1);

    public ImmutableSet<ClusterBlock> waitForNoBlocksOnNode(TimeValue timeout, Client nodeClient) throws InterruptedException {
        long start = System.currentTimeMillis();
        ImmutableSet<ClusterBlock> blocks;
        do {
            blocks = nodeClient.admin().cluster().prepareState().setLocal(true).execute().actionGet()
                    .getState().blocks().global(ClusterBlockLevel.METADATA);
        }
        while (!blocks.isEmpty() && (System.currentTimeMillis() - start) < timeout.millis());
        return blocks;
    }

    public Client startNode(Settings.Builder settings) {
        String name = internalCluster().startNode(settings);
        return internalCluster().client(name);
    }

    @Test
    public void testRecoverAfterNodes() throws Exception {
        logger.info("--> start node (1)");
        Client clientNode1 = startNode(settingsBuilder().put("gateway.recover_after_nodes", 3));
        assertThat(clientNode1.admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));

        logger.info("--> start node (2)");
        Client clientNode2 = startNode(settingsBuilder().put("gateway.recover_after_nodes", 3));
        Thread.sleep(BLOCK_WAIT_TIMEOUT.millis());
        assertThat(clientNode1.admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));
        assertThat(clientNode2.admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));

        logger.info("--> start node (3)");
        Client clientNode3 = startNode(settingsBuilder().put("gateway.recover_after_nodes", 3));

        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, clientNode1).isEmpty(), equalTo(true));
        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, clientNode2).isEmpty(), equalTo(true));
        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, clientNode3).isEmpty(), equalTo(true));
    }

    @Test
    public void testRecoverAfterMasterNodes() throws Exception {
        logger.info("--> start master_node (1)");
        Client master1 = startNode(settingsBuilder().put("gateway.recover_after_master_nodes", 2).put("node.data", false).put("node.master", true));
        assertThat(master1.admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));

        logger.info("--> start data_node (1)");
        Client data1 = startNode(settingsBuilder().put("gateway.recover_after_master_nodes", 2).put("node.data", true).put("node.master", false));
        assertThat(master1.admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));
        assertThat(data1.admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));

        logger.info("--> start data_node (2)");
        Client data2 = startNode(settingsBuilder().put("gateway.recover_after_master_nodes", 2).put("node.data", true).put("node.master", false));
        assertThat(master1.admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));
        assertThat(data1.admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));
        assertThat(data2.admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));

        logger.info("--> start master_node (2)");
        Client master2 = startNode(settingsBuilder().put("gateway.recover_after_master_nodes", 2).put("node.data", false).put("node.master", true));
        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, master1).isEmpty(), equalTo(true));
        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, master2).isEmpty(), equalTo(true));
        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, data1).isEmpty(), equalTo(true));
        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, data2).isEmpty(), equalTo(true));
    }

    @Test
    public void testRecoverAfterDataNodes() throws Exception {
        logger.info("--> start master_node (1)");
        Client master1 = startNode(settingsBuilder().put("gateway.recover_after_data_nodes", 2).put("node.data", false).put("node.master", true));
        assertThat(master1.admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));

        logger.info("--> start data_node (1)");
        Client data1 = startNode(settingsBuilder().put("gateway.recover_after_data_nodes", 2).put("node.data", true).put("node.master", false));
        assertThat(master1.admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));
        assertThat(data1.admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));

        logger.info("--> start master_node (2)");
        Client master2 = startNode(settingsBuilder().put("gateway.recover_after_data_nodes", 2).put("node.data", false).put("node.master", true));
        assertThat(master2.admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));
        assertThat(data1.admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));
        assertThat(master2.admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));

        logger.info("--> start data_node (2)");
        Client data2 = startNode(settingsBuilder().put("gateway.recover_after_data_nodes", 2).put("node.data", true).put("node.master", false));
        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, master1).isEmpty(), equalTo(true));
        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, master2).isEmpty(), equalTo(true));
        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, data1).isEmpty(), equalTo(true));
        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, data2).isEmpty(), equalTo(true));
    }
}
