/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.integration.gateway.none;

import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.junit.After;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

/**
 *
 */
public class RecoverAfterNodesTests extends AbstractNodesTests {

    private final static TimeValue BLOCK_WAIT_TIMEOUT = TimeValue.timeValueSeconds(1);

    @After
    public void closeNodes() {
        closeAllNodes();
    }

    @Test
    public void testRecoverAfterNodes() throws Exception {
        logger.info("--> start node (1)");
        startNode("node1", settingsBuilder().put("gateway.recover_after_nodes", 3));
        assertThat(client("node1").admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));

        logger.info("--> start node (2)");
        startNode("node2", settingsBuilder().put("gateway.recover_after_nodes", 3));
        // Sleeping here for the same time that we wait to check for empty blocks
        Thread.sleep(BLOCK_WAIT_TIMEOUT.millis());
        assertThat(client("node1").admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));
        assertThat(client("node2").admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));

        logger.info("--> start node (3)");
        startNode("node3", settingsBuilder().put("gateway.recover_after_nodes", 3));

        assertThat(waitForNoBlocks(BLOCK_WAIT_TIMEOUT, "node1").isEmpty(), equalTo(true));
        assertThat(waitForNoBlocks(BLOCK_WAIT_TIMEOUT, "node2").isEmpty(), equalTo(true));
        assertThat(waitForNoBlocks(BLOCK_WAIT_TIMEOUT, "node3").isEmpty(), equalTo(true));
    }

    @Test
    public void testRecoverAfterMasterNodes() throws Exception {
        logger.info("--> start master_node (1)");
        startNode("master1", settingsBuilder().put("gateway.recover_after_master_nodes", 2).put("node.data", false).put("node.master", true));
        assertThat(client("master1").admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));

        logger.info("--> start data_node (1)");
        startNode("data1", settingsBuilder().put("gateway.recover_after_master_nodes", 2).put("node.data", true).put("node.master", false));
        assertThat(client("master1").admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));
        assertThat(client("data1").admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));

        logger.info("--> start data_node (2)");
        startNode("data2", settingsBuilder().put("gateway.recover_after_master_nodes", 2).put("node.data", true).put("node.master", false));
        assertThat(client("master1").admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));
        assertThat(client("data1").admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));
        assertThat(client("data2").admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));

        logger.info("--> start master_node (2)");
        startNode("master2", settingsBuilder().put("gateway.recover_after_master_nodes", 2).put("node.data", false).put("node.master", true));
        assertThat(waitForNoBlocks(BLOCK_WAIT_TIMEOUT, "master1").isEmpty(), equalTo(true));
        assertThat(waitForNoBlocks(BLOCK_WAIT_TIMEOUT, "master2").isEmpty(), equalTo(true));
        assertThat(waitForNoBlocks(BLOCK_WAIT_TIMEOUT, "data1").isEmpty(), equalTo(true));
        assertThat(waitForNoBlocks(BLOCK_WAIT_TIMEOUT, "data2").isEmpty(), equalTo(true));
    }

    @Test
    public void testRecoverAfterDataNodes() throws Exception {
        logger.info("--> start master_node (1)");
        startNode("master1", settingsBuilder().put("gateway.recover_after_data_nodes", 2).put("node.data", false).put("node.master", true));
        assertThat(client("master1").admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));

        logger.info("--> start data_node (1)");
        startNode("data1", settingsBuilder().put("gateway.recover_after_data_nodes", 2).put("node.data", true).put("node.master", false));
        assertThat(client("master1").admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));
        assertThat(client("data1").admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));

        logger.info("--> start master_node (2)");
        startNode("master2", settingsBuilder().put("gateway.recover_after_data_nodes", 2).put("node.data", false).put("node.master", true));
        assertThat(client("master1").admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));
        assertThat(client("data1").admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));
        assertThat(client("master2").admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));

        logger.info("--> start data_node (2)");
        startNode("data2", settingsBuilder().put("gateway.recover_after_data_nodes", 2).put("node.data", true).put("node.master", false));
        assertThat(waitForNoBlocks(BLOCK_WAIT_TIMEOUT, "master1").isEmpty(), equalTo(true));
        assertThat(waitForNoBlocks(BLOCK_WAIT_TIMEOUT, "master2").isEmpty(), equalTo(true));
        assertThat(waitForNoBlocks(BLOCK_WAIT_TIMEOUT, "data1").isEmpty(), equalTo(true));
        assertThat(waitForNoBlocks(BLOCK_WAIT_TIMEOUT, "data2").isEmpty(), equalTo(true));
    }
}
