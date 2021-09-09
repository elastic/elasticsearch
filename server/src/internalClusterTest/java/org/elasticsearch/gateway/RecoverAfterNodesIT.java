/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.util.Set;

import static org.elasticsearch.gateway.GatewayService.RECOVER_AFTER_DATA_NODES_SETTING;
import static org.elasticsearch.test.NodeRoles.dataOnlyNode;
import static org.elasticsearch.test.NodeRoles.masterOnlyNode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class RecoverAfterNodesIT extends ESIntegTestCase {
    private static final TimeValue BLOCK_WAIT_TIMEOUT = TimeValue.timeValueSeconds(10);


    public Set<ClusterBlock> waitForNoBlocksOnNode(TimeValue timeout, Client nodeClient) {
        long start = System.currentTimeMillis();
        Set<ClusterBlock> blocks;
        do {
            blocks = nodeClient.admin().cluster().prepareState().setLocal(true).execute().actionGet()
                    .getState().blocks().global(ClusterBlockLevel.METADATA_WRITE);
        } while (blocks.isEmpty() == false && (System.currentTimeMillis() - start) < timeout.millis());
        return blocks;
    }

    public Client startNode(Settings.Builder settings) {
        String name = internalCluster().startNode(Settings.builder().put(settings.build()));
        return internalCluster().client(name);
    }

    public void testRecoverAfterDataNodes() {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start master_node (1)");
        Client master1 = startNode(Settings.builder().put(RECOVER_AFTER_DATA_NODES_SETTING.getKey(), 2).put(masterOnlyNode()));
        assertThat(master1.admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA_WRITE),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));

        logger.info("--> start data_node (1)");
        Client data1 = startNode(Settings.builder().put(RECOVER_AFTER_DATA_NODES_SETTING.getKey(), 2).put(dataOnlyNode()));
        assertThat(master1.admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA_WRITE),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));
        assertThat(data1.admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA_WRITE),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));

        logger.info("--> start master_node (2)");
        Client master2 = startNode(Settings.builder().put(RECOVER_AFTER_DATA_NODES_SETTING.getKey(), 2).put(masterOnlyNode()));
        assertThat(master2.admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA_WRITE),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));
        assertThat(data1.admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA_WRITE),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));
        assertThat(master2.admin().cluster().prepareState().setLocal(true).execute().actionGet()
                .getState().blocks().global(ClusterBlockLevel.METADATA_WRITE),
                hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK));

        logger.info("--> start data_node (2)");
        Client data2 = startNode(Settings.builder().put(RECOVER_AFTER_DATA_NODES_SETTING.getKey(), 2).put(dataOnlyNode()));
        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, master1).isEmpty(), equalTo(true));
        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, master2).isEmpty(), equalTo(true));
        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, data1).isEmpty(), equalTo(true));
        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, data2).isEmpty(), equalTo(true));
    }
}
