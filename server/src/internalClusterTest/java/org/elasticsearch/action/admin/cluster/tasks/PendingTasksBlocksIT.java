/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.tasks;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Arrays;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE;

public class PendingTasksBlocksIT extends ESIntegTestCase {

    public void testPendingTasksWithIndexBlocks() {
        createIndex("test");
        ensureGreen("test");

        // This test checks that the Pending Cluster Tasks operation is never blocked, even if an index is read only or whatever.
        for (String blockSetting : Arrays.asList(
            SETTING_BLOCKS_READ,
            SETTING_BLOCKS_WRITE,
            SETTING_READ_ONLY,
            SETTING_BLOCKS_METADATA,
            SETTING_READ_ONLY_ALLOW_DELETE
        )) {
            try {
                enableIndexBlock("test", blockSetting);
                PendingClusterTasksResponse response = client().admin().cluster().preparePendingClusterTasks().get();
                assertNotNull(response.getPendingTasks());
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }
    }

    public void testPendingTasksWithClusterReadOnlyBlock() {
        if (randomBoolean()) {
            createIndex("test");
            ensureGreen("test");
        }

        try {
            setClusterReadOnly(true);
            PendingClusterTasksResponse response = client().admin().cluster().preparePendingClusterTasks().get();
            assertNotNull(response.getPendingTasks());
        } finally {
            setClusterReadOnly(false);
        }
    }

    public void testPendingTasksWithClusterNotRecoveredBlock() throws Exception {
        if (randomBoolean()) {
            createIndex("test");
            ensureGreen("test");
        }

        // restart the cluster but prevent it from performing state recovery
        final int nodeCount = client().admin().cluster().prepareNodesInfo("data:true").get().getNodes().size();
        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                return Settings.builder().put(GatewayService.RECOVER_AFTER_DATA_NODES_SETTING.getKey(), nodeCount + 1).build();
            }

            @Override
            public boolean validateClusterForming() {
                return false;
            }
        });

        assertNotNull(client().admin().cluster().preparePendingClusterTasks().get().getPendingTasks());

        // starting one more node allows the cluster to recover
        internalCluster().startNode();
        ensureGreen();
    }

}
