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
        for (String blockSetting : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE, SETTING_READ_ONLY, SETTING_BLOCKS_METADATA,
            SETTING_READ_ONLY_ALLOW_DELETE)) {
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
        final int nodeCount = client().admin().cluster().prepareNodesInfo("data:true", "master:true").get().getNodes().size();
        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                return Settings.builder()
                    .put(GatewayService.RECOVER_AFTER_NODES_SETTING.getKey(), nodeCount + 1)
                    .build();
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
