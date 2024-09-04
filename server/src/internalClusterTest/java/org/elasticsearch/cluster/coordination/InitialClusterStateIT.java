/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.action.admin.cluster.stats.TransportClusterStatsAction;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import static org.elasticsearch.node.Node.INITIAL_STATE_TIMEOUT_SETTING;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, autoManageMasterNodes = false)
public class InitialClusterStateIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(INITIAL_STATE_TIMEOUT_SETTING.getKey(), TimeValue.ZERO)
            .build();
    }

    private static void assertClusterUuid(boolean expectCommitted, String expectedValue) {
        for (String nodeName : internalCluster().getNodeNames()) {
            final Metadata metadata = client(nodeName).admin().cluster().prepareState().setLocal(true).get().getState().metadata();
            assertEquals(expectCommitted, metadata.clusterUUIDCommitted());
            assertEquals(expectedValue, metadata.clusterUUID());

            final ClusterStatsResponse response = safeAwait(
                listener -> client(nodeName).execute(TransportClusterStatsAction.TYPE, new ClusterStatsRequest(), listener)
            );
            assertEquals(expectedValue, response.getClusterUUID());
        }
    }

    public void testClusterUuidInInitialClusterState() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);

        try {
            internalCluster().startDataOnlyNode();
            assertClusterUuid(false, Metadata.UNKNOWN_CLUSTER_UUID);

            internalCluster().startMasterOnlyNode();
            internalCluster().validateClusterFormed();

            final var clusterUUID = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state().metadata().clusterUUID();
            assertNotEquals(Metadata.UNKNOWN_CLUSTER_UUID, clusterUUID);
            assertClusterUuid(true, clusterUUID);

            internalCluster().stopCurrentMasterNode();
            assertClusterUuid(true, clusterUUID);

            internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
                @Override
                public boolean validateClusterForming() {
                    return false;
                }
            });
            assertClusterUuid(true, clusterUUID);
        } finally {
            while (true) {
                var node = internalCluster().getRandomNodeName();
                if (node == null) {
                    break;
                }
                assertTrue(internalCluster().stopNode(node));
            }
        }
    }
}
