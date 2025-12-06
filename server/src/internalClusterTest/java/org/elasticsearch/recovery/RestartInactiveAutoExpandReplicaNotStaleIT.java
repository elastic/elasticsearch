/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.recovery;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class RestartInactiveAutoExpandReplicaNotStaleIT extends ESIntegTestCase {

    public void testNotStale() throws Exception {
        internalCluster().startMasterOnlyNode();
        String primary = internalCluster().startDataOnlyNode();
        createIndex("test", Settings.builder().put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1").build());
        index("test", "test", "{}");

        String replica = internalCluster().startDataOnlyNode();

        ensureGreen();

        ClusterStateResponse clusterStateResponse = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get();
        IndexMetadata target = clusterStateResponse.getState().getMetadata().getProject().index("test");

        internalCluster().restartNode(replica, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                IndicesService indicesService = internalCluster().getInstance(IndicesService.class, primary);

                indicesService.indexService(target.getIndex()).getShard(0).syncRetentionLeases();
                Thread.sleep(10);
                internalCluster().stopNode(primary);
                return super.onNodeStopped(nodeName);
            }
        });

        ensureYellow();
    }
}
