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

package org.elasticsearch.bwcompat;


import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchBackwardsCompatIntegrationTest;
import org.junit.Test;

public class ClusterStateBackwardsCompat extends ElasticsearchBackwardsCompatIntegrationTest {

    @Test
    public void testClusterState() throws Exception {
        createIndex("test");

        NodesInfoResponse nodesInfo = client().admin().cluster().prepareNodesInfo().execute().actionGet();
        Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.ignore_cluster_name", true)
                .put("node.name", "transport_client_" + getTestName()).build();

        // connect to each node with a custom TransportClient, issue a ClusterStateRequest to test serialization
        for (NodeInfo n : nodesInfo.getNodes()) {
            TransportClient tc = null;
            try {
                tc = new TransportClient(settings).addTransportAddress(n.getNode().address());
                tc.admin().cluster().prepareState().clear().execute().actionGet();
            } finally {
                if (tc != null)
                    tc.close();
            }
        }
    }
}
