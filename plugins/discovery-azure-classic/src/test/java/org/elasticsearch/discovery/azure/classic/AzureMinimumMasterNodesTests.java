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

package org.elasticsearch.discovery.azure.classic;

import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.cloud.azure.classic.AbstractAzureComputeServiceTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;

/**
 * Reported issue in #15
 * (https://github.com/elastic/elasticsearch-cloud-azure/issues/15)
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE,
        numDataNodes = 0,
        transportClientRatio = 0.0,
        numClientNodes = 0,
        autoMinMasterNodes = false)
@AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-cloud-azure/issues/89")
public class AzureMinimumMasterNodesTests extends AbstractAzureComputeServiceTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("discovery.zen.minimum_master_nodes", 2)
                .put("discovery.initial_state_timeout", "1s")
            .build();
    }

    public void testSimpleOnlyMasterNodeElection() throws IOException {
        final String node1 = internalCluster().startNode();
        registerAzureNode(node1);
        expectThrows(MasterNotDiscoveredException.class, () ->
            // master is not elected yet
            client().admin().cluster().prepareState().setMasterNodeTimeout("100ms").get().getState().nodes().getMasterNodeId()
        );

        final String node2 = internalCluster().startNode();
        registerAzureNode(node2);
        assertNotNull(client().admin().cluster().prepareState().setMasterNodeTimeout("1s").get().getState().nodes().getMasterNodeId());

        internalCluster().stopCurrentMasterNode();
        expectThrows(MasterNotDiscoveredException.class, () ->
            // master has been stopped
            client().admin().cluster().prepareState().setMasterNodeTimeout("1s").get().getState().nodes().getMasterNodeId()
        );

        final String node3 = internalCluster().startNode();
        registerAzureNode(node3);
        assertNotNull(client().admin().cluster().prepareState().setMasterNodeTimeout("1s").get().getState().nodes().getMasterNodeId());
    }
}
