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

package org.elasticsearch.discovery.azure;

import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.cloud.azure.AbstractAzureComputeServiceTestCase;
import org.elasticsearch.cloud.azure.AzureComputeServiceTwoNodesMock;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Reported issue in #15
 * (https://github.com/elastic/elasticsearch-cloud-azure/issues/15)
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE,
        numDataNodes = 0,
        transportClientRatio = 0.0,
        numClientNodes = 0)
@AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-cloud-azure/issues/89")
public class AzureMinimumMasterNodesTests extends AbstractAzureComputeServiceTestCase {

    public AzureMinimumMasterNodesTests() {
        super(AzureComputeServiceTwoNodesMock.TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("discovery.zen.minimum_master_nodes", 2)
                // Make the test run faster
                .put(ZenDiscovery.JOIN_TIMEOUT_SETTING.getKey(), "50ms")
                .put(ZenDiscovery.PING_TIMEOUT_SETTING.getKey(), "10ms")
                .put("discovery.initial_state_timeout", "100ms");
        return builder.build();
    }

    public void testSimpleOnlyMasterNodeElection() throws IOException {
        logger.info("--> start data node / non master node");
        internalCluster().startNode();
        try {
            assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("100ms").execute().actionGet().getState().nodes().getMasterNodeId(), nullValue());
            fail("should not be able to find master");
        } catch (MasterNotDiscoveredException e) {
            // all is well, no master elected
        }
        logger.info("--> start another node");
        internalCluster().startNode();
        assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("1s").execute().actionGet().getState().nodes().getMasterNodeId(), notNullValue());

        logger.info("--> stop master node");
        internalCluster().stopCurrentMasterNode();

        try {
            assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("1s").execute().actionGet().getState().nodes().getMasterNodeId(), nullValue());
            fail("should not be able to find master");
        } catch (MasterNotDiscoveredException e) {
            // all is well, no master elected
        }

        logger.info("--> start another node");
        internalCluster().startNode();
        assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("1s").execute().actionGet().getState().nodes().getMasterNodeId(), notNullValue());
    }
}
