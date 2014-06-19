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

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Reported issue in #15
 * (https://github.com/elasticsearch/elasticsearch-cloud-azure/issues/15)
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST,
        numDataNodes = 0,
        transportClientRatio = 0.0,
        numClientNodes = 0)
public class AzureMinimumMasterNodesTest extends AbstractAzureComputeServiceTest {

    public AzureMinimumMasterNodesTest() {
        super(AzureComputeServiceTwoNodesMock.class);
    }

    @Override
    protected final Settings settingsBuilder() {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder()
                .put("discovery.zen.minimum_master_nodes", 2)
                .put(super.settingsBuilder());
        return builder.build();
    }

    @Test
    public void simpleOnlyMasterNodeElection() throws IOException {
        logger.info("--> start data node / non master node");
        internalCluster().startNode(settingsBuilder());
        try {
            assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("100ms").execute().actionGet().getState().nodes().masterNodeId(), nullValue());
            fail("should not be able to find master");
        } catch (MasterNotDiscoveredException e) {
            // all is well, no master elected
        }
        logger.info("--> start another node");
        internalCluster().startNode(settingsBuilder());
        assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("1s").execute().actionGet().getState().nodes().masterNodeId(), notNullValue());

        logger.info("--> stop master node");
        internalCluster().stopCurrentMasterNode();

        try {
            assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("1s").execute().actionGet().getState().nodes().masterNodeId(), nullValue());
            fail("should not be able to find master");
        } catch (MasterNotDiscoveredException e) {
            // all is well, no master elected
        }

        logger.info("--> start another node");
        internalCluster().startNode(settingsBuilder());
        assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("1s").execute().actionGet().getState().nodes().masterNodeId(), notNullValue());
    }
}
