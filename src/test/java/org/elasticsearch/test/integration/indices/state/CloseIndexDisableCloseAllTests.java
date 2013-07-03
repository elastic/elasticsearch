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
package org.elasticsearch.test.integration.indices.state;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class CloseIndexDisableCloseAllTests extends AbstractNodesTests {

    @BeforeClass
    public void createNodes() {
        ImmutableSettings.Builder settings = ImmutableSettings.builder().put("action.disable_close_all_indices", true);
        startNode("server1", settings);
        startNode("server2", settings);
    }

    @AfterClass
    public void closeNodes() {
        closeAllNodes();
    }

    @AfterMethod
    public void wipeAllIndices() {
        wipeIndices(client("server1"), "_all");
    }

    @Test(expectedExceptions = ElasticSearchIllegalArgumentException.class)
    public void testCloseAllExplicitly() {
        Client client = client("server1");
        createIndices(client, "test1", "test2", "test3");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        client.admin().indices().prepareClose("_all").execute().actionGet();
    }

    @Test(expectedExceptions = ElasticSearchIllegalArgumentException.class)
    public void testCloseAllWildcard() {
        Client client = client("server2");
        createIndices(client, "test1", "test2", "test3");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        client.admin().indices().prepareClose("*").execute().actionGet();
    }

    @Test(expectedExceptions = ElasticSearchIllegalArgumentException.class)
    public void testCloseAllWildcard2() {
        Client client = client("server2");
        createIndices(client, "test1", "test2", "test3");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        client.admin().indices().prepareClose("test*").execute().actionGet();
    }

    @Test
    public void testCloseWildcardNonMatchingAll() {
        Client client = client("server1");
        createIndices(client, "test1", "test2", "test3");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        CloseIndexResponse closeIndexResponse = client.admin().indices().prepareClose("*", "-test1").execute().actionGet();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test2", "test3");
    }

    @Test(expectedExceptions = ElasticSearchIllegalArgumentException.class)
    public void testCloseWildcardMatchingAll() {
        Client client = client("server2");
        createIndices(client, "test1", "test2", "test3");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        client.admin().indices().prepareClose("*", "-test1", "+test1").execute().actionGet();
    }

    @Test
    public void testCloseWildcardNonMatchingAll2() {
        Client client = client("server1");
        createIndices(client, "test1", "test2", "test3", "a");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        CloseIndexResponse closeIndexResponse = client.admin().indices().prepareClose("test*").execute().actionGet();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1", "test2", "test3");
    }

    private void assertIndexIsClosed(String... indices) {
        checkIndexState(IndexMetaData.State.CLOSE, indices);
    }

    private void checkIndexState(IndexMetaData.State state, String... indices) {
        ClusterStateResponse clusterStateResponse = client("server1").admin().cluster().prepareState().execute().actionGet();
        for (String index : indices) {
            IndexMetaData indexMetaData = clusterStateResponse.getState().metaData().indices().get(index);
            assertThat(indexMetaData, notNullValue());
            assertThat(indexMetaData.getState(), equalTo(state));
        }
    }
}
