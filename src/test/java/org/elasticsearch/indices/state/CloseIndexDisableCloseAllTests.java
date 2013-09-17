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
package org.elasticsearch.indices.state;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.elasticsearch.test.AbstractIntegrationTest.ClusterScope;
import org.elasticsearch.test.AbstractIntegrationTest.Scope;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@ClusterScope(scope=Scope.SUITE, numNodes=2)
public class CloseIndexDisableCloseAllTests extends AbstractIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder().put("action.disable_close_all_indices", true).put(super.nodeSettings(nodeOrdinal)).build();
    }


    @Test(expected = ElasticSearchIllegalArgumentException.class)
    public void testCloseAllExplicitly() {
        createIndex("test1", "test2", "test3");
        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        client().admin().indices().prepareClose("_all").execute().actionGet();
    }

    @Test(expected = ElasticSearchIllegalArgumentException.class)
    public void testCloseAllWildcard() {
        createIndex("test1", "test2", "test3");
        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        client().admin().indices().prepareClose("*").execute().actionGet();
    }

    @Test(expected = ElasticSearchIllegalArgumentException.class)
    public void testCloseAllWildcard2() {
        createIndex("test1", "test2", "test3");
        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        client().admin().indices().prepareClose("test*").execute().actionGet();
    }

    @Test
    public void testCloseWildcardNonMatchingAll() {
        createIndex("test1", "test2", "test3");
        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        CloseIndexResponse closeIndexResponse = client().admin().indices().prepareClose("*", "-test1").execute().actionGet();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test2", "test3");
    }

    @Test(expected = ElasticSearchIllegalArgumentException.class)
    public void testCloseWildcardMatchingAll() {
        createIndex("test1", "test2", "test3");
        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        client().admin().indices().prepareClose("*", "-test1", "+test1").execute().actionGet();
    }

    @Test
    public void testCloseWildcardNonMatchingAll2() {
        createIndex( "test1", "test2", "test3", "a");
        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        CloseIndexResponse closeIndexResponse = client().admin().indices().prepareClose("test*").execute().actionGet();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1", "test2", "test3");
    }

    private void assertIndexIsClosed(String... indices) {
        checkIndexState(IndexMetaData.State.CLOSE, indices);
    }

    private void checkIndexState(IndexMetaData.State state, String... indices) {
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().execute().actionGet();
        for (String index : indices) {
            IndexMetaData indexMetaData = clusterStateResponse.getState().metaData().indices().get(index);
            assertThat(indexMetaData, notNullValue());
            assertThat(indexMetaData.getState(), equalTo(state));
        }
    }
}
