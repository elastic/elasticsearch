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
package org.elasticsearch.cluster;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.*;

/**
 * Scoped as test, because the if the test with cluster read only block fails, all other tests fail as well, as this is not cleaned up properly
 */
@ClusterScope(scope= Scope.TEST)
public class BlockClusterStatsTests extends ElasticsearchIntegrationTest {

    @Test
    public void testBlocks() throws Exception {
        assertAcked(prepareCreate("foo").addAlias(new Alias("foo-alias")));
        try {
            assertAcked(client().admin().indices().prepareUpdateSettings("foo").setSettings(
                    ImmutableSettings.settingsBuilder().put("index.blocks.read_only", true)));
            ClusterUpdateSettingsResponse updateSettingsResponse = client().admin().cluster().prepareUpdateSettings().setTransientSettings(
                    ImmutableSettings.settingsBuilder().put("cluster.blocks.read_only", true).build()).get();
            assertThat(updateSettingsResponse.isAcknowledged(), is(true));

            ClusterStateResponse clusterStateResponseUnfiltered = client().admin().cluster().prepareState().setLocal(true).clear().setBlocks(true).get();
            assertThat(clusterStateResponseUnfiltered.getState().blocks().global(), hasSize(1));
            assertThat(clusterStateResponseUnfiltered.getState().blocks().indices().size(), is(1));
            ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().clear().get();
            assertThat(clusterStateResponse.getState().blocks().global(), hasSize(0));
            assertThat(clusterStateResponse.getState().blocks().indices().size(), is(0));

            try {
                client().admin().indices().prepareClose("foo-alias").get();
                fail("close index should have failed");
            } catch(ClusterBlockException e) {
                assertClusterAndIndexBlocks(e);
            }

            try {
                client().admin().indices().prepareDeleteMapping("foo-alias").setType("test").get();
                fail("delete mapping should have failed");
            } catch(ClusterBlockException e) {
                assertClusterAndIndexBlocks(e);
            }

            try {
                client().admin().indices().preparePutMapping("foo-alias").setType("type1").setSource("field1", "type=string").get();
                fail("put mapping should have failed");
            } catch(ClusterBlockException e) {
                assertClusterAndIndexBlocks(e);
            }

            try {
                client().admin().indices().preparePutWarmer("foo-alias").setSearchRequest(Requests.searchRequest("foo-alias")).get();
                fail("put warmer should have failed");
            } catch(ClusterBlockException e) {
                assertClusterAndIndexBlocks(e);
            }

            try {
                client().admin().indices().prepareDeleteWarmer().setIndices("foo-alias").setNames("warmer1").get();
                fail("delete warmer should have failed");
            } catch(ClusterBlockException e) {
                assertClusterAndIndexBlocks(e);
            }

            try {
                client().admin().indices().prepareTypesExists("foo-alias").setTypes("test").get();
                fail("types exists should have failed");
            } catch(ClusterBlockException e) {
                assertClusterAndIndexBlocks(e);
            }

            try {
                client().admin().indices().prepareExists("foo-alias").get();
                fail("indices exists should have failed");
            } catch(ClusterBlockException e) {
                assertClusterAndIndexBlocks(e);
            }

        } finally {
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(
                    ImmutableSettings.settingsBuilder().put("cluster.blocks.read_only", false).build()).get());
            assertAcked(client().admin().indices().prepareUpdateSettings("foo").setSettings(
                    ImmutableSettings.settingsBuilder().put("index.blocks.read_only", false)));
        }
    }

    private void assertClusterAndIndexBlocks(ClusterBlockException e) {
        assertThat(e.blocks().size(), equalTo(2));
        for (ClusterBlock clusterBlock : e.blocks()) {
            assertThat(clusterBlock.status(), equalTo(RestStatus.FORBIDDEN));
            assertThat(clusterBlock.id(), either(equalTo(5)).or(equalTo(6)));
            assertThat(clusterBlock.description(), either(containsString("cluster read-only (api)")).or(containsString("index read-only (api)")));
        }
    }
}
