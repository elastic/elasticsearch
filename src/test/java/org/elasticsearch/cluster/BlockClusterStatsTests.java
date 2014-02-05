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

import org.elasticsearch.action.admin.cluster.settings.update.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.junit.Test;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Scoped as test, because the if the test with cluster read only block fails, all other tests fail as well, as this is not cleaned up properly
 */
@ClusterScope(scope= ElasticsearchIntegrationTest.Scope.TEST)
public class BlockClusterStatsTests extends ElasticsearchIntegrationTest {

    @Test
    public void testBlocks() throws Exception {
        createIndex("foo");
        ClusterUpdateSettingsResponse updateSettingsResponse = client().admin().cluster().prepareUpdateSettings().setTransientSettings(
                ImmutableSettings.settingsBuilder().put("cluster.blocks.read_only", true).build()).get();
        assertThat(updateSettingsResponse.isAcknowledged(), is(true));
        UpdateSettingsResponse indexSettingsResponse = client().admin().indices().prepareUpdateSettings("foo").setSettings(
                ImmutableSettings.settingsBuilder().put("index.blocks.read_only", true)).get();
        assertThat(indexSettingsResponse.isAcknowledged(), is(true));

        ClusterStateResponse clusterStateResponseUnfiltered = client().admin().cluster().prepareState().clear().setBlocks(true).get();
        assertThat(clusterStateResponseUnfiltered.getState().blocks().global(), hasSize(1));
        assertThat(clusterStateResponseUnfiltered.getState().blocks().indices().size(), is(1));

        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().clear().get();
        assertThat(clusterStateResponse.getState().blocks().global(), hasSize(0));
        assertThat(clusterStateResponse.getState().blocks().indices().size(), is(0));
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(
                ImmutableSettings.settingsBuilder().put("cluster.blocks.read_only", false).build()).get());
    }
}
