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

package org.elasticsearch.test.integration.indices.wamer;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.warmer.IndexWarmerMissingException;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 */
public class SimpleIndicesWarmerTests extends AbstractSharedClusterTest {


    @Test
    public void simpleWarmerTests() {
        client().admin().indices().prepareDelete().execute().actionGet();

        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1))
                .execute().actionGet();

        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client().admin().indices().preparePutWarmer("warmer_1")
                .setSearchRequest(client().prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "value1")))
                .execute().actionGet();
        client().admin().indices().preparePutWarmer("warmer_2")
                .setSearchRequest(client().prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "value2")))
                .execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource("field", "value1").setRefresh(true).execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field", "value2").setRefresh(true).execute().actionGet();
    }

    @Test
    public void templateWarmer() {
        client().admin().indices().prepareDelete().execute().actionGet();

        client().admin().indices().preparePutTemplate("template_1")
                .setSource("{\n" +
                        "    \"template\" : \"*\",\n" +
                        "    \"warmers\" : {\n" +
                        "        \"warmer_1\" : {\n" +
                        "            \"types\" : [],\n" +
                        "            \"source\" : {\n" +
                        "                \"query\" : {\n" +
                        "                    \"match_all\" : {}\n" +
                        "                }\n" +
                        "            }\n" +
                        "        }\n" +
                        "    }\n" +
                        "}")
                .execute().actionGet();

        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1))
                .execute().actionGet();

        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        IndexWarmersMetaData warmersMetaData = clusterState.metaData().index("test").custom(IndexWarmersMetaData.TYPE);
        assertThat(warmersMetaData, Matchers.notNullValue());
        assertThat(warmersMetaData.entries().size(), equalTo(1));

        client().prepareIndex("test", "type1", "1").setSource("field", "value1").setRefresh(true).execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field", "value2").setRefresh(true).execute().actionGet();
    }

    @Test
    public void createIndexWarmer() {
        client().admin().indices().prepareDelete().execute().actionGet();

        client().admin().indices().prepareCreate("test")
                .setSource("{\n" +
                        "    \"settings\" : {\n" +
                        "        \"index.number_of_shards\" : 1\n" +
                        "    },\n" +
                        "    \"warmers\" : {\n" +
                        "        \"warmer_1\" : {\n" +
                        "            \"types\" : [],\n" +
                        "            \"source\" : {\n" +
                        "                \"query\" : {\n" +
                        "                    \"match_all\" : {}\n" +
                        "                }\n" +
                        "            }\n" +
                        "        }\n" +
                        "    }\n" +
                        "}")
                .execute().actionGet();

        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        IndexWarmersMetaData warmersMetaData = clusterState.metaData().index("test").custom(IndexWarmersMetaData.TYPE);
        assertThat(warmersMetaData, Matchers.notNullValue());
        assertThat(warmersMetaData.entries().size(), equalTo(1));

        client().prepareIndex("test", "type1", "1").setSource("field", "value1").setRefresh(true).execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field", "value2").setRefresh(true).execute().actionGet();
    }

    @Test
    public void deleteNonExistentIndexWarmerTest() {
        client().admin().indices().prepareDelete().execute().actionGet();

        client().admin().indices().prepareCreate("test").execute().actionGet();

        try {
            client().admin().indices().prepareDeleteWarmer().setIndices("test").setName("foo").execute().actionGet(1000);
            assert false : "warmer foo should not exist";
        } catch (IndexWarmerMissingException ex) {
            assertThat(ex.name(), equalTo("foo"));
        }
    }

    @Test // issue 3246
    public void ensureThatIndexWarmersCanBeChangedOnRuntime() throws Exception {
        client().admin().indices().prepareDelete().execute().actionGet();
        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1, "index.number_of_replicas", 0))
                .execute().actionGet();

        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        client().admin().indices().preparePutWarmer("custom_warmer")
                .setSearchRequest(client().prepareSearch("test").setTypes("test").setQuery(QueryBuilders.matchAllQuery()))
                .execute().actionGet();

        client().prepareIndex("test", "test", "1").setSource("foo", "bar").setRefresh(true).execute().actionGet();

        client().admin().indices().prepareUpdateSettings("test").setSettings(ImmutableSettings.builder().put("index.warmer.enabled", false)).execute().actionGet();

        long warmerRunsAfterDisabling = getWarmerRuns();
        assertThat(getWarmerRuns(), greaterThanOrEqualTo(1L));

        client().prepareIndex("test", "test", "2").setSource("foo2", "bar2").setRefresh(true).execute().actionGet();

        assertThat(getWarmerRuns(), equalTo(warmerRunsAfterDisabling));
    }

    private long getWarmerRuns() {
        IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats("test").clear().setWarmer(true).execute().actionGet();
        return indicesStatsResponse.getIndex("test").getPrimaries().warmer.total();
    }
}
