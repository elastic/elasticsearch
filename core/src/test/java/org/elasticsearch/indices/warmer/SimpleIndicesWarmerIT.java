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

package org.elasticsearch.indices.warmer;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.warmer.delete.DeleteWarmerResponse;
import org.elasticsearch.action.admin.indices.warmer.get.GetWarmersResponse;
import org.elasticsearch.action.admin.indices.warmer.put.PutWarmerResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.cache.request.IndicesRequestCache;
import org.elasticsearch.search.warmer.IndexWarmerMissingException;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

public class SimpleIndicesWarmerIT extends ESIntegTestCase {

    @Test
    public void simpleWarmerTests() {
        createIndex("test");
        ensureGreen();

        PutWarmerResponse putWarmerResponse = client().admin().indices().preparePutWarmer("warmer_1")
                .setSearchRequest(client().prepareSearch("test").setTypes("a1").setQuery(QueryBuilders.termQuery("field", "value1")))
                .execute().actionGet();
        assertThat(putWarmerResponse.isAcknowledged(), equalTo(true));
        putWarmerResponse = client().admin().indices().preparePutWarmer("warmer_2")
                .setSearchRequest(client().prepareSearch("test").setTypes("a2").setQuery(QueryBuilders.termQuery("field", "value2")))
                .execute().actionGet();
        assertThat(putWarmerResponse.isAcknowledged(), equalTo(true));

        client().prepareIndex("test", "type1", "1").setSource("field", "value1").setRefresh(true).execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field", "value2").setRefresh(true).execute().actionGet();

        GetWarmersResponse getWarmersResponse = client().admin().indices().prepareGetWarmers("tes*")
                .execute().actionGet();
        assertThat(getWarmersResponse.getWarmers().size(), equalTo(1));
        assertThat(getWarmersResponse.getWarmers().get("test").size(), equalTo(2));
        assertThat(getWarmersResponse.getWarmers().get("test").get(0).name(), equalTo("warmer_1"));
        assertThat(getWarmersResponse.getWarmers().get("test").get(1).name(), equalTo("warmer_2"));

        getWarmersResponse = client().admin().indices().prepareGetWarmers("test").addWarmers("warmer_*")
                .execute().actionGet();
        assertThat(getWarmersResponse.getWarmers().size(), equalTo(1));
        assertThat(getWarmersResponse.getWarmers().get("test").size(), equalTo(2));
        assertThat(getWarmersResponse.getWarmers().get("test").get(0).name(), equalTo("warmer_1"));
        assertThat(getWarmersResponse.getWarmers().get("test").get(1).name(), equalTo("warmer_2"));

        getWarmersResponse = client().admin().indices().prepareGetWarmers("test").addWarmers("warmer_1")
                .execute().actionGet();
        assertThat(getWarmersResponse.getWarmers().size(), equalTo(1));
        assertThat(getWarmersResponse.getWarmers().get("test").size(), equalTo(1));
        assertThat(getWarmersResponse.getWarmers().get("test").get(0).name(), equalTo("warmer_1"));

        getWarmersResponse = client().admin().indices().prepareGetWarmers("test").addWarmers("warmer_2")
                .execute().actionGet();
        assertThat(getWarmersResponse.getWarmers().size(), equalTo(1));
        assertThat(getWarmersResponse.getWarmers().get("test").size(), equalTo(1));
        assertThat(getWarmersResponse.getWarmers().get("test").get(0).name(), equalTo("warmer_2"));

        getWarmersResponse = client().admin().indices().prepareGetWarmers("test").addTypes("a*").addWarmers("warmer_2")
                .execute().actionGet();
        assertThat(getWarmersResponse.getWarmers().size(), equalTo(1));
        assertThat(getWarmersResponse.getWarmers().get("test").size(), equalTo(1));
        assertThat(getWarmersResponse.getWarmers().get("test").get(0).name(), equalTo("warmer_2"));

        getWarmersResponse = client().admin().indices().prepareGetWarmers("test").addTypes("a1").addWarmers("warmer_2")
                .execute().actionGet();
        assertThat(getWarmersResponse.getWarmers().size(), equalTo(0));
    }

    @Test
    public void templateWarmer() {
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

        createIndex("test");
        ensureGreen();

        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        IndexWarmersMetaData warmersMetaData = clusterState.metaData().index("test").custom(IndexWarmersMetaData.TYPE);
        assertThat(warmersMetaData, Matchers.notNullValue());
        assertThat(warmersMetaData.entries().size(), equalTo(1));

        client().prepareIndex("test", "type1", "1").setSource("field", "value1").setRefresh(true).execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field", "value2").setRefresh(true).execute().actionGet();
    }

    @Test
    public void createIndexWarmer() {
        assertAcked(prepareCreate("test")
                .setSource("{\n" +
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
                        "}"));

        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        IndexWarmersMetaData warmersMetaData = clusterState.metaData().index("test").custom(IndexWarmersMetaData.TYPE);
        assertThat(warmersMetaData, Matchers.notNullValue());
        assertThat(warmersMetaData.entries().size(), equalTo(1));

        client().prepareIndex("test", "type1", "1").setSource("field", "value1").setRefresh(true).execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field", "value2").setRefresh(true).execute().actionGet();
    }

    @Test
    public void deleteNonExistentIndexWarmerTest() {
        createIndex("test");
        try {
            client().admin().indices().prepareDeleteWarmer().setIndices("test").setNames("foo").execute().actionGet();
            fail("warmer foo should not exist");
        } catch (IndexWarmerMissingException ex) {
            assertThat(ex.names()[0], equalTo("foo"));
        }
    }

    @Test // issue 8991
    public void deleteAllIndexWarmerDoesNotThrowWhenNoWarmers() {
        createIndex("test");
        DeleteWarmerResponse deleteWarmerResponse = client().admin().indices().prepareDeleteWarmer()
                .setIndices("test").setNames("_all").execute().actionGet();
        assertThat(deleteWarmerResponse.isAcknowledged(), equalTo(true));

        deleteWarmerResponse = client().admin().indices().prepareDeleteWarmer()
                .setIndices("test").setNames("foo", "_all", "bar").execute().actionGet();
        assertThat(deleteWarmerResponse.isAcknowledged(), equalTo(true));
    }

    @Test
    public void deleteIndexWarmerTest() {
        createIndex("test");
        ensureGreen();

        PutWarmerResponse putWarmerResponse = client().admin().indices().preparePutWarmer("custom_warmer")
                .setSearchRequest(client().prepareSearch("test").setTypes("test").setQuery(QueryBuilders.matchAllQuery()))
                .get();
        assertThat(putWarmerResponse.isAcknowledged(), equalTo(true));

        GetWarmersResponse getWarmersResponse = client().admin().indices().prepareGetWarmers("test").get();
        assertThat(getWarmersResponse.warmers().size(), equalTo(1));
        ObjectObjectCursor<String, List<IndexWarmersMetaData.Entry>> entry = getWarmersResponse.warmers().iterator().next();
        assertThat(entry.key, equalTo("test"));
        assertThat(entry.value.size(), equalTo(1));
        assertThat(entry.value.iterator().next().name(), equalTo("custom_warmer"));

        DeleteWarmerResponse deleteWarmerResponse = client().admin().indices().prepareDeleteWarmer().setIndices("test").setNames("custom_warmer").get();
        assertThat(deleteWarmerResponse.isAcknowledged(), equalTo(true));

        getWarmersResponse = client().admin().indices().prepareGetWarmers("test").get();
        assertThat(getWarmersResponse.warmers().size(), equalTo(0));
    }

    @Test // issue 3246
    public void ensureThatIndexWarmersCanBeChangedOnRuntime() throws Exception {
        createIndex("test");
        ensureGreen();

        PutWarmerResponse putWarmerResponse = client().admin().indices().preparePutWarmer("custom_warmer")
                .setSearchRequest(client().prepareSearch("test").setTypes("test").setQuery(QueryBuilders.matchAllQuery()))
                .execute().actionGet();
        assertThat(putWarmerResponse.isAcknowledged(), equalTo(true));

        client().prepareIndex("test", "test", "1").setSource("foo", "bar").setRefresh(true).execute().actionGet();

        logger.info("--> Disabling warmers execution");
        client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put("index.warmer.enabled", false)).execute().actionGet();

        long warmerRunsAfterDisabling = getWarmerRuns();
        assertThat(warmerRunsAfterDisabling, greaterThanOrEqualTo(1L));

        client().prepareIndex("test", "test", "2").setSource("foo2", "bar2").setRefresh(true).execute().actionGet();

        assertThat(getWarmerRuns(), equalTo(warmerRunsAfterDisabling));
    }

    @Test
    public void gettingAllWarmersUsingAllAndWildcardsShouldWork() throws Exception {
        createIndex("test");
        ensureGreen();

        PutWarmerResponse putWarmerResponse = client().admin().indices().preparePutWarmer("custom_warmer")
                .setSearchRequest(client().prepareSearch("test").setTypes("test").setQuery(QueryBuilders.matchAllQuery()))
                .execute().actionGet();
        assertThat(putWarmerResponse.isAcknowledged(), equalTo(true));

        PutWarmerResponse anotherPutWarmerResponse = client().admin().indices().preparePutWarmer("second_custom_warmer")
                .setSearchRequest(client().prepareSearch("test").setTypes("test").setQuery(QueryBuilders.matchAllQuery()))
                .execute().actionGet();
        assertThat(anotherPutWarmerResponse.isAcknowledged(), equalTo(true));

        GetWarmersResponse getWarmersResponse = client().admin().indices().prepareGetWarmers("*").addWarmers("*").get();
        assertThat(getWarmersResponse.warmers().size(), is(1));

        getWarmersResponse = client().admin().indices().prepareGetWarmers("_all").addWarmers("_all").get();
        assertThat(getWarmersResponse.warmers().size(), is(1));

        getWarmersResponse = client().admin().indices().prepareGetWarmers("t*").addWarmers("c*").get();
        assertThat(getWarmersResponse.warmers().size(), is(1));

        getWarmersResponse = client().admin().indices().prepareGetWarmers("test").addWarmers("custom_warmer", "second_custom_warmer").get();
        assertThat(getWarmersResponse.warmers().size(), is(1));
    }

    private long getWarmerRuns() {
        IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats("test").clear().setWarmer(true).execute().actionGet();
        return indicesStatsResponse.getIndex("test").getPrimaries().warmer.total();
    }

    public void testQueryCacheOnWarmer() {
        createIndex("test");
        ensureGreen();

        assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED, false)));
        logger.info("register warmer with no query cache, validate no cache is used");
        assertAcked(client().admin().indices().preparePutWarmer("warmer_1")
                .setSearchRequest(client().prepareSearch("test").setTypes("a1").setQuery(QueryBuilders.matchAllQuery()))
                .get());

        client().prepareIndex("test", "type1", "1").setSource("field", "value1").setRefresh(true).execute().actionGet();
        assertThat(client().admin().indices().prepareStats("test").setRequestCache(true).get().getTotal().getRequestCache().getMemorySizeInBytes(), equalTo(0l));

        logger.info("register warmer with query cache, validate caching happened");
        assertAcked(client().admin().indices().preparePutWarmer("warmer_1")
                .setSearchRequest(client().prepareSearch("test").setTypes("a1").setQuery(QueryBuilders.matchAllQuery()).setRequestCache(true))
                .get());

        // index again, to make sure it gets refreshed
        client().prepareIndex("test", "type1", "1").setSource("field", "value1").setRefresh(true).execute().actionGet();
        assertThat(client().admin().indices().prepareStats("test").setRequestCache(true).get().getTotal().getRequestCache().getMemorySizeInBytes(), greaterThan(0l));

        client().admin().indices().prepareClearCache().setRequestCache(true).get(); // clean the cache
        assertThat(client().admin().indices().prepareStats("test").setRequestCache(true).get().getTotal().getRequestCache().getMemorySizeInBytes(), equalTo(0l));

        logger.info("enable default query caching on the index level, and test that no flag on warmer still caches");
        assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED, true)));

        assertAcked(client().admin().indices().preparePutWarmer("warmer_1")
                .setSearchRequest(client().prepareSearch("test").setTypes("a1").setQuery(QueryBuilders.matchAllQuery()))
                .get());

        // index again, to make sure it gets refreshed
        client().prepareIndex("test", "type1", "1").setSource("field", "value1").setRefresh(true).execute().actionGet();
        assertThat(client().admin().indices().prepareStats("test").setRequestCache(true).get().getTotal().getRequestCache().getMemorySizeInBytes(), greaterThan(0l));
    }
}
