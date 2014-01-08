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
import com.google.common.collect.ImmutableList;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.warmer.delete.DeleteWarmerResponse;
import org.elasticsearch.action.admin.indices.warmer.get.GetWarmersResponse;
import org.elasticsearch.action.admin.indices.warmer.put.PutWarmerResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.mapper.FieldMapper.Loading;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.warmer.IndexWarmerMissingException;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.Matchers.*;

/**
 */
public class SimpleIndicesWarmerTests extends ElasticsearchIntegrationTest {


    @Test
    public void simpleWarmerTests() {
        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1))
                .execute().actionGet();
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

        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1))
                .execute().actionGet();
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
        createIndex("test");

        try {
            client().admin().indices().prepareDeleteWarmer().setIndices("test").setNames("foo").execute().actionGet(1000);
            assert false : "warmer foo should not exist";
        } catch (IndexWarmerMissingException ex) {
            assertThat(ex.names()[0], equalTo("foo"));
        }
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
        ObjectObjectCursor<String, ImmutableList<IndexWarmersMetaData.Entry>> entry = getWarmersResponse.warmers().iterator().next();
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
        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1, "index.number_of_replicas", 0))
                .execute().actionGet();
        ensureGreen();

        PutWarmerResponse putWarmerResponse = client().admin().indices().preparePutWarmer("custom_warmer")
                .setSearchRequest(client().prepareSearch("test").setTypes("test").setQuery(QueryBuilders.matchAllQuery()))
                .execute().actionGet();
        assertThat(putWarmerResponse.isAcknowledged(), equalTo(true));

        client().prepareIndex("test", "test", "1").setSource("foo", "bar").setRefresh(true).execute().actionGet();

        logger.info("--> Disabling warmers execution");
        client().admin().indices().prepareUpdateSettings("test").setSettings(ImmutableSettings.builder().put("index.warmer.enabled", false)).execute().actionGet();

        long warmerRunsAfterDisabling = getWarmerRuns();
        assertThat(warmerRunsAfterDisabling, greaterThanOrEqualTo(1L));

        client().prepareIndex("test", "test", "2").setSource("foo2", "bar2").setRefresh(true).execute().actionGet();

        assertThat(getWarmerRuns(), equalTo(warmerRunsAfterDisabling));
    }

    private long getWarmerRuns() {
        IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats("test").clear().setWarmer(true).execute().actionGet();
        return indicesStatsResponse.getIndex("test").getPrimaries().warmer.total();
    }

    private long getSegmentsMemoryUsage(String idx) {
        IndicesSegmentResponse response = client().admin().indices().segments(Requests.indicesSegmentsRequest("idx")).actionGet();
        IndexSegments indicesSegments = response.getIndices().get(idx);
        long total = 0;
        for (IndexShardSegments indexShardSegments : indicesSegments) {
            for (ShardSegments shardSegments : indexShardSegments) {
                for (Segment segment : shardSegments) {
                    System.out.println("+=" + segment.memoryInBytes + " " + indexShardSegments.getShardId() + " " + shardSegments.getIndex());
                    total += segment.memoryInBytes;
                }
            }
        }
        return total;
    }

    private enum LoadingMethod {
        LAZY {
            @Override
            void createIndex(String indexName, String type, String fieldName) {
                client().admin().indices().prepareCreate(indexName).setSettings(ImmutableSettings.builder().put(SINGLE_SHARD_NO_REPLICA).put(SearchService.NORMS_LOADING_KEY, Loading.LAZY_VALUE)).execute().actionGet();
            }
        },
        EAGER {
            @Override
            void createIndex(String indexName, String type, String fieldName) {
                client().admin().indices().prepareCreate(indexName).setSettings(ImmutableSettings.builder().put(SINGLE_SHARD_NO_REPLICA).put(SearchService.NORMS_LOADING_KEY, Loading.EAGER_VALUE)).execute().actionGet();
            }
            @Override
            boolean isLazy() {
                return false;
            }
        },
        EAGER_PER_FIELD {
            @Override
            void createIndex(String indexName, String type, String fieldName) throws Exception {
                client().admin().indices().prepareCreate(indexName).setSettings(ImmutableSettings.builder().put(SINGLE_SHARD_NO_REPLICA).put(SearchService.NORMS_LOADING_KEY, Loading.LAZY_VALUE)).addMapping(type, JsonXContent.contentBuilder()
                        .startObject(type)
                            .startObject("properties")
                                .startObject(fieldName)
                                    .field("type", "string")
                                    .startObject("norms")
                                        .field("loading", Loading.EAGER_VALUE)
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                        ).execute().actionGet();
            }
            @Override
            boolean isLazy() {
                return false;
            }
        };
        private static Settings SINGLE_SHARD_NO_REPLICA = ImmutableSettings.builder().put("number_of_shards", 1).put("number_of_replicas", 0).build();
        abstract void createIndex(String indexName, String type, String fieldName) throws Exception;
        boolean isLazy() {
            return true;
        }
    }

    static {
        assert Version.CURRENT.luceneVersion == org.apache.lucene.util.Version.LUCENE_46 :
                "remove me when LUCENE-5373 is fixed";
    }

    @Ignore("enable me when LUCENE-5373 is fixed, see assertion above")
    public void testEagerLoading() throws Exception {
        for (LoadingMethod method : LoadingMethod.values()) {
            System.out.println("METHOD " + method);
            method.createIndex("idx", "t", "foo");
            client().prepareIndex("idx", "t", "1").setSource("foo", "bar").setRefresh(true).execute().actionGet();
            long memoryUsage0 = getSegmentsMemoryUsage("idx");
            // queries load norms if they were not loaded before
            client().prepareSearch("idx").setQuery(QueryBuilders.matchQuery("foo", "bar")).execute().actionGet();
            long memoryUsage1 = getSegmentsMemoryUsage("idx");
            if (method.isLazy()) {
                assertThat(memoryUsage1, greaterThan(memoryUsage0));
            } else {
                assertThat(memoryUsage1, equalTo(memoryUsage0));
            }
            wipeIndices("idx");
        }
    }

}
