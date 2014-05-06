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

package org.elasticsearch.indices.cache;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Test;

import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.Matchers.*;

/**
 */
@ClusterScope(scope= Scope.SUITE, numDataNodes =1, numClientNodes = 0, randomDynamicTemplates = false)
public class CacheTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        //Filter cache is cleaned periodically, default is 60s, so make sure it runs often. Thread.sleep for 60s is bad
        return  ImmutableSettings.settingsBuilder().put(super.nodeSettings(nodeOrdinal)).put("indices.cache.filter.clean_interval", "1ms").build();
    }

    @Test
    public void testClearCacheFilterKeys() {
                client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        client().prepareIndex("test", "type", "1").setSource("field", "value").execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        NodesStatsResponse nodesStats = client().admin().cluster().prepareNodesStats().setIndices(true).execute().actionGet();
        assertThat(nodesStats.getNodes()[0].getIndices().getFilterCache().getMemorySizeInBytes(), equalTo(0l));
        IndicesStatsResponse indicesStats = client().admin().indices().prepareStats("test").clear().setFilterCache(true).execute().actionGet();
        assertThat(indicesStats.getTotal().getFilterCache().getMemorySizeInBytes(), equalTo(0l));

        SearchResponse searchResponse = client().prepareSearch().setQuery(filteredQuery(matchAllQuery(), FilterBuilders.termFilter("field", "value").cacheKey("test_key"))).execute().actionGet();
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        nodesStats = client().admin().cluster().prepareNodesStats().setIndices(true).execute().actionGet();
        assertThat(nodesStats.getNodes()[0].getIndices().getFilterCache().getMemorySizeInBytes(), greaterThan(0l));
        indicesStats = client().admin().indices().prepareStats("test").clear().setFilterCache(true).execute().actionGet();
        assertThat(indicesStats.getTotal().getFilterCache().getMemorySizeInBytes(), greaterThan(0l));

        client().admin().indices().prepareClearCache().setFilterKeys("test_key").execute().actionGet();
        nodesStats = client().admin().cluster().prepareNodesStats().setIndices(true).execute().actionGet();
        assertThat(nodesStats.getNodes()[0].getIndices().getFilterCache().getMemorySizeInBytes(), equalTo(0l));
        indicesStats = client().admin().indices().prepareStats("test").clear().setFilterCache(true).execute().actionGet();
        assertThat(indicesStats.getTotal().getFilterCache().getMemorySizeInBytes(), equalTo(0l));
    }

    @Test
    public void testFieldDataStats() {
                client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        client().prepareIndex("test", "type", "1").setSource("field", "value1", "field2", "value1").execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("field", "value2", "field2", "value2").execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        NodesStatsResponse nodesStats = client().admin().cluster().prepareNodesStats().setIndices(true).execute().actionGet();
        assertThat(nodesStats.getNodes()[0].getIndices().getFieldData().getMemorySizeInBytes(), equalTo(0l));
        IndicesStatsResponse indicesStats = client().admin().indices().prepareStats("test").clear().setFieldData(true).execute().actionGet();
        assertThat(indicesStats.getTotal().getFieldData().getMemorySizeInBytes(), equalTo(0l));

        // sort to load it to field data...
        client().prepareSearch().addSort("field", SortOrder.ASC).execute().actionGet();
        client().prepareSearch().addSort("field", SortOrder.ASC).execute().actionGet();

        nodesStats = client().admin().cluster().prepareNodesStats().setIndices(true).execute().actionGet();
        assertThat(nodesStats.getNodes()[0].getIndices().getFieldData().getMemorySizeInBytes(), greaterThan(0l));
        indicesStats = client().admin().indices().prepareStats("test").clear().setFieldData(true).execute().actionGet();
        assertThat(indicesStats.getTotal().getFieldData().getMemorySizeInBytes(), greaterThan(0l));

        // sort to load it to field data...
        client().prepareSearch().addSort("field2", SortOrder.ASC).execute().actionGet();
        client().prepareSearch().addSort("field2", SortOrder.ASC).execute().actionGet();

        // now check the per field stats
        nodesStats = client().admin().cluster().prepareNodesStats().setIndices(new CommonStatsFlags().set(CommonStatsFlags.Flag.FieldData, true).fieldDataFields("*")).execute().actionGet();
        assertThat(nodesStats.getNodes()[0].getIndices().getFieldData().getMemorySizeInBytes(), greaterThan(0l));
        assertThat(nodesStats.getNodes()[0].getIndices().getFieldData().getFields().get("field"), greaterThan(0l));
        assertThat(nodesStats.getNodes()[0].getIndices().getFieldData().getFields().get("field"), lessThan(nodesStats.getNodes()[0].getIndices().getFieldData().getMemorySizeInBytes()));

        indicesStats = client().admin().indices().prepareStats("test").clear().setFieldData(true).setFieldDataFields("*").execute().actionGet();
        assertThat(indicesStats.getTotal().getFieldData().getMemorySizeInBytes(), greaterThan(0l));
        assertThat(indicesStats.getTotal().getFieldData().getFields().get("field"), greaterThan(0l));
        assertThat(indicesStats.getTotal().getFieldData().getFields().get("field"), lessThan(indicesStats.getTotal().getFieldData().getMemorySizeInBytes()));

        client().admin().indices().prepareClearCache().setFieldDataCache(true).execute().actionGet();
        nodesStats = client().admin().cluster().prepareNodesStats().setIndices(true).execute().actionGet();
        assertThat(nodesStats.getNodes()[0].getIndices().getFieldData().getMemorySizeInBytes(), equalTo(0l));
        indicesStats = client().admin().indices().prepareStats("test").clear().setFieldData(true).execute().actionGet();
        assertThat(indicesStats.getTotal().getFieldData().getMemorySizeInBytes(), equalTo(0l));

    }

    @Test
    public void testClearAllCaches() throws Exception {
                client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder()
                        .put("index.number_of_replicas", 0)
                        .put("index.number_of_shards", 1))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        client().prepareIndex("test", "type", "1").setSource("field", "value1").execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("field", "value2").execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        NodesStatsResponse nodesStats = client().admin().cluster().prepareNodesStats().setIndices(true)
                .execute().actionGet();
        assertThat(nodesStats.getNodes()[0].getIndices().getFieldData().getMemorySizeInBytes(), equalTo(0l));
        assertThat(nodesStats.getNodes()[0].getIndices().getFilterCache().getMemorySizeInBytes(), equalTo(0l));

        IndicesStatsResponse indicesStats = client().admin().indices().prepareStats("test")
                .clear().setFieldData(true).setFilterCache(true)
                .execute().actionGet();
        assertThat(indicesStats.getTotal().getFieldData().getMemorySizeInBytes(), equalTo(0l));
        assertThat(indicesStats.getTotal().getFilterCache().getMemorySizeInBytes(), equalTo(0l));

        // sort to load it to field data and filter to load filter cache
        client().prepareSearch()
                .setPostFilter(FilterBuilders.termFilter("field", "value1"))
                .addSort("field", SortOrder.ASC)
                .execute().actionGet();
        client().prepareSearch()
                .setPostFilter(FilterBuilders.termFilter("field", "value2"))
                .addSort("field", SortOrder.ASC)
                .execute().actionGet();

        nodesStats = client().admin().cluster().prepareNodesStats().setIndices(true)
                .execute().actionGet();
        assertThat(nodesStats.getNodes()[0].getIndices().getFieldData().getMemorySizeInBytes(), greaterThan(0l));
        assertThat(nodesStats.getNodes()[0].getIndices().getFilterCache().getMemorySizeInBytes(), greaterThan(0l));

        indicesStats = client().admin().indices().prepareStats("test")
                .clear().setFieldData(true).setFilterCache(true)
                .execute().actionGet();
        assertThat(indicesStats.getTotal().getFieldData().getMemorySizeInBytes(), greaterThan(0l));
        assertThat(indicesStats.getTotal().getFilterCache().getMemorySizeInBytes(), greaterThan(0l));

        client().admin().indices().prepareClearCache().execute().actionGet();
        Thread.sleep(100); // Make sure the filter cache entries have been removed...
        nodesStats = client().admin().cluster().prepareNodesStats().setIndices(true)
                .execute().actionGet();
        assertThat(nodesStats.getNodes()[0].getIndices().getFieldData().getMemorySizeInBytes(), equalTo(0l));
        assertThat(nodesStats.getNodes()[0].getIndices().getFilterCache().getMemorySizeInBytes(), equalTo(0l));

        indicesStats = client().admin().indices().prepareStats("test")
                .clear().setFieldData(true).setFilterCache(true)
                .execute().actionGet();
        assertThat(indicesStats.getTotal().getFieldData().getMemorySizeInBytes(), equalTo(0l));
        assertThat(indicesStats.getTotal().getFilterCache().getMemorySizeInBytes(), equalTo(0l));
    }

}
