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

package org.elasticsearch.test.integration.indices.cache;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 */
@Test
public class ClearCacheTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("node1", ImmutableSettings.settingsBuilder().put("index.cache.stats.refresh_interval", 0));
        client = getClient();
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node1");
    }

    @Test
    public void testClearCacheFilterKeys() {
        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        client.prepareIndex("test", "type", "1").setSource("field", "value").execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();

        NodesStatsResponse nodesStats = client.admin().cluster().prepareNodesStats().setIndices(true).execute().actionGet();
        assertThat(nodesStats.getNodes()[0].getIndices().getCache().filterSizeInBytes(), equalTo(0l));

        SearchResponse searchResponse = client.prepareSearch().setQuery(filteredQuery(matchAllQuery(), FilterBuilders.termFilter("field", "value").cacheKey("test_key"))).execute().actionGet();
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        nodesStats = client.admin().cluster().prepareNodesStats().setIndices(true).execute().actionGet();
        assertThat(nodesStats.getNodes()[0].getIndices().getCache().filterSizeInBytes(), greaterThan(0l));

        client.admin().indices().prepareClearCache().setFilterKeys("test_key").execute().actionGet();
        nodesStats = client.admin().cluster().prepareNodesStats().setIndices(true).execute().actionGet();
        assertThat(nodesStats.getNodes()[0].getIndices().getCache().filterSizeInBytes(), equalTo(0l));
    }
}
