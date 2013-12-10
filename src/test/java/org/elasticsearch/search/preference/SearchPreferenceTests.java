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

package org.elasticsearch.search.preference;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Priority;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.Matchers.*;

public class SearchPreferenceTests extends ElasticsearchIntegrationTest {

    @Test // see #2896
    public void testStopOneNodePreferenceWithRedState() throws InterruptedException {
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", cluster().size()+2).put("index.number_of_replicas", 0)).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type1", ""+i).setSource("field1", "value1").execute().actionGet();
        }
        client().admin().indices().prepareRefresh().execute().actionGet();
        cluster().stopRandomNode();
        client().admin().cluster().prepareHealth().setWaitForStatus(ClusterHealthStatus.RED).execute().actionGet();
        String[] preferences = new String[] {"_primary", "_local", "_primary_first", "_only_local", "_prefer_node:somenode", "_prefer_node:server2"};
        for (String pref : preferences) {
            SearchResponse searchResponse = client().prepareSearch().setSearchType(SearchType.COUNT).setPreference(pref).execute().actionGet();
            assertThat(RestStatus.OK, equalTo(searchResponse.status()));
            assertThat(pref, searchResponse.getFailedShards(), greaterThanOrEqualTo(0));
            searchResponse = client().prepareSearch().setPreference(pref).execute().actionGet();
            assertThat(RestStatus.OK, equalTo(searchResponse.status()));
            assertThat(pref, searchResponse.getFailedShards(), greaterThanOrEqualTo(0));
        }
    }
    

    @Test
    public void noPreferenceRandom() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 1)).execute().actionGet();
        ensureGreen();
        
        client().prepareIndex("test", "type1").setSource("field1", "value1").execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        final Client client = cluster().smartClient();
        SearchResponse searchResponse = client.prepareSearch("test").setQuery(matchAllQuery()).execute().actionGet();
        String firstNodeId = searchResponse.getHits().getAt(0).shard().nodeId();
        searchResponse = client.prepareSearch("test").setQuery(matchAllQuery()).execute().actionGet();
        String secondNodeId = searchResponse.getHits().getAt(0).shard().nodeId();

        assertThat(firstNodeId, not(equalTo(secondNodeId)));
    }

    @Test
    public void simplePreferenceTests() throws Exception {
        createIndex("test");
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client().prepareIndex("test", "type1").setSource("field1", "value1").execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPreference("_local").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPreference("_local").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPreference("_primary").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPreference("_primary").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPreference("1234").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPreference("1234").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
    }

    @Test (expected = ElasticSearchIllegalArgumentException.class)
    public void testThatSpecifyingNonExistingNodesReturnsUsefulError() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareSearch().setQuery(matchAllQuery()).setPreference("_only_node:DOES-NOT-EXIST").execute().actionGet();
    }
}
