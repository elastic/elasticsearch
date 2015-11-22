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

package org.elasticsearch.search.preference;

import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SearchPreferenceIT extends ESIntegTestCase {
    // see #2896
    public void testStopOneNodePreferenceWithRedState() throws InterruptedException, IOException {
        assertAcked(prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", cluster().numDataNodes()+2).put("index.number_of_replicas", 0)));
        ensureGreen();
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type1", ""+i).setSource("field1", "value1").execute().actionGet();
        }
        refresh();
        internalCluster().stopRandomDataNode();
        client().admin().cluster().prepareHealth().setWaitForStatus(ClusterHealthStatus.RED).execute().actionGet();
        String[] preferences = new String[] {"_primary", "_local", "_primary_first", "_prefer_node:somenode", "_prefer_node:server2"};
        for (String pref : preferences) {
            logger.info("--> Testing out preference={}", pref);
            SearchResponse searchResponse = client().prepareSearch().setSize(0).setPreference(pref).execute().actionGet();
            assertThat(RestStatus.OK, equalTo(searchResponse.status()));
            assertThat(pref, searchResponse.getFailedShards(), greaterThanOrEqualTo(0));
            searchResponse = client().prepareSearch().setPreference(pref).execute().actionGet();
            assertThat(RestStatus.OK, equalTo(searchResponse.status()));
            assertThat(pref, searchResponse.getFailedShards(), greaterThanOrEqualTo(0));
        }

        //_only_local is a stricter preference, we need to send the request to a data node
        SearchResponse searchResponse = dataNodeClient().prepareSearch().setSize(0).setPreference("_only_local").execute().actionGet();
        assertThat(RestStatus.OK, equalTo(searchResponse.status()));
        assertThat("_only_local", searchResponse.getFailedShards(), greaterThanOrEqualTo(0));
        searchResponse = dataNodeClient().prepareSearch().setPreference("_only_local").execute().actionGet();
        assertThat(RestStatus.OK, equalTo(searchResponse.status()));
        assertThat("_only_local", searchResponse.getFailedShards(), greaterThanOrEqualTo(0));
    }

    public void testNoPreferenceRandom() throws Exception {
        assertAcked(prepareCreate("test").setSettings(
                //this test needs at least a replica to make sure two consecutive searches go to two different copies of the same data
                settingsBuilder().put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, between(1, maximumNumberOfReplicas()))
        ));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "value1").execute().actionGet();
        refresh();

        final Client client = internalCluster().smartClient();
        SearchResponse searchResponse = client.prepareSearch("test").setQuery(matchAllQuery()).execute().actionGet();
        String firstNodeId = searchResponse.getHits().getAt(0).shard().nodeId();
        searchResponse = client.prepareSearch("test").setQuery(matchAllQuery()).execute().actionGet();
        String secondNodeId = searchResponse.getHits().getAt(0).shard().nodeId();

        assertThat(firstNodeId, not(equalTo(secondNodeId)));
    }

    public void testSimplePreference() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings("number_of_replicas=1").get();
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "value1").execute().actionGet();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPreference("_local").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPreference("_local").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPreference("_primary").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPreference("_primary").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPreference("_replica").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPreference("_replica").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPreference("_replica_first").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPreference("_replica_first").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPreference("1234").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPreference("1234").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
    }

    public void testReplicaPreference() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings("number_of_replicas=0").get();
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "value1").execute().actionGet();
        refresh();

        try {
            client().prepareSearch().setQuery(matchAllQuery()).setPreference("_replica").execute().actionGet();
            fail("should have failed because there are no replicas");
        } catch (Exception e) {
            // pass
        }

        SearchResponse resp = client().prepareSearch().setQuery(matchAllQuery()).setPreference("_replica_first").execute().actionGet();
        assertThat(resp.getHits().totalHits(), equalTo(1l));

        client().admin().indices().prepareUpdateSettings("test").setSettings("number_of_replicas=1").get();
        ensureGreen("test");

        resp = client().prepareSearch().setQuery(matchAllQuery()).setPreference("_replica").execute().actionGet();
        assertThat(resp.getHits().totalHits(), equalTo(1l));
    }

    public void testThatSpecifyingNonExistingNodesReturnsUsefulError() throws Exception {
        createIndex("test");
        ensureGreen();

        try {
            client().prepareSearch().setQuery(matchAllQuery()).setPreference("_only_node:DOES-NOT-EXIST").execute().actionGet();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("No data node with id[DOES-NOT-EXIST] found"));
        }
    }
}
