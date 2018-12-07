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

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SearchPreferenceIT extends ESIntegTestCase {

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                .put(OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING.getKey(), false).build();
    }

    // see #2896
    public void testStopOneNodePreferenceWithRedState() throws InterruptedException, IOException {
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", cluster().numDataNodes()+2)
                .put("index.number_of_replicas", 0)));
        ensureGreen();
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type1", ""+i).setSource("field1", "value1").get();
        }
        refresh();
        internalCluster().stopRandomDataNode();
        client().admin().cluster().prepareHealth().setWaitForStatus(ClusterHealthStatus.RED).get();
        String[] preferences = new String[]{"_local", "_prefer_nodes:somenode", "_prefer_nodes:server2", "_prefer_nodes:somenode,server2"};
        for (String pref : preferences) {
            logger.info("--> Testing out preference={}", pref);
            SearchResponse searchResponse = client().prepareSearch().setSize(0).setPreference(pref).get();
            assertThat(RestStatus.OK, equalTo(searchResponse.status()));
            assertThat(pref, searchResponse.getFailedShards(), greaterThanOrEqualTo(0));
            searchResponse = client().prepareSearch().setPreference(pref).get();
            assertThat(RestStatus.OK, equalTo(searchResponse.status()));
            assertThat(pref, searchResponse.getFailedShards(), greaterThanOrEqualTo(0));
        }

        //_only_local is a stricter preference, we need to send the request to a data node
        SearchResponse searchResponse = dataNodeClient().prepareSearch().setSize(0).setPreference("_only_local").get();
        assertThat(RestStatus.OK, equalTo(searchResponse.status()));
        assertThat("_only_local", searchResponse.getFailedShards(), greaterThanOrEqualTo(0));
        searchResponse = dataNodeClient().prepareSearch().setPreference("_only_local").get();
        assertThat(RestStatus.OK, equalTo(searchResponse.status()));
        assertThat("_only_local", searchResponse.getFailedShards(), greaterThanOrEqualTo(0));
    }

    public void testNoPreferenceRandom() throws Exception {
        assertAcked(prepareCreate("test").setSettings(
                //this test needs at least a replica to make sure two consecutive searches go to two different copies of the same data
                Settings.builder().put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, between(1, maximumNumberOfReplicas()))
        ));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "value1").get();
        refresh();

        final Client client = internalCluster().smartClient();
        SearchResponse searchResponse = client.prepareSearch("test").setQuery(matchAllQuery()).get();
        String firstNodeId = searchResponse.getHits().getAt(0).getShard().getNodeId();
        searchResponse = client.prepareSearch("test").setQuery(matchAllQuery()).get();
        String secondNodeId = searchResponse.getHits().getAt(0).getShard().getNodeId();

        assertThat(firstNodeId, not(equalTo(secondNodeId)));
    }

    public void testSimplePreference() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings("{\"number_of_replicas\": 1}", XContentType.JSON).get();
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "value1").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(matchAllQuery()).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPreference("_local").get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setPreference("1234").get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
    }

    public void testThatSpecifyingNonExistingNodesReturnsUsefulError() throws Exception {
        createIndex("test");
        ensureGreen();

        try {
            client().prepareSearch().setQuery(matchAllQuery()).setPreference("_only_nodes:DOES-NOT-EXIST").get();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e, hasToString(containsString("no data nodes with criteria [DOES-NOT-EXIST] found for shard: [test][")));
        }
    }

    public void testNodesOnlyRandom() throws Exception {
        assertAcked(prepareCreate("test").setSettings(
            //this test needs at least a replica to make sure two consecutive searches go to two different copies of the same data
            Settings.builder().put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, between(1, maximumNumberOfReplicas()))));
        ensureGreen();
        client().prepareIndex("test", "type1").setSource("field1", "value1").get();
        refresh();

        final Client client = internalCluster().smartClient();
        SearchRequestBuilder request = client.prepareSearch("test")
            .setQuery(matchAllQuery()).setPreference("_only_nodes:*,nodes*"); // multiple wildchar  to cover multi-param usecase
        assertSearchOnRandomNodes(request);

        request = client.prepareSearch("test")
            .setQuery(matchAllQuery()).setPreference("_only_nodes:*");
        assertSearchOnRandomNodes(request);

        ArrayList<String> allNodeIds = new ArrayList<>();
        ArrayList<String> allNodeNames = new ArrayList<>();
        ArrayList<String> allNodeHosts = new ArrayList<>();
        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().get();
        for (NodeStats node : nodeStats.getNodes()) {
            allNodeIds.add(node.getNode().getId());
            allNodeNames.add(node.getNode().getName());
            allNodeHosts.add(node.getHostname());
        }

        String node_expr = "_only_nodes:" + Strings.arrayToCommaDelimitedString(allNodeIds.toArray());
        request = client.prepareSearch("test").setQuery(matchAllQuery()).setPreference(node_expr);
        assertSearchOnRandomNodes(request);

        node_expr = "_only_nodes:" + Strings.arrayToCommaDelimitedString(allNodeNames.toArray());
        request = client.prepareSearch("test").setQuery(matchAllQuery()).setPreference(node_expr);
        assertSearchOnRandomNodes(request);

        node_expr = "_only_nodes:" + Strings.arrayToCommaDelimitedString(allNodeHosts.toArray());
        request = client.prepareSearch("test").setQuery(matchAllQuery()).setPreference(node_expr);
        assertSearchOnRandomNodes(request);

        node_expr = "_only_nodes:" + Strings.arrayToCommaDelimitedString(allNodeHosts.toArray());
        request = client.prepareSearch("test").setQuery(matchAllQuery()).setPreference(node_expr);
        assertSearchOnRandomNodes(request);

        // Mix of valid and invalid nodes
        node_expr = "_only_nodes:*,invalidnode";
        request = client.prepareSearch("test").setQuery(matchAllQuery()).setPreference(node_expr);
        assertSearchOnRandomNodes(request);
    }

    private void assertSearchOnRandomNodes(SearchRequestBuilder request) {
        Set<String> hitNodes = new HashSet<>();
        for (int i = 0; i < 2; i++) {
            SearchResponse searchResponse = request.get();
            assertThat(searchResponse.getHits().getHits().length, greaterThan(0));
            hitNodes.add(searchResponse.getHits().getAt(0).getShard().getNodeId());
        }
        assertThat(hitNodes.size(), greaterThan(1));
    }
}
