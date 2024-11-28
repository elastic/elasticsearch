/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.routing;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponses;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SearchPreferenceIT extends ESIntegTestCase {

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING.getKey(), false)
            .build();
    }

    // see #2896
    public void testStopOneNodePreferenceWithRedState() throws IOException {
        assertAcked(prepareCreate("test").setSettings(indexSettings(cluster().numDataNodes() + 2, 0)));
        ensureGreen();
        for (int i = 0; i < 10; i++) {
            prepareIndex("test").setId("" + i).setSource("field1", "value1").get();
        }
        refresh();
        internalCluster().stopRandomDataNode();
        clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForStatus(ClusterHealthStatus.RED).get();
        String[] preferences = new String[] {
            "_local",
            "_prefer_nodes:somenode",
            "_prefer_nodes:server2",
            "_prefer_nodes:somenode,server2" };
        for (String pref : preferences) {
            logger.info("--> Testing out preference={}", pref);
            assertResponses(response -> {
                assertThat(RestStatus.OK, equalTo(response.status()));
                assertThat(pref, response.getFailedShards(), greaterThanOrEqualTo(0));
            }, prepareSearch().setSize(0).setPreference(pref), prepareSearch().setPreference(pref));
        }

        // _only_local is a stricter preference, we need to send the request to a data node
        assertResponses(response -> {
            assertThat(RestStatus.OK, equalTo(response.status()));
            assertThat("_only_local", response.getFailedShards(), greaterThanOrEqualTo(0));
        },
            dataNodeClient().prepareSearch().setSize(0).setPreference("_only_local"),
            dataNodeClient().prepareSearch().setPreference("_only_local")
        );
    }

    public void testNoPreferenceRandom() {
        assertAcked(
            prepareCreate("test").setSettings(
                // this test needs at least a replica to make sure two consecutive searches go to two different copies of the same data
                Settings.builder().put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, between(1, maximumNumberOfReplicas()))
            )
        );
        ensureGreen();

        prepareIndex("test").setSource("field1", "value1").get();
        refresh();

        final Client client = internalCluster().smartClient();
        assertResponse(
            client.prepareSearch("test").setQuery(matchAllQuery()),
            fist -> assertResponse(
                client.prepareSearch("test").setQuery(matchAllQuery()),
                second -> assertThat(
                    fist.getHits().getAt(0).getShard().getNodeId(),
                    not(equalTo(second.getHits().getAt(0).getShard().getNodeId()))
                )
            )
        );
    }

    public void testSimplePreference() {
        indicesAdmin().prepareCreate("test").setSettings("{\"number_of_replicas\": 1}", XContentType.JSON).get();
        ensureGreen();

        prepareIndex("test").setSource("field1", "value1").get();
        refresh();

        assertResponses(
            response -> assertThat(response.getHits().getTotalHits().value(), equalTo(1L)),
            prepareSearch().setQuery(matchAllQuery()),
            prepareSearch().setQuery(matchAllQuery()).setPreference("_local"),
            prepareSearch().setQuery(matchAllQuery()).setPreference("1234")
        );
    }

    public void testThatSpecifyingNonExistingNodesReturnsUsefulError() {
        createIndex("test");
        ensureGreen();

        try {
            prepareSearch().setQuery(matchAllQuery()).setPreference("_only_nodes:DOES-NOT-EXIST").get();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e, hasToString(containsString("no data nodes with criteria [DOES-NOT-EXIST] found for shard: [test][")));
        }
    }

    public void testNodesOnlyRandom() {
        assertAcked(
            prepareCreate("test").setSettings(
                // this test needs at least a replica to make sure two consecutive searches go to two different copies of the same data
                Settings.builder().put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, between(1, maximumNumberOfReplicas()))
            )
        );
        ensureGreen();
        prepareIndex("test").setSource("field1", "value1").get();
        refresh();

        final Client client = internalCluster().smartClient();
        // multiple wildchar to cover multi-param usecase
        SearchRequestBuilder request = client.prepareSearch("test").setQuery(matchAllQuery()).setPreference("_only_nodes:*,nodes*");
        assertSearchOnRandomNodes(request);

        request = client.prepareSearch("test").setQuery(matchAllQuery()).setPreference("_only_nodes:*");
        assertSearchOnRandomNodes(request);

        ArrayList<String> allNodeIds = new ArrayList<>();
        ArrayList<String> allNodeNames = new ArrayList<>();
        ArrayList<String> allNodeHosts = new ArrayList<>();
        NodesStatsResponse nodeStats = clusterAdmin().prepareNodesStats().get();
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
            assertResponse(request, response -> {
                assertThat(response.getHits().getHits().length, greaterThan(0));
                hitNodes.add(response.getHits().getAt(0).getShard().getNodeId());
            });
        }
        assertThat(hitNodes.size(), greaterThan(1));
    }

    public void testCustomPreferenceUnaffectedByOtherShardMovements() {

        /*
         * Custom preferences can be used to encourage searches to go to a consistent set of shard copies, meaning that other copies' data
         * is rarely touched and can be dropped from the filesystem cache. This works best if the set of shards searched doesn't change
         * unnecessarily, so this test verifies a consistent routing even as other shards are created/relocated/removed.
         */

        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(indexSettings())
                    .put(SETTING_NUMBER_OF_REPLICAS, between(1, maximumNumberOfReplicas()))
                    .put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
            )
        );
        ensureGreen();
        prepareIndex("test").setSource("field1", "value1").get();
        refresh();

        final String customPreference = randomAlphaOfLength(10);

        final String nodeId;
        var response = prepareSearch("test").setQuery(matchAllQuery()).setPreference(customPreference).get();
        try {
            nodeId = response.getHits().getAt(0).getShard().getNodeId();
        } finally {
            response.decRef();
        }

        assertSearchesSpecificNode("test", customPreference, nodeId);

        final int replicasInNewIndex = between(1, maximumNumberOfReplicas());
        assertAcked(
            prepareCreate("test2").setSettings(Settings.builder().put(indexSettings()).put(SETTING_NUMBER_OF_REPLICAS, replicasInNewIndex))
        );
        ensureGreen();

        assertSearchesSpecificNode("test", customPreference, nodeId);

        setReplicaCount(replicasInNewIndex - 1, "test2");

        assertSearchesSpecificNode("test", customPreference, nodeId);

        updateIndexSettings(
            Settings.builder()
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put(
                    IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name",
                    internalCluster().getNodeNameThat(DiscoveryNode::canContainData)
                ),
            "test2"
        );

        ensureGreen();

        assertSearchesSpecificNode("test", customPreference, nodeId);

        assertAcked(indicesAdmin().prepareDelete("test2"));

        assertSearchesSpecificNode("test", customPreference, nodeId);
    }

    private static void assertSearchesSpecificNode(String index, String customPreference, String nodeId) {
        assertResponse(prepareSearch(index).setQuery(matchAllQuery()).setPreference(customPreference), response -> {
            assertThat(response.getHits().getHits().length, equalTo(1));
            assertThat(response.getHits().getAt(0).getShard().getNodeId(), equalTo(nodeId));
        });
    }
}
