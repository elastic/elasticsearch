/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ClusterScope(scope = Scope.TEST)
public class SearchContextInvalidationIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("search.default_allow_partial_results", false)
            .build();
    }

    public void testScrollReturns404WhenNodeLeavesCluster() throws Exception {
        assertAcked(
            prepareCreate("test-index").setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
        );

        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            docs.add(prepareIndex("test-index").setSource("field", "value" + i));
        }
        indexRandom(true, docs);

        String nodeWithShard = getNodeWithShard("test-index");
        assertNotNull("Should find node with shard", nodeWithShard);

        SearchResponse searchResponse = null;
        try {
            searchResponse = prepareSearch("test-index").setQuery(matchAllQuery())
                .setSize(10)
                .setScroll(TimeValue.timeValueMinutes(5))
                .get();

            String scrollId = searchResponse.getScrollId();
            assertNotNull("Should have scroll ID", scrollId);
            assertThat(searchResponse.getHits().getHits().length, greaterThan(0));

            internalCluster().stopNode(nodeWithShard);
            ensureStableCluster(internalCluster().size());

            SearchContextMissingNodesException ex = expectThrows(
                SearchContextMissingNodesException.class,
                client().prepareSearchScroll(scrollId).setScroll(TimeValue.timeValueMinutes(1))
            );

            assertThat(ex.status(), equalTo(RestStatus.NOT_FOUND));
            assertThat(ex.getContextType(), equalTo(SearchContextMissingNodesException.ContextType.SCROLL));
            assertThat(
                ex.getMessage(),
                containsString("Search context of type [" + ex.getContextType() + "] references nodes that have left the cluster")
            );
        } finally {
            if (searchResponse != null) {
                searchResponse.decRef();
            }
        }
    }

    public void testScrollWorksWhenUnrelatedNodeLeaves() throws Exception {
        String dataNode1 = internalCluster().startDataOnlyNode();
        String dataNode2 = internalCluster().startDataOnlyNode();

        assertAcked(
            prepareCreate("test-index").setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing.allocation.require._name", dataNode1)
            )
        );

        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            docs.add(prepareIndex("test-index").setSource("field", "value" + i));
        }
        indexRandom(true, docs);
        ensureGreen("test-index");

        SearchResponse searchResponse = null;
        SearchResponse scrollResponse = null;
        ClearScrollResponse clearResponse = null;
        try {
            searchResponse = prepareSearch("test-index").setQuery(matchAllQuery())
                .setSize(10)
                .setScroll(TimeValue.timeValueMinutes(5))
                .get();

            String scrollId = searchResponse.getScrollId();
            assertNotNull(scrollId);

            internalCluster().stopNode(dataNode2);
            ensureStableCluster(internalCluster().size());

            scrollResponse = client().prepareSearchScroll(scrollId).setScroll(TimeValue.timeValueMinutes(1)).get();
            assertNotNull(scrollResponse);

            clearResponse = client().prepareClearScroll().addScrollId(scrollId).get();
            assertTrue(clearResponse.isSucceeded());
        } finally {
            if (searchResponse != null) searchResponse.decRef();
            if (scrollResponse != null) scrollResponse.decRef();
            if (clearResponse != null) clearResponse.decRef();
        }
    }

    /**
     * Verifies that a PIT search returns 404 when the node holding the shard leaves the cluster
     * and the PIT search context IDs are not retryable. For retryable PITs (e.g., searchable snapshots in stateful or
     * relocated PITs in serverless), the search can be retried on other nodes holding the same data;
     * see RetrySearchIntegTests for those scenarios.
     */
    public void testPitReturns404WhenNodeLeavesCluster() throws Exception {
        String dataNode1 = internalCluster().startDataOnlyNode();

        assertAcked(
            prepareCreate("test-index").setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing.allocation.require._name", dataNode1)
            )
        );

        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            docs.add(prepareIndex("test-index").setSource("field", "value" + i));
        }
        indexRandom(true, docs);
        ensureGreen("test-index");

        OpenPointInTimeRequest openRequest = new OpenPointInTimeRequest("test-index").keepAlive(TimeValue.timeValueMinutes(5));
        OpenPointInTimeResponse openResponse = client().execute(TransportOpenPointInTimeAction.TYPE, openRequest).actionGet();
        BytesReference pitId = openResponse.getPointInTimeId();
        assertNotNull("Should have PIT ID", pitId);

        SearchResponse searchResponse = prepareSearch().setPointInTime(new PointInTimeBuilder(pitId))
            .setQuery(matchAllQuery())
            .setSize(10)
            .get();
        try {
            assertThat(searchResponse.getHits().getHits().length, greaterThan(0));
        } finally {
            searchResponse.decRef();
        }

        internalCluster().stopNode(dataNode1);
        ensureStableCluster(internalCluster().size());

        SearchContextMissingNodesException ex = expectThrows(
            SearchContextMissingNodesException.class,
            prepareSearch().setPointInTime(new PointInTimeBuilder(pitId))
                .setQuery(matchAllQuery())
                .setSize(10)
                .setAllowPartialSearchResults(false)
        );

        assertThat(ex.status(), equalTo(RestStatus.NOT_FOUND));
        assertThat(ex.getContextType(), equalTo(SearchContextMissingNodesException.ContextType.PIT));
        assertThat(
            ex.getMessage(),
            containsString("Search context of type [" + ex.getContextType() + "] references nodes that have left the cluster")
        );
    }

    public void testPitWorksWhenUnrelatedNodeLeaves() throws Exception {
        String dataNode1 = internalCluster().startDataOnlyNode();
        String dataNode2 = internalCluster().startDataOnlyNode();

        assertAcked(
            prepareCreate("test-index").setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing.allocation.require._name", dataNode1)
            )
        );

        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            docs.add(prepareIndex("test-index").setSource("field", "value" + i));
        }
        indexRandom(true, docs);
        ensureGreen("test-index");

        OpenPointInTimeRequest openRequest = new OpenPointInTimeRequest("test-index").keepAlive(TimeValue.timeValueMinutes(5));
        OpenPointInTimeResponse openResponse = client().execute(TransportOpenPointInTimeAction.TYPE, openRequest).actionGet();
        BytesReference pitId = openResponse.getPointInTimeId();
        assertNotNull("Should have PIT ID", pitId);

        internalCluster().stopNode(dataNode2);
        ensureStableCluster(internalCluster().size());

        SearchResponse searchResponse = prepareSearch().setPointInTime(new PointInTimeBuilder(pitId))
            .setQuery(matchAllQuery())
            .setSize(10)
            .get();
        try {
            assertNotNull(searchResponse);
            assertThat(searchResponse.getHits().getHits().length, greaterThan(0));
        } finally {
            searchResponse.decRef();
        }

        ClosePointInTimeResponse closeResponse = client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pitId))
            .actionGet();
        assertTrue(closeResponse.isSucceeded());
    }

    private String getNodeWithShard(String indexName) {
        ClusterState clusterState = clusterService().state();

        for (var shardRouting : clusterState.routingTable().allShards(indexName)) {
            if (shardRouting.primary() && shardRouting.assignedToNode()) {
                String nodeId = shardRouting.currentNodeId();
                DiscoveryNode node = clusterState.nodes().get(nodeId);
                if (node != null) {
                    return node.getName();
                }
            }
        }
        return null;
    }
}
