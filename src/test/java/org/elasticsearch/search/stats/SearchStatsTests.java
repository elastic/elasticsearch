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

package org.elasticsearch.search.stats;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.search.stats.SearchStats.Stats;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

/**
 */
public class SearchStatsTests extends ElasticsearchIntegrationTest {

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    @Test
    public void testSimpleStats() throws Exception {
        // clear all stats first
        client().admin().indices().prepareStats().clear().execute().actionGet();
        final int numNodes = cluster().numDataNodes();
        assertThat(numNodes, greaterThanOrEqualTo(2));
        final int shardsIdx1 = randomIntBetween(1, 10); // we make sure each node gets at least a single shard...
        final int shardsIdx2 = Math.max(numNodes - shardsIdx1, randomIntBetween(1, 10));
        assertThat(numNodes, lessThanOrEqualTo(shardsIdx1 + shardsIdx2));
        assertAcked(prepareCreate("test1").setSettings(ImmutableSettings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, shardsIdx1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)));
        int docsTest1 = scaledRandomIntBetween(3*shardsIdx1, 5*shardsIdx1);
        for (int i = 0; i < docsTest1; i++) {
            client().prepareIndex("test1", "type", Integer.toString(i)).setSource("field", "value").execute().actionGet();
            if (rarely()) {
                refresh();
            }
        }
        assertAcked(prepareCreate("test2").setSettings(ImmutableSettings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, shardsIdx2)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)));
        int docsTest2 = scaledRandomIntBetween(3*shardsIdx2, 5*shardsIdx2);
        for (int i = 0; i < docsTest2; i++) {
            client().prepareIndex("test2", "type", Integer.toString(i)).setSource("field", "value").execute().actionGet();
            if (rarely()) {
                refresh();
            }
        }
        assertThat(shardsIdx1+shardsIdx2, equalTo(numAssignedShards("test1", "test2")));
        assertThat(numAssignedShards("test1", "test2"), greaterThanOrEqualTo(2));
        // THERE WILL BE AT LEAST 2 NODES HERE SO WE CAN WAIT FOR GREEN
        ensureGreen();
        refresh();
        int iters = scaledRandomIntBetween(100, 150);
        for (int i = 0; i < iters; i++) {
            SearchResponse searchResponse = internalCluster().clientNodeClient().prepareSearch()
                    .setQuery(QueryBuilders.termQuery("field", "value")).setStats("group1", "group2")
                    .addHighlightedField("field")
                    .addScriptField("scrip1", "_source.field")
                    .setSize(100)
                    .execute().actionGet();
            assertHitCount(searchResponse, docsTest1 + docsTest2);
            assertAllSuccessful(searchResponse);
        }

        IndicesStatsResponse indicesStats = client().admin().indices().prepareStats().execute().actionGet();
        logger.debug("###### indices search stats: " + indicesStats.getTotal().getSearch());
        assertThat(indicesStats.getTotal().getSearch().getTotal().getQueryCount(), greaterThan(0l));
        assertThat(indicesStats.getTotal().getSearch().getTotal().getQueryTimeInMillis(), greaterThan(0l));
        assertThat(indicesStats.getTotal().getSearch().getTotal().getFetchCount(), greaterThan(0l));
        assertThat(indicesStats.getTotal().getSearch().getTotal().getFetchTimeInMillis(), greaterThan(0l));
        assertThat(indicesStats.getTotal().getSearch().getGroupStats(), nullValue());

        indicesStats = client().admin().indices().prepareStats().setGroups("group1").execute().actionGet();
        assertThat(indicesStats.getTotal().getSearch().getGroupStats(), notNullValue());
        assertThat(indicesStats.getTotal().getSearch().getGroupStats().get("group1").getQueryCount(), greaterThan(0l));
        assertThat(indicesStats.getTotal().getSearch().getGroupStats().get("group1").getQueryTimeInMillis(), greaterThan(0l));
        assertThat(indicesStats.getTotal().getSearch().getGroupStats().get("group1").getFetchCount(), greaterThan(0l));
        assertThat(indicesStats.getTotal().getSearch().getGroupStats().get("group1").getFetchTimeInMillis(), greaterThan(0l));
        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().execute().actionGet();
        NodeStats[] nodes = nodeStats.getNodes();
        Set<String> nodeIdsWithIndex = nodeIdsWithIndex("test1", "test2");
        int num = 0;
        for (NodeStats stat : nodes) {
            Stats total = stat.getIndices().getSearch().getTotal();
            if (nodeIdsWithIndex.contains(stat.getNode().getId())) {
                assertThat(total.getQueryCount(), greaterThan(0l));
                assertThat(total.getQueryTimeInMillis(), greaterThan(0l));
                num++;
            } else {
                assertThat(total.getQueryCount(), equalTo(0l));
                assertThat(total.getQueryTimeInMillis(), equalTo(0l));
            }
        }
        
        assertThat(num, greaterThan(0));
     
    }

    private Set<String> nodeIdsWithIndex(String... indices) {
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        GroupShardsIterator allAssignedShardsGrouped = state.routingTable().allAssignedShardsGrouped(indices, true);
        Set<String> nodes = new HashSet<>();
        for (ShardIterator shardIterator : allAssignedShardsGrouped) {
            for (ShardRouting routing : shardIterator.asUnordered()) {
                if (routing.active()) {
                    nodes.add(routing.currentNodeId());
                }

            }
        }
        return nodes;
    }

    @Test
    public void testOpenContexts() {
        createIndex("test1");
        final int docs = scaledRandomIntBetween(20, 50);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex("test1", "type", Integer.toString(i)).setSource("field", "value").execute().actionGet();
        }
        IndicesStatsResponse indicesStats = client().admin().indices().prepareStats().execute().actionGet();
        assertThat(indicesStats.getTotal().getSearch().getOpenContexts(), equalTo(0l));

        SearchResponse searchResponse = client().prepareSearch()
                .setSearchType(SearchType.SCAN)
                .setQuery(matchAllQuery())
                .setSize(5)
                .setScroll(TimeValue.timeValueMinutes(2))
                .execute().actionGet();

        indicesStats = client().admin().indices().prepareStats().execute().actionGet();
        assertThat(indicesStats.getTotal().getSearch().getOpenContexts(), equalTo((long)numAssignedShards("test1")));

        // scroll, but with no timeout (so no context)
        searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).execute().actionGet();

        indicesStats = client().admin().indices().prepareStats().execute().actionGet();
        assertThat(indicesStats.getTotal().getSearch().getOpenContexts(), equalTo(0l));
    }

    protected int numAssignedShards(String... indices) {
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        GroupShardsIterator allAssignedShardsGrouped = state.routingTable().allAssignedShardsGrouped(indices, true);
        return allAssignedShardsGrouped.size();
    }
}
