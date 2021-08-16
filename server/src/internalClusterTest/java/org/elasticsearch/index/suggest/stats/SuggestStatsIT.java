/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.suggest.stats;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SuggestStatsIT extends ESIntegTestCase {
    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    public void testSimpleStats() throws Exception {
        // clear all stats first
        client().admin().indices().prepareStats().clear().execute().actionGet();
        final int numNodes = cluster().numDataNodes();
        assertThat(numNodes, greaterThanOrEqualTo(2));
        final int shardsIdx1 = randomIntBetween(1, 10); // we make sure each node gets at least a single shard...
        final int shardsIdx2 = Math.max(numNodes - shardsIdx1, randomIntBetween(1, 10));
        final int totalShards = shardsIdx1 + shardsIdx2;
        assertThat(numNodes, lessThanOrEqualTo(totalShards));
        assertAcked(prepareCreate("test1").setSettings(Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, shardsIdx1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0))
                .addMapping("type", "f", "type=text"));
        assertAcked(prepareCreate("test2").setSettings(Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, shardsIdx2)
                .put(SETTING_NUMBER_OF_REPLICAS, 0))
                .addMapping("type", "f", "type=text"));
        assertThat(shardsIdx1 + shardsIdx2, equalTo(numAssignedShards("test1", "test2")));
        assertThat(numAssignedShards("test1", "test2"), greaterThanOrEqualTo(2));
        ensureGreen();

        for (int i = 0; i < randomIntBetween(20, 100); i++) {
            index("test" + ((i % 2) + 1), "type", "" + i, "f", "test" + i);
        }
        refresh();

        int suggestAllIdx = scaledRandomIntBetween(20, 50);
        int suggestIdx1 = scaledRandomIntBetween(20, 50);
        int suggestIdx2 = scaledRandomIntBetween(20, 50);

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < suggestAllIdx; i++) {
            SearchResponse suggestResponse = addSuggestions(internalCluster().coordOnlyNodeClient().prepareSearch(), i).get();
            assertAllSuccessful(suggestResponse);
        }
        for (int i = 0; i < suggestIdx1; i++) {
            SearchResponse suggestResponse = addSuggestions(internalCluster().coordOnlyNodeClient().prepareSearch("test1"), i).get();
            assertAllSuccessful(suggestResponse);
        }
        for (int i = 0; i < suggestIdx2; i++) {
            SearchResponse suggestResponse = addSuggestions(internalCluster().coordOnlyNodeClient().prepareSearch("test2"), i).get();
            assertAllSuccessful(suggestResponse);
        }
        long endTime = System.currentTimeMillis();

        IndicesStatsResponse indicesStats = client().admin().indices().prepareStats().execute().actionGet();
        final SearchStats.Stats suggest = indicesStats.getTotal().getSearch().getTotal();

        // check current
        assertThat(suggest.getSuggestCurrent(), equalTo(0L));

        // check suggest count
        assertThat(suggest.getSuggestCount(),
            equalTo((long) (suggestAllIdx * totalShards + suggestIdx1 * shardsIdx1 + suggestIdx2 * shardsIdx2)));
        assertThat(indicesStats.getIndices().get("test1").getTotal().getSearch().getTotal().getSuggestCount(),
            equalTo((long) ((suggestAllIdx + suggestIdx1) * shardsIdx1)));
        assertThat(indicesStats.getIndices().get("test2").getTotal().getSearch().getTotal().getSuggestCount(),
            equalTo((long) ((suggestAllIdx + suggestIdx2) * shardsIdx2)));

        logger.info("iter {}, iter1 {}, iter2 {}, {}", suggestAllIdx, suggestIdx1, suggestIdx2, endTime - startTime);
        // check suggest time
        assertThat(suggest.getSuggestTimeInMillis(), greaterThanOrEqualTo(0L));
        // the upperbound is num shards * total time since we do searches in parallel
        assertThat(suggest.getSuggestTimeInMillis(), lessThanOrEqualTo(totalShards * (endTime - startTime)));

        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().execute().actionGet();
        Set<String> nodeIdsWithIndex = nodeIdsWithIndex("test1", "test2");
        int num = 0;
        for (NodeStats stat : nodeStats.getNodes()) {
            SearchStats.Stats suggestStats = stat.getIndices().getSearch().getTotal();
            logger.info("evaluating {}", stat.getNode());
            if (nodeIdsWithIndex.contains(stat.getNode().getId())) {
                assertThat(suggestStats.getSuggestCount(), greaterThan(0L));
                assertThat(suggestStats.getSuggestTimeInMillis(), greaterThanOrEqualTo(0L));
                num++;
            } else {
                assertThat(suggestStats.getSuggestCount(), equalTo(0L));
                assertThat(suggestStats.getSuggestTimeInMillis(), equalTo(0L));
            }
        }

        assertThat(num, greaterThan(0));

    }

    private SearchRequestBuilder addSuggestions(SearchRequestBuilder request, int i) {
        final SuggestBuilder suggestBuilder = new SuggestBuilder();
        for (int s = 0; s < randomIntBetween(2, 10); s++) {
            if (randomBoolean()) {
                suggestBuilder.addSuggestion("s" + s, new PhraseSuggestionBuilder("f").text("test" + i + " test" + (i - 1)));
            } else {
                suggestBuilder.addSuggestion("s" + s, new TermSuggestionBuilder("f").text("test" + i));
            }
        }
        return request.suggest(suggestBuilder);
    }

    private Set<String> nodeIdsWithIndex(String... indices) {
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        GroupShardsIterator<ShardIterator> allAssignedShardsGrouped = state.routingTable().allAssignedShardsGrouped(indices, true);
        Set<String> nodes = new HashSet<>();
        for (ShardIterator shardIterator : allAssignedShardsGrouped) {
            for (ShardRouting routing : shardIterator) {
                if (routing.active()) {
                    nodes.add(routing.currentNodeId());
                }

            }
        }
        return nodes;
    }


    protected int numAssignedShards(String... indices) {
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        GroupShardsIterator<?> allAssignedShardsGrouped = state.routingTable().allAssignedShardsGrouped(indices, true);
        return allAssignedShardsGrouped.size();
    }
}
