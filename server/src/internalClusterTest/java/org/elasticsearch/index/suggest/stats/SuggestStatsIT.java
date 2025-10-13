/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.suggest.stats;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
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
        indicesAdmin().prepareStats().clear().get();
        final int numNodes = cluster().numDataNodes();
        assertThat(numNodes, greaterThanOrEqualTo(2));
        final int shardsIdx1 = randomIntBetween(1, 10); // we make sure each node gets at least a single shard...
        final int shardsIdx2 = Math.max(numNodes - shardsIdx1, randomIntBetween(1, 10));
        final int totalShards = shardsIdx1 + shardsIdx2;
        assertThat(numNodes, lessThanOrEqualTo(totalShards));
        assertAcked(prepareCreate("test1").setSettings(indexSettings(shardsIdx1, 0)).setMapping("f", "type=text"));
        assertAcked(prepareCreate("test2").setSettings(indexSettings(shardsIdx2, 0)).setMapping("f", "type=text"));
        assertThat(shardsIdx1 + shardsIdx2, equalTo(numAssignedShards("test1", "test2")));
        assertThat(numAssignedShards("test1", "test2"), greaterThanOrEqualTo(2));
        ensureGreen();

        for (int i = 0; i < randomIntBetween(20, 100); i++) {
            indexDoc("test" + ((i % 2) + 1), "" + i, "f", "test" + i);
        }
        refresh();

        int suggestAllIdx = scaledRandomIntBetween(20, 50);
        int suggestIdx1 = scaledRandomIntBetween(20, 50);
        int suggestIdx2 = scaledRandomIntBetween(20, 50);

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < suggestAllIdx; i++) {
            assertResponse(
                addSuggestions(internalCluster().coordOnlyNodeClient().prepareSearch(), i),
                response -> assertAllSuccessful(response)
            );
        }
        for (int i = 0; i < suggestIdx1; i++) {
            assertResponse(
                addSuggestions(internalCluster().coordOnlyNodeClient().prepareSearch("test1"), i),
                response -> assertAllSuccessful(response)
            );
        }
        for (int i = 0; i < suggestIdx2; i++) {
            assertResponse(
                addSuggestions(internalCluster().coordOnlyNodeClient().prepareSearch("test2"), i),
                response -> assertAllSuccessful(response)
            );
        }
        long endTime = System.currentTimeMillis();

        IndicesStatsResponse indicesStats = indicesAdmin().prepareStats().get();
        final SearchStats.Stats suggest = indicesStats.getTotal().getSearch().getTotal();

        // check current
        assertThat(suggest.getSuggestCurrent(), equalTo(0L));

        // check suggest count
        assertThat(
            suggest.getSuggestCount(),
            equalTo((long) (suggestAllIdx * totalShards + suggestIdx1 * shardsIdx1 + suggestIdx2 * shardsIdx2))
        );
        assertThat(
            indicesStats.getIndices().get("test1").getTotal().getSearch().getTotal().getSuggestCount(),
            equalTo((long) ((suggestAllIdx + suggestIdx1) * shardsIdx1))
        );
        assertThat(
            indicesStats.getIndices().get("test2").getTotal().getSearch().getTotal().getSuggestCount(),
            equalTo((long) ((suggestAllIdx + suggestIdx2) * shardsIdx2))
        );

        logger.info("iter {}, iter1 {}, iter2 {}, {}", suggestAllIdx, suggestIdx1, suggestIdx2, endTime - startTime);
        // check suggest time
        assertThat(suggest.getSuggestTimeInMillis(), greaterThanOrEqualTo(0L));
        // the upperbound is num shards * total time since we do searches in parallel
        assertThat(suggest.getSuggestTimeInMillis(), lessThanOrEqualTo(totalShards * (endTime - startTime)));

        NodesStatsResponse nodeStats = clusterAdmin().prepareNodesStats().get();
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
        ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        List<ShardIterator> allAssignedShardsGrouped = state.routingTable().allAssignedShardsGrouped(indices, true);
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
        ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        List<?> allAssignedShardsGrouped = state.routingTable().allAssignedShardsGrouped(indices, true);
        return allAssignedShardsGrouped.size();
    }
}
