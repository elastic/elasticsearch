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

package org.elasticsearch.index.suggest.stats;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.suggest.SuggestRequestBuilder;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.hamcrest.Matchers.*;

/**
 */
@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SuggestStatsIT extends ESIntegTestCase {

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
        final int totalShards = shardsIdx1 + shardsIdx2;
        assertThat(numNodes, lessThanOrEqualTo(totalShards));
        assertAcked(prepareCreate("test1").setSettings(Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, shardsIdx1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0))
                .addMapping("type", "f", "type=string"));
        assertAcked(prepareCreate("test2").setSettings(Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, shardsIdx2)
                .put(SETTING_NUMBER_OF_REPLICAS, 0))
                .addMapping("type", "f", "type=string"));
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
            SuggestResponse suggestResponse = addSuggestions(internalCluster().clientNodeClient().prepareSuggest(), i).get();
            assertAllSuccessful(suggestResponse);
        }
        for (int i = 0; i < suggestIdx1; i++) {
            SuggestResponse suggestResponse = addSuggestions(internalCluster().clientNodeClient().prepareSuggest("test1"), i).get();
            assertAllSuccessful(suggestResponse);
        }
        for (int i = 0; i < suggestIdx2; i++) {
            SuggestResponse suggestResponse = addSuggestions(internalCluster().clientNodeClient().prepareSuggest("test2"), i).get();
            assertAllSuccessful(suggestResponse);
        }
        long endTime = System.currentTimeMillis();

        IndicesStatsResponse indicesStats = client().admin().indices().prepareStats().execute().actionGet();

        // check current
        assertThat(indicesStats.getTotal().getSuggest().getCurrent(), equalTo(0l));

        // check suggest count
        assertThat(indicesStats.getTotal().getSuggest().getCount(), equalTo((long) (suggestAllIdx * totalShards + suggestIdx1 * shardsIdx1 + suggestIdx2 * shardsIdx2)));
        assertThat(indicesStats.getIndices().get("test1").getTotal().getSuggest().getCount(), equalTo((long) ((suggestAllIdx + suggestIdx1) * shardsIdx1)));
        assertThat(indicesStats.getIndices().get("test2").getTotal().getSuggest().getCount(), equalTo((long) ((suggestAllIdx + suggestIdx2) * shardsIdx2)));

        logger.info("iter {}, iter1 {}, iter2 {}, {}", suggestAllIdx, suggestIdx1, suggestIdx2, endTime - startTime);
        // check suggest time
        assertThat(indicesStats.getTotal().getSuggest().getTimeInMillis(), greaterThan(0l));
        // the upperbound is num shards * total time since we do searches in parallel
        assertThat(indicesStats.getTotal().getSuggest().getTimeInMillis(), lessThanOrEqualTo(totalShards * (endTime - startTime)));

        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().execute().actionGet();
        NodeStats[] nodes = nodeStats.getNodes();
        Set<String> nodeIdsWithIndex = nodeIdsWithIndex("test1", "test2");
        int num = 0;
        for (NodeStats stat : nodes) {
            SuggestStats suggestStats = stat.getIndices().getSuggest();
            logger.info("evaluating {}", stat.getNode());
            if (nodeIdsWithIndex.contains(stat.getNode().getId())) {
                assertThat(suggestStats.getCount(), greaterThan(0l));
                assertThat(suggestStats.getTimeInMillis(), greaterThan(0l));
                num++;
            } else {
                assertThat(suggestStats.getCount(), equalTo(0l));
                assertThat(suggestStats.getTimeInMillis(), equalTo(0l));
            }
        }

        assertThat(num, greaterThan(0));

    }

    private SuggestRequestBuilder addSuggestions(SuggestRequestBuilder request, int i) {
        for (int s = 0; s < randomIntBetween(2, 10); s++) {
            if (randomBoolean()) {
                request.addSuggestion(new PhraseSuggestionBuilder("s" + s).field("f").text("test" + i + " test" + (i - 1)));
            } else {
                request.addSuggestion(new TermSuggestionBuilder("s" + s).field("f").text("test" + i));
            }
        }
        return request;
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


    protected int numAssignedShards(String... indices) {
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        GroupShardsIterator allAssignedShardsGrouped = state.routingTable().allAssignedShardsGrouped(indices, true);
        return allAssignedShardsGrouped.size();
    }
}
