/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.stats;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.search.stats.SearchStats.Stats;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SearchStatsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap("_source.field", vars -> {
                Map<?, ?> src = (Map) vars.get("_source");
                return src.get("field");
            });
        }
    }

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    public void testSimpleStats() throws Exception {
        // clear all stats first
        client().admin().indices().prepareStats().clear().get();
        final int numNodes = cluster().numDataNodes();
        assertThat(numNodes, greaterThanOrEqualTo(2));
        final int shardsIdx1 = randomIntBetween(1, 10); // we make sure each node gets at least a single shard...
        final int shardsIdx2 = Math.max(numNodes - shardsIdx1, randomIntBetween(1, 10));
        assertThat(numNodes, lessThanOrEqualTo(shardsIdx1 + shardsIdx2));
        assertAcked(prepareCreate("test1").setSettings(Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, shardsIdx1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)));
        int docsTest1 = scaledRandomIntBetween(3*shardsIdx1, 5*shardsIdx1);
        for (int i = 0; i < docsTest1; i++) {
            client().prepareIndex("test1").setId(Integer.toString(i)).setSource("field", "value").get();
            if (rarely()) {
                refresh();
            }
        }
        assertAcked(prepareCreate("test2").setSettings(Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, shardsIdx2)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)));
        int docsTest2 = scaledRandomIntBetween(3*shardsIdx2, 5*shardsIdx2);
        for (int i = 0; i < docsTest2; i++) {
            client().prepareIndex("test2").setId(Integer.toString(i)).setSource("field", "value").get();
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
            SearchResponse searchResponse = internalCluster().coordOnlyNodeClient().prepareSearch()
                    .setQuery(QueryBuilders.termQuery("field", "value")).setStats("group1", "group2")
                    .highlighter(new HighlightBuilder().field("field"))
                    .addScriptField("script1",
                        new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_source.field", Collections.emptyMap()))
                    .setSize(100)
                    .get();
            assertHitCount(searchResponse, docsTest1 + docsTest2);
            assertAllSuccessful(searchResponse);
        }

        IndicesStatsResponse indicesStats = client().admin().indices().prepareStats().get();
        logger.debug("###### indices search stats: {}", indicesStats.getTotal().getSearch());
        assertThat(indicesStats.getTotal().getSearch().getTotal().getQueryCount(), greaterThan(0L));
        assertThat(indicesStats.getTotal().getSearch().getTotal().getQueryTimeInMillis(), greaterThan(0L));
        assertThat(indicesStats.getTotal().getSearch().getTotal().getFetchCount(), greaterThan(0L));
        assertThat(indicesStats.getTotal().getSearch().getTotal().getFetchTimeInMillis(), greaterThan(0L));
        assertThat(indicesStats.getTotal().getSearch().getGroupStats(), nullValue());

        indicesStats = client().admin().indices().prepareStats().setGroups("group1").get();
        assertThat(indicesStats.getTotal().getSearch().getGroupStats(), notNullValue());
        assertThat(indicesStats.getTotal().getSearch().getGroupStats().get("group1").getQueryCount(), greaterThan(0L));
        assertThat(indicesStats.getTotal().getSearch().getGroupStats().get("group1").getQueryTimeInMillis(), greaterThan(0L));
        assertThat(indicesStats.getTotal().getSearch().getGroupStats().get("group1").getFetchCount(), greaterThan(0L));
        assertThat(indicesStats.getTotal().getSearch().getGroupStats().get("group1").getFetchTimeInMillis(), greaterThan(0L));
        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().get();

        Set<String> nodeIdsWithIndex = nodeIdsWithIndex("test1", "test2");
        int num = 0;
        for (NodeStats stat : nodeStats.getNodes()) {
            Stats total = stat.getIndices().getSearch().getTotal();
            if (nodeIdsWithIndex.contains(stat.getNode().getId())) {
                assertThat(total.getQueryCount(), greaterThan(0L));
                assertThat(total.getQueryTimeInMillis(), greaterThan(0L));
                num++;
            } else {
                assertThat(total.getQueryCount(), equalTo(0L));
                assertThat(total.getQueryTimeInMillis(), equalTo(0L));
            }
        }

        assertThat(num, greaterThan(0));

    }

    private Set<String> nodeIdsWithIndex(String... indices) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
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

    public void testOpenContexts() {
        String index = "test1";
        createIndex(index);
        ensureGreen(index);

        // create shards * docs number of docs and attempt to distribute them equally
        // this distribution will not be perfect; each shard will have an integer multiple of docs (possibly zero)
        // we do this so we have a lot of pages to scroll through
        final int docs = scaledRandomIntBetween(20, 50);
        for (int s = 0; s < numAssignedShards(index); s++) {
            for (int i = 0; i < docs; i++) {
                client()
                        .prepareIndex(index).setId(Integer.toString(s * docs + i))
                        .setSource("field", "value")
                        .setRouting(Integer.toString(s))
                        .get();
            }
        }
        client().admin().indices().prepareRefresh(index).get();

        IndicesStatsResponse indicesStats = client().admin().indices().prepareStats(index).get();
        assertThat(indicesStats.getTotal().getSearch().getOpenContexts(), equalTo(0L));

        int size = scaledRandomIntBetween(1, docs);
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(size)
                .setScroll(TimeValue.timeValueMinutes(2))
                .get();
        assertSearchResponse(searchResponse);

        // refresh the stats now that scroll contexts are opened
        indicesStats = client().admin().indices().prepareStats(index).get();

        assertThat(indicesStats.getTotal().getSearch().getOpenContexts(), equalTo((long) numAssignedShards(index)));
        assertThat(indicesStats.getTotal().getSearch().getTotal().getScrollCurrent(), equalTo((long) numAssignedShards(index)));

        int hits = 0;
        while (true) {
            if (searchResponse.getHits().getHits().length == 0) {
                break;
            }
            hits += searchResponse.getHits().getHits().length;
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(2))
                    .get();
        }
        long expected = 0;

        // the number of queries executed is equal to at least the sum of number of pages in shard over all shards
        IndicesStatsResponse r = client().admin().indices().prepareStats(index).get();
        for (int s = 0; s < numAssignedShards(index); s++) {
            expected += (long)Math.ceil(r.getShards()[s].getStats().getDocs().getCount() / size);
        }
        indicesStats = client().admin().indices().prepareStats().get();
        Stats stats = indicesStats.getTotal().getSearch().getTotal();
        assertEquals(hits, docs * numAssignedShards(index));
        assertThat(stats.getQueryCount(), greaterThanOrEqualTo(expected));

        clearScroll(searchResponse.getScrollId());

        indicesStats = client().admin().indices().prepareStats().get();
        stats = indicesStats.getTotal().getSearch().getTotal();
        assertThat(indicesStats.getTotal().getSearch().getOpenContexts(), equalTo(0L));
        assertThat(stats.getScrollCount(), equalTo((long)numAssignedShards(index)));
        assertThat(stats.getScrollTimeInMillis(), greaterThan(0L));
    }

    protected int numAssignedShards(String... indices) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        GroupShardsIterator allAssignedShardsGrouped = state.routingTable().allAssignedShardsGrouped(indices, true);
        return allAssignedShardsGrouped.size();
    }
}
