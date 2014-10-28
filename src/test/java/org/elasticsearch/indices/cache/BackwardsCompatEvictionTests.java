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

package org.elasticsearch.indices.cache;

import com.google.common.base.Predicate;
import org.apache.lucene.util.English;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.metrics.EvictionStats;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchBackwardsCompatIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ElasticsearchIntegrationTest.ClusterScope(scope= ElasticsearchIntegrationTest.Scope.TEST, minNumDataNodes=1, maxNumDataNodes = 3, numClientNodes = 0)
public class BackwardsCompatEvictionTests extends ElasticsearchBackwardsCompatIntegrationTest {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        // Set the fielddata and filter size to 1b and expire to 1ms, forces evictions immediately
        return  ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("indices.fielddata.cache.expire", "1ms")
                .put("indices.fielddata.cache.size", "1b")
                .put("indices.cache.filter.expire", "1ms")
                .put("indices.cache.filter.size", "1b")
                .build();
    }

    @Override
    protected Settings externalNodeSettings(int nodeOrdinal) {
        // Set the fielddata and filter size to 1b and expire to 1ms, forces evictions immediately
        return  ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("indices.fielddata.cache.expire", "1ms")
                .put("indices.fielddata.cache.size", "1b")
                .put("indices.cache.filter.expire", "1ms")
                .put("indices.cache.filter.size", "1b")
                .build();
    }

    @Override
    public Settings indexSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();

        int numberOfShards = between(5, 10);    // We need to override this so that all nodes have at least one shard
        if (numberOfShards > 0) {
            builder.put(SETTING_NUMBER_OF_SHARDS, numberOfShards).build();
        }
        int numberOfReplicas = numberOfReplicas();
        if (numberOfReplicas >= 0) {
            builder.put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas).build();
        }

        return builder.build();
    }


    @Test
    public void testFieldDataEvictions() throws Exception {
        createIndex("test");
        ensureGreen();

        NodesInfoResponse nodesInfo = client().admin().cluster().prepareNodesInfo().execute().actionGet();
        Map<String, NodeInfo> versions = nodesInfo.getNodesMap();

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("client.transport.ignore_cluster_name", true).build();

        // We explicitly connect to each node with a custom TransportClient
        for (NodeInfo n : nodesInfo.getNodes()) {
            TransportClient tc = new TransportClient(settings).addTransportAddress(n.getNode().address());
            NodesStatsResponse ns = tc.admin().cluster().prepareNodesStats().setIndices(true).execute().actionGet();

            // This is the version of the node we are talking to via Transport Client
            Version tcNodeVersion = versions.get(n.getNode().getId()).getVersion();

            for (NodeStats stats : ns.getNodes()) {

                // If the node we are talking to is 1.5.0+, it will have full responses
                if (tcNodeVersion.onOrAfter(Version.V_1_5_0)) {

                    EvictionStats ev = stats.getIndices().getFieldData().getEvictionStats();
                    Version nodeVersion = versions.get(stats.getNode().getId()).getVersion();

                    // If the node in the stats (not the node we are talking to) is 1.5.0+, it will have eviction rates
                    if (nodeVersion.onOrAfter(Version.V_1_5_0)) {
                        assertThat(ev.getEvictions(), equalTo(0l));
                        assertThat(ev.getEvictionsOneMinuteRate(), equalTo(0D));
                        assertThat(ev.getEvictionsFiveMinuteRate(), equalTo(0D));
                        assertThat(ev.getEvictionsFifteenMinuteRate(), equalTo(0D));
                    } else {
                        // Otherwise it will have negative one's for rates
                        assertThat(ev.getEvictions(), equalTo(0l));
                        assertThat(ev.getEvictionsOneMinuteRate(), equalTo(-1D));
                        assertThat(ev.getEvictionsFiveMinuteRate(), equalTo(-1D));
                        assertThat(ev.getEvictionsFifteenMinuteRate(), equalTo(-1D));
                    }
                } else {
                    // If the node we are talking to is < 1.3.0, it will only have eviction counts and no rates.
                    // But because this test code is executing in 1.5.0+, evictions will be present in response and negative
                    EvictionStats ev = stats.getIndices().getFieldData().getEvictionStats();
                    assertThat(ev.getEvictions(), equalTo(0l));
                    assertThat(ev.getEvictionsOneMinuteRate(), equalTo(-1D));
                    assertThat(ev.getEvictionsFiveMinuteRate(), equalTo(-1D));
                    assertThat(ev.getEvictionsFifteenMinuteRate(), equalTo(-1D));
                }
            }
            tc.close();
        }

        int numDocs = randomIntBetween(500, 5000);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test", "type1", String.valueOf(i)).setSource(
                    "field1", English.intToEnglish(i),
                    "field2", i
            );
        }

        indexRandom(true, docs);
        ensureGreen();

        // sort to load it to field data...run multiple queries to thrash the evictions
        for (int i = 0; i < 100; i++) {
            client().prepareSearch().addSort("field1", SortOrder.ASC).execute().actionGet();
            client().prepareSearch().addSort("field2", SortOrder.ASC).execute().actionGet();
        }

        // Just to give enough time for evictions to occur
        waitForEvictions(CacheType.FIELDDATA);

        // We explicitly connect to each node with a custom TransportClient
        for (NodeInfo n : nodesInfo.getNodes()) {
            TransportClient tc = new TransportClient(settings).addTransportAddress(n.getNode().address());
            NodesStatsResponse ns = tc.admin().cluster().prepareNodesStats().setIndices(true).execute().actionGet();

            // This is the version of the node we are talking to via Transport Client
            Version tcNodeVersion = versions.get(n.getNode().getId()).getVersion();

            for (NodeStats stats : ns.getNodes()) {
                boolean hasDocs = stats.getIndices().getDocs().getCount() > 0;

                // If the node we are talking to is 1.5.0+, it will have full responses
                if (tcNodeVersion.onOrAfter(Version.V_1_5_0)) {

                    EvictionStats ev = stats.getIndices().getFieldData().getEvictionStats();
                    Version nodeVersion = versions.get(stats.getNode().getId()).getVersion();

                    // If the node in the stats (not the node we are talking to) is 1.5.0+, it will have eviction rates
                    if (nodeVersion.onOrAfter(Version.V_1_5_0)) {
                        assertThat(ev.getEvictions(), hasDocs ? greaterThan(0l) : equalTo(0L));
                        assertThat(ev.getEvictionsOneMinuteRate(), hasDocs ? greaterThan(0D) : equalTo(0D));
                        assertThat(ev.getEvictionsFiveMinuteRate(), hasDocs ? greaterThan(0D) : equalTo(0D));
                        assertThat(ev.getEvictionsFifteenMinuteRate(), hasDocs ? greaterThan(0D) : equalTo(0D));
                    } else {
                        // Otherwise it will have negative one's for rates
                        assertThat(ev.getEvictions(), hasDocs ? greaterThan(0l) : equalTo(0L));
                        assertThat(ev.getEvictionsOneMinuteRate(), equalTo(-1D));
                        assertThat(ev.getEvictionsFiveMinuteRate(), equalTo(-1D));
                        assertThat(ev.getEvictionsFifteenMinuteRate(), equalTo(-1D));
                    }
                } else {
                    // If the node we are talking to is < 1.3.0, it will only have eviction counts and no rates.
                    // But because this test code is executing in 1.5.0+, evictions will be present in response and negative
                    EvictionStats ev = stats.getIndices().getFieldData().getEvictionStats();
                    assertThat(ev.getEvictions(), hasDocs ? greaterThan(0l) : equalTo(0L));
                    assertThat(ev.getEvictionsOneMinuteRate(), equalTo(-1D));
                    assertThat(ev.getEvictionsFiveMinuteRate(), equalTo(-1D));
                    assertThat(ev.getEvictionsFifteenMinuteRate(), equalTo(-1D));
                }
            }
            tc.close();
        }
    }

    @Test
    public void testFilterEvictions() throws Exception {
        createIndex("test");
        ensureGreen();

        NodesInfoResponse nodesInfo = client().admin().cluster().prepareNodesInfo().execute().actionGet();
        Map<String, NodeInfo> versions = nodesInfo.getNodesMap();

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("client.transport.ignore_cluster_name", true).build();

        // We explicitly connect to each node with a custom TransportClient
        for (NodeInfo n : nodesInfo.getNodes()) {
            TransportClient tc = new TransportClient(settings).addTransportAddress(n.getNode().address());
            NodesStatsResponse ns = tc.admin().cluster().prepareNodesStats().setIndices(true).execute().actionGet();

            // This is the version of the node we are talking to via Transport Client
            Version tcNodeVersion = versions.get(n.getNode().getId()).getVersion();

            for (NodeStats stats : ns.getNodes()) {

                // If the node we are talking to is 1.5.0+, it will have full responses
                if (tcNodeVersion.onOrAfter(Version.V_1_5_0)) {

                    EvictionStats ev = stats.getIndices().getFilterCache().getEvictionStats();
                    Version nodeVersion = versions.get(stats.getNode().getId()).getVersion();

                    // If the node in the stats (not the node we are talking to) is 1.5.0+, it will have eviction rates
                    if (nodeVersion.onOrAfter(Version.V_1_5_0)) {
                        assertThat(ev.getEvictions(), equalTo(0l));
                        assertThat(ev.getEvictionsOneMinuteRate(), equalTo(0D));
                        assertThat(ev.getEvictionsFiveMinuteRate(), equalTo(0D));
                        assertThat(ev.getEvictionsFifteenMinuteRate(), equalTo(0D));
                    } else {
                        // Otherwise it will have negative one's for rates
                        assertThat(ev.getEvictions(), equalTo(0l));
                        assertThat(ev.getEvictionsOneMinuteRate(), equalTo(-1D));
                        assertThat(ev.getEvictionsFiveMinuteRate(), equalTo(-1D));
                        assertThat(ev.getEvictionsFifteenMinuteRate(), equalTo(-1D));
                    }
                } else {
                    // If the node we are talking to is < 1.3.0, it will only have eviction counts and no rates.
                    // But because this test code is executing in 1.5.0+, evictions will be present in response and negative
                    EvictionStats ev = stats.getIndices().getFilterCache().getEvictionStats();
                    assertThat(ev.getEvictions(), equalTo(0l));
                    assertThat(ev.getEvictionsOneMinuteRate(), equalTo(-1D));
                    assertThat(ev.getEvictionsFiveMinuteRate(), equalTo(-1D));
                    assertThat(ev.getEvictionsFifteenMinuteRate(), equalTo(-1D));
                }
            }
            tc.close();
        }


        int numDocs = randomIntBetween(1000, 3000);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test", "type1", String.valueOf(i)).setSource(
                    "field1", English.intToEnglish(i),
                    "field2", i
            );
        }

        indexRandom(true, docs);
        ensureGreen();

        // Run some searches to cache and evict filters
        for (int i = 0; i < 2000; i++) {
            client().prepareSearch().setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), FilterBuilders.termFilter("field1", English.intToEnglish(i )))).execute().actionGet();
            client().prepareSearch().setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), FilterBuilders.termFilter("field2", i % numDocs))).execute().actionGet();
        }

        // Just to give enough time for evictions to occur
        waitForEvictions(CacheType.FILTER);

        // We explicitly connect to each node with a custom TransportClient
        for (NodeInfo n : nodesInfo.getNodes()) {
            TransportClient tc = new TransportClient(settings).addTransportAddress(n.getNode().address());
            NodesStatsResponse ns = tc.admin().cluster().prepareNodesStats().setIndices(true).execute().actionGet();

            // This is the version of the node we are talking to via Transport Client
            Version tcNodeVersion = versions.get(n.getNode().getId()).getVersion();

            for (NodeStats stats : ns.getNodes()) {
                boolean hasDocs = stats.getIndices().getDocs().getCount() > 0;

                // If the node we are talking to is 1.5.0+, it will have full responses
                if (tcNodeVersion.onOrAfter(Version.V_1_5_0)) {

                    EvictionStats ev = stats.getIndices().getFilterCache().getEvictionStats();
                    Version nodeVersion = versions.get(stats.getNode().getId()).getVersion();

                    // If the node in the stats (not the node we are talking to) is 1.5.0+, it will have eviction rates
                    if (nodeVersion.onOrAfter(Version.V_1_5_0)) {
                        assertThat(ev.getEvictions(), hasDocs ? greaterThan(0l) : equalTo(0L));
                        assertThat(ev.getEvictionsOneMinuteRate(), hasDocs ? greaterThan(0D) : equalTo(0D));
                        assertThat(ev.getEvictionsFiveMinuteRate(), hasDocs ? greaterThan(0D) : equalTo(0D));
                        assertThat(ev.getEvictionsFifteenMinuteRate(), hasDocs ? greaterThan(0D) : equalTo(0D));
                    } else {
                        // Otherwise it will have negative one's for rates
                        assertThat(ev.getEvictions(), hasDocs ? greaterThan(0l) : equalTo(0L));
                        assertThat(ev.getEvictionsOneMinuteRate(), equalTo(-1D));
                        assertThat(ev.getEvictionsFiveMinuteRate(), equalTo(-1D));
                        assertThat(ev.getEvictionsFifteenMinuteRate(), equalTo(-1D));
                    }
                } else {
                    // If the node we are talking to is < 1.3.0, it will only have eviction counts and no rates.
                    // But because this test code is executing in 1.5.0+, evictions will be present in response and negative
                    EvictionStats ev = stats.getIndices().getFilterCache().getEvictionStats();
                    assertThat(ev.getEvictions(), hasDocs ? greaterThan(0l) : equalTo(0L));
                    assertThat(ev.getEvictionsOneMinuteRate(), equalTo(-1D));
                    assertThat(ev.getEvictionsFiveMinuteRate(), equalTo(-1D));
                    assertThat(ev.getEvictionsFifteenMinuteRate(), equalTo(-1D));
                }
            }
            tc.close();
        }
    }

    private enum CacheType {
        FILTER, FIELDDATA
    }

    private void waitForEvictions(final CacheType cacheType) throws Exception {
        assertTrue(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                boolean success = true;
                NodesStatsResponse nodesStats = client().admin().cluster().prepareNodesStats().setIndices(true).execute().actionGet();
                EvictionStats ev;
                for (NodeStats n : nodesStats.getNodes()) {
                    if (cacheType == CacheType.FILTER) {
                        ev = n.getIndices().getFilterCache().getEvictionStats();
                    } else {
                        ev = n.getIndices().getFieldData().getEvictionStats();
                    }

                    if (ev.getEvictions() <= 0 || ev.getEvictionsOneMinuteRate() == 0) {
                        success = false;
                        break;
                    }
                }

                return success;
            }
        }, 10, TimeUnit.SECONDS));
    }
}
