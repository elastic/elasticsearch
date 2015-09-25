/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.cluster;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsNodes;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStatsCollector;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;

public class ClusterStatsIT extends MarvelIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MarvelSettings.INTERVAL, "3s")
                .put(MarvelSettings.COLLECTORS, ClusterStatsCollector.NAME)
                .build();
    }

    @Test
    public void testClusterStats() throws Exception {
        ensureGreen();

        logger.debug("--> creating some indices so that every data nodes will at leats a shard");
        ClusterStatsNodes.Counts counts = client().admin().cluster().prepareClusterStats().get().getNodesStats().getCounts();
        assertThat(counts.getTotal(), greaterThan(0));

        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            assertAcked(prepareCreate("test-" + i).setSettings(Settings.settingsBuilder()
                    .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, counts.getDataOnly() + counts.getMasterData())
                    .build()));
            index("test-" + i, "foo", "1", jsonBuilder().startObject().field("dummy_field", 1).endObject());
        }

        securedFlush();
        securedRefresh();

        logger.debug("--> waiting for cluster stats to report data for each node");
        assertBusy(new Runnable() {
            @Override
            public void run() {
                NodesInfoResponse nodesInfoResponse = client().admin().cluster().prepareNodesInfo().get();
                for (Map.Entry<String, NodeInfo> node : nodesInfoResponse.getNodesMap().entrySet()) {
                    // Checks that node has shards
                    ClusterStatsResponse clusterStatsResponse = client().admin().cluster().prepareClusterStats().setNodesIds(node.getKey()).get();
                    assertNotNull(clusterStatsResponse.getIndicesStats().getShards());
                    assertNotNull(clusterStatsResponse.getIndicesStats().getShards());
                    assertThat(clusterStatsResponse.getIndicesStats().getShards().getTotal(), greaterThan(0));

                    NodesStatsResponse nodeStatsResponse = client().admin().cluster().prepareNodesStats(node.getKey()).setFs(true).get();
                    for (NodeStats nodeStats : nodeStatsResponse) {
                        assertThat(nodeStats.getFs().total().getAvailable().bytes(), greaterThan(-1L));
                    }
                }
            }
        }, 30L, TimeUnit.SECONDS);

        logger.debug("--> delete all indices in case of cluster stats documents have been indexed with no shards data");
        deleteMarvelIndices();

        awaitMarvelDocsCount(greaterThan(0L), ClusterStatsCollector.TYPE);

        logger.debug("--> searching for marvel documents of type [{}]", ClusterStatsCollector.TYPE);
        SearchResponse response = client().prepareSearch().setTypes(ClusterStatsCollector.TYPE).get();
        assertThat(response.getHits().getTotalHits(), greaterThan(0L));

        logger.debug("--> checking that every document contains the expected fields");
        String[] filters = ClusterStatsRenderer.FILTERS;
        for (SearchHit searchHit : response.getHits().getHits()) {
            Map<String, Object> fields = searchHit.sourceAsMap();

            for (String filter : filters) {
                assertContains(filter, fields);
            }
        }

        logger.debug("--> cluster stats successfully collected");
    }
}
