/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.engine.ThreadPoolMergeScheduler;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

public class StatelessMergeIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ThreadPoolMergeScheduler.MERGE_THREAD_POOL_SCHEDULER.getKey(), true);
    }

    public void testMergesUseTheMergeThreadPool() {
        String indexNode = startMasterAndIndexNode();
        startSearchNode();

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());

        final int minMerges = randomIntBetween(1, 5);
        long totalDocs = 0;
        while (true) {
            int docs = randomIntBetween(100, 200);
            totalDocs += docs;
            indexDocs(indexName, docs);
            flush(indexName);

            var mergesResponse = client().admin().indices().prepareStats(indexName).clear().setMerge(true).get();
            var primaries = mergesResponse.getIndices().get(indexName).getPrimaries();
            if (primaries.merge.getTotal() >= minMerges) {
                break;
            }
        }

        forceMerge();
        refresh(indexName);

        final long expectedTotalDocs = totalDocs;
        assertHitCount(prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setTrackTotalHits(true), expectedTotalDocs);

        IndicesStatsResponse indicesStats = client().admin().indices().prepareStats(indexName).setMerge(true).get();
        long mergeCount = indicesStats.getIndices().get(indexName).getPrimaries().merge.getTotal();
        NodesStatsResponse nodesStatsResponse = client().admin().cluster().prepareNodesStats(indexNode).setThreadPool(true).get();
        assertThat(nodesStatsResponse.getNodes().size(), equalTo(1));
        NodeStats nodeStats = nodesStatsResponse.getNodes().get(0);
        assertThat(
            mergeCount,
            equalTo(
                nodeStats.getThreadPool()
                    .stats()
                    .stream()
                    .filter(s -> Stateless.MERGE_THREAD_POOL.equals(s.name()))
                    .findAny()
                    .get()
                    .completed()
            )
        );
    }
}
