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

import co.elastic.elasticsearch.stateless.engine.MergeMetrics;
import co.elastic.elasticsearch.stateless.engine.ThreadPoolMergeScheduler;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class StatelessMergeIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestTelemetryPlugin.class);
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ThreadPoolMergeScheduler.MERGE_THREAD_POOL_SCHEDULER.getKey(), true);
    }

    public void testMergesUseTheMergeThreadPool() {
        String indexNode = startMasterAndIndexNode();
        startSearchNode();

        final String indexName = randomIdentifier();
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

    public void testMergeMetricsPublication() {
        String indexNode = startMasterAndIndexNode();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());

        ensureGreen(indexName);

        int numDocs = randomIntBetween(100, 1000);
        indexDocs(indexName, numDocs);
        refresh(indexName);

        var plugin = internalCluster().getInstance(PluginsService.class, indexNode)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();

        for (int i = 0; i < 4; ++i) {
            indexDocs(indexName, randomIntBetween(100, 200));
            refresh(indexName);
        }

        forceMerge();

        List<Measurement> docs = plugin.getLongCounterMeasurement(MergeMetrics.MERGE_DOCS_TOTAL);
        List<Measurement> bytes = plugin.getLongCounterMeasurement(MergeMetrics.MERGE_SEGMENTS_SIZE);
        List<Measurement> time = plugin.getLongHistogramMeasurement(MergeMetrics.MERGE_TIME_IN_MILLIS);

        assertThat(bytes.size(), equalTo(docs.size()));
        assertThat(time.size(), equalTo(docs.size()));

        // Will likely have 1 merge, but could have more
        for (int i = 0; i < docs.size(); ++i) {
            var mergeDocs = docs.get(i);
            var mergeBytes = bytes.get(i);
            var mergeTime = time.get(i);

            assertThat(mergeDocs.getLong(), greaterThan(0L));
            assertThat(mergeBytes.getLong(), greaterThan(0L));
            assertThat(mergeTime.getLong(), greaterThan(0L));

            assertThat(mergeDocs.attributes().get("index_name"), equalTo(indexName));
            assertThat(mergeBytes.attributes().get("index_name"), equalTo(indexName));
            assertThat(mergeTime.attributes().get("index_name"), equalTo(indexName));

            assertThat(mergeDocs.attributes().get("shard_id"), equalTo(0));
            assertThat(mergeBytes.attributes().get("shard_id"), equalTo(0));
            assertThat(mergeTime.attributes().get("shard_id"), equalTo(0));

            Object mergeId = mergeDocs.attributes().get("merge_id");
            assertThat(mergeBytes.attributes().get("merge_id"), equalTo(mergeId));
            assertThat(mergeTime.attributes().get("merge_id"), equalTo(mergeId));
        }

        long totalDocs = docs.stream().mapToLong(Measurement::getLong).sum();
        long totalBytes = bytes.stream().mapToLong(Measurement::getLong).sum();

        IndicesStatsResponse statsResponse = client().admin().indices().prepareStats(indexName).setMerge(true).get();
        MergeStats mergeStats = statsResponse.getIndex(indexName).getShards()[0].getStats().merge;
        assertThat(totalDocs, equalTo(mergeStats.getTotalNumDocs()));
        assertThat(totalBytes, equalTo(mergeStats.getTotalSize().getBytes()));
    }

    private static Measurement getSingleRecordedMetric(Function<String, List<Measurement>> metricGetter, String name) {
        final List<Measurement> measurements = metricGetter.apply(name);
        assertFalse("Metric is not recorded", measurements.isEmpty());
        assertThat(measurements.size(), equalTo(1));
        return measurements.get(0);
    }
}
