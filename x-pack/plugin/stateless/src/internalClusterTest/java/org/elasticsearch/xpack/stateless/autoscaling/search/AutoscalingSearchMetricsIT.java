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

package co.elastic.elasticsearch.stateless.autoscaling.search;

import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;
import co.elastic.elasticsearch.stateless.AbstractServerlessStatelessPluginIntegTestCase;
import co.elastic.elasticsearch.stateless.api.ShardSizeStatsReader;
import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;
import co.elastic.elasticsearch.stateless.commits.HollowShardsService;
import co.elastic.elasticsearch.stateless.engine.HollowIndexEngine;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;

import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_TTL;
import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class AutoscalingSearchMetricsIT extends AbstractServerlessStatelessPluginIntegTestCase {

    private static final long DEFAULT_BOOST_WINDOW = TimeValue.timeValueDays(7).millis();
    private static final long ONE_DAY = TimeValue.timeValueDays(1).millis();

    public void testSearchTierMetricsInteractiveMetrics() throws Exception {

        startMasterOnlyNode();
        startIndexNode();
        startSearchNode(
            Settings.builder().put(SearchShardSizeCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1)).build()
        );

        var searchMetricsService = internalCluster().getCurrentMasterNodeInstance(SearchMetricsService.class);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());

        // new documents should count towards non-interactive part
        var now = System.currentTimeMillis();
        var boostWindow = now - DEFAULT_BOOST_WINDOW;
        indexDocumentsWithTimestamp(
            indexName,
            randomIntBetween(1, 100),
            boostWindow + ONE_DAY /* +1d to ensure docs are not leaving boost window during test run*/,
            now
        );
        refresh(indexName);
        assertBusy(() -> {
            var metrics = searchMetricsService.getSearchTierMetrics();
            assertThat(metrics.getMaxShardCopies(), equalTo(new MaxShardCopies(1, MetricQuality.EXACT)));
            assertThat(metrics.getStorageMetrics().totalInteractiveDataSizeInBytes(), greaterThan(0L));
            assertThat(metrics.getStorageMetrics().totalDataSizeInBytes(), greaterThan(0L));
        });
    }

    public void testSearchTierMetricsNonInteractiveMetrics() throws Exception {

        startMasterOnlyNode();
        startIndexNode();
        startSearchNode(
            Settings.builder().put(SearchShardSizeCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1)).build()
        );

        var searchMetricsService = internalCluster().getCurrentMasterNodeInstance(SearchMetricsService.class);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());

        // old documents should count towards non-interactive part
        var now = System.currentTimeMillis();
        var boostWindow = now - DEFAULT_BOOST_WINDOW;
        indexDocumentsWithTimestamp(indexName, randomIntBetween(1, 100), 1L, boostWindow - 1L);
        refresh(indexName);
        assertBusy(() -> {
            var metrics = searchMetricsService.getSearchTierMetrics();
            assertThat(metrics.getMaxShardCopies(), equalTo(new MaxShardCopies(1, MetricQuality.EXACT)));
            assertThat(metrics.getStorageMetrics().totalInteractiveDataSizeInBytes(), equalTo(0L));
            assertThat(metrics.getStorageMetrics().totalDataSizeInBytes(), greaterThan(0L));
        });
    }

    public void testSearchTierMetricsAfterChangingBoostWindow() throws Exception {

        startMasterOnlyNode();
        startIndexNode();
        startSearchNode(
            Settings.builder().put(SearchShardSizeCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1)).build()
        );

        var searchMetricsService = internalCluster().getCurrentMasterNodeInstance(SearchMetricsService.class);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());
        var now = System.currentTimeMillis();
        var boostWindow = now - DEFAULT_BOOST_WINDOW;
        var extendedBoostWindow = now - 2 * DEFAULT_BOOST_WINDOW;
        indexDocumentsWithTimestamp(
            indexName,
            randomIntBetween(1, 100),
            extendedBoostWindow + ONE_DAY /* +1d to ensure docs are not leaving extended boost window during test run*/,
            boostWindow
        );
        refresh(indexName);

        assertBusy(() -> {
            var metrics = searchMetricsService.getSearchTierMetrics();
            assertThat(metrics.getMaxShardCopies(), equalTo(new MaxShardCopies(1, MetricQuality.EXACT)));
            assertThat(metrics.getStorageMetrics().totalInteractiveDataSizeInBytes(), equalTo(0L));
            assertThat(metrics.getStorageMetrics().totalDataSizeInBytes(), greaterThan(0L));
        });

        // extend boost window to 2 weeks
        updateClusterSettings(Settings.builder().put(ServerlessSharedSettings.BOOST_WINDOW_SETTING.getKey(), TimeValue.timeValueDays(14)));

        assertBusy(() -> {
            var metrics = searchMetricsService.getSearchTierMetrics();
            assertThat(metrics.getMaxShardCopies(), equalTo(new MaxShardCopies(1, MetricQuality.EXACT)));
            assertThat(metrics.getStorageMetrics().totalInteractiveDataSizeInBytes(), greaterThan(0L));
            assertThat(metrics.getStorageMetrics().totalDataSizeInBytes(), greaterThan(0L));
        });
    }

    public void testIndicesWithZeroReplicasAreNotTakenIntoAccount() throws Exception {
        var masterNode = startMasterNode();
        startMasterNode();
        startIndexNode();
        startSearchNode(
            Settings.builder().put(SearchShardSizeCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1)).build()
        );

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());

        var now = System.currentTimeMillis();
        var boostWindow = now - DEFAULT_BOOST_WINDOW;
        indexDocumentsWithTimestamp(
            indexName,
            randomIntBetween(1, 100),
            boostWindow + ONE_DAY /* +1d to ensure docs are not leaving boost window during test run*/,
            now
        );
        refresh(indexName);
        assertBusy(() -> {
            var searchMetricsService = internalCluster().getCurrentMasterNodeInstance(SearchMetricsService.class);
            var metrics = searchMetricsService.getSearchTierMetrics();
            assertThat(metrics.getMaxShardCopies(), equalTo(new MaxShardCopies(0, MetricQuality.EXACT)));
            assertThat(metrics.getStorageMetrics().quality(), is(equalTo(MetricQuality.EXACT)));
        });

        internalCluster().stopNode(masterNode);
        ensureStableCluster(3);

        assertBusy(() -> {
            var searchMetricsService = internalCluster().getCurrentMasterNodeInstance(SearchMetricsService.class);
            var metrics = searchMetricsService.getSearchTierMetrics();
            assertThat(metrics.getMaxShardCopies(), equalTo(new MaxShardCopies(0, MetricQuality.EXACT)));
            assertThat(metrics.getStorageMetrics().quality(), is(equalTo(MetricQuality.EXACT)));
        });

        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);

        assertBusy(() -> {
            var searchMetricsService = internalCluster().getCurrentMasterNodeInstance(SearchMetricsService.class);
            var metrics = searchMetricsService.getSearchTierMetrics();
            assertThat(metrics.getMaxShardCopies(), equalTo(new MaxShardCopies(1, MetricQuality.EXACT)));
            assertThat(metrics.getStorageMetrics().quality(), is(equalTo(MetricQuality.EXACT)));
            assertThat(metrics.getStorageMetrics().totalInteractiveDataSizeInBytes(), greaterThan(0L));
            assertThat(metrics.getStorageMetrics().totalDataSizeInBytes(), greaterThan(0L));
        });
    }

    public void testIndicesWithUpdatedReplicasAreTakenIntoAccount() throws Exception {
        startMasterNode();
        startIndexNode();
        startSearchNode(
            Settings.builder().put(SearchShardSizeCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1)).build()
        );

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());

        var now = System.currentTimeMillis();
        var boostWindow = now - DEFAULT_BOOST_WINDOW;
        indexDocumentsWithTimestamp(
            indexName,
            randomIntBetween(1, 100),
            boostWindow + ONE_DAY /* +1d to ensure docs are not leaving boost window during test run*/,
            now
        );
        refresh(indexName);
        assertBusy(() -> {
            var searchMetricsService = internalCluster().getCurrentMasterNodeInstance(SearchMetricsService.class);
            var metrics = searchMetricsService.getSearchTierMetrics();
            assertThat(metrics.getMaxShardCopies(), equalTo(new MaxShardCopies(1, MetricQuality.EXACT)));
            assertThat(metrics.getStorageMetrics().quality(), is(equalTo(MetricQuality.EXACT)));
            assertThat(metrics.getStorageMetrics().totalInteractiveDataSizeInBytes(), greaterThan(0L));
            assertThat(metrics.getStorageMetrics().totalDataSizeInBytes(), greaterThan(0L));
        });

        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0), indexName);

        assertBusy(() -> {
            var searchMetricsService = internalCluster().getCurrentMasterNodeInstance(SearchMetricsService.class);
            var metrics = searchMetricsService.getSearchTierMetrics();
            assertThat(metrics.getMaxShardCopies(), equalTo(new MaxShardCopies(0, MetricQuality.EXACT)));
            assertThat(metrics.getStorageMetrics().quality(), is(equalTo(MetricQuality.EXACT)));
        });
    }

    public void testShardSizesForHollowShards() throws Exception {
        var masterNode = startMasterNode();
        String searchNode = startSearchNode(
            Settings.builder().put(SearchShardSizeCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1)).build()
        );
        var indexNodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), true)
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .build();
        String indexNodeA = startIndexNode(indexNodeSettings);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        var now = System.currentTimeMillis();
        var boostWindow = now - DEFAULT_BOOST_WINDOW;
        indexDocumentsWithTimestamp(indexName, randomIntBetween(1, 100), boostWindow + ONE_DAY, now);
        refresh(indexName);
        assertBusy(() -> {
            var searchMetricsService = internalCluster().getCurrentMasterNodeInstance(SearchMetricsService.class);
            var metrics = searchMetricsService.getSearchTierMetrics();
            assertThat(metrics.getMaxShardCopies(), equalTo(new MaxShardCopies(1, MetricQuality.EXACT)));
            assertThat(metrics.getStorageMetrics().totalInteractiveDataSizeInBytes(), greaterThan(0L));
        });
        var shardSizeBeforeHollowing = getShardSize(new ShardId(resolveIndex(indexName), 0));

        flush(indexName);
        assertBusy(() -> {
            var indexShard = findIndexShard(resolveIndex(indexName), 0);
            assertThat(indexShard.getEngineOrNull(), instanceOf(IndexEngine.class));
            assertTrue(internalCluster().getInstance(HollowShardsService.class, indexNodeA).isHollowableIndexShard(indexShard));
        });
        String indexNodeB = startIndexNode(indexNodeSettings);
        hollowShards(indexName, 1, indexNodeA, indexNodeB);

        var indexShard = findIndexShard(resolveIndex(indexName), 0);
        assertThat(indexShard.getEngineOrNull(), instanceOf(HollowIndexEngine.class));

        // Restarting the master node to update shard size cache, to reflect the hollow information which is later asserted.
        internalCluster().restartNode(masterNode);
        ensureGreen(indexName);
        if (randomBoolean()) {
            internalCluster().restartNode(searchNode);
            ensureGreen(indexName);
        }

        var shardSize = getShardSize(indexShard.shardId());
        assertThat(shardSize.totalSizeInBytes(), equalTo(shardSizeBeforeHollowing.totalSizeInBytes()));
        assertThat(shardSize.generation(), equalTo(shardSizeBeforeHollowing.generation() + 1));
    }

    private static ShardSizeStatsReader.ShardSize getShardSize(ShardId shardId) {
        var searchMetricsService = internalCluster().getCurrentMasterNodeInstance(SearchMetricsService.class);
        var shardMetrics = searchMetricsService.getShardMetrics().get(shardId);
        return shardMetrics.shardSize;
    }

    private String startMasterNode() {
        return internalCluster().startMasterOnlyNode(
            nodeSettings().put(StoreHeartbeatService.MAX_MISSED_HEARTBEATS.getKey(), 1)
                .put(StoreHeartbeatService.HEARTBEAT_FREQUENCY.getKey(), TimeValue.timeValueSeconds(1))
                .build()
        );
    }

    private void indexDocumentsWithTimestamp(String indexName, int numDocs, long minTimestamp, long maxTimestamp) {
        var bulkRequest = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(
                new IndexRequest(indexName).source(DataStream.TIMESTAMP_FIELD_NAME, randomLongBetween(minTimestamp, maxTimestamp))
            );
        }
        assertNoFailures(bulkRequest.get());
    }
}
