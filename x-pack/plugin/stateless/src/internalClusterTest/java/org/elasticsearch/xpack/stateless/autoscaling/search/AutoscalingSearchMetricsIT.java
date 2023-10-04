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
import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class AutoscalingSearchMetricsIT extends AbstractStatelessIntegTestCase {

    private static final long DEFAULT_BOOST_WINDOW = TimeValue.timeValueDays(7).millis();
    private static final long ONE_DAY = TimeValue.timeValueDays(1).millis();

    public void testSearchTierMetricsInteractiveMetrics() throws Exception {

        startMasterOnlyNode();
        startIndexNode();
        startSearchNode(Settings.builder().put(ShardSizesCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1)).build());

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
        startSearchNode(Settings.builder().put(ShardSizesCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1)).build());

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
        startSearchNode(Settings.builder().put(ShardSizesCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1)).build());

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
        startSearchNode(Settings.builder().put(ShardSizesCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1)).build());

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
        startSearchNode(Settings.builder().put(ShardSizesCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1)).build());

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
