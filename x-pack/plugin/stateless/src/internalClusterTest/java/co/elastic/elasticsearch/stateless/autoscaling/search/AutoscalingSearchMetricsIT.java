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

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class AutoscalingSearchMetricsIT extends AbstractStatelessIntegTestCase {

    private static final long BOOST_WINDOW = TimeValue.timeValueDays(7).millis();

    public void testSearchTierMetricsInteractiveMetrics() throws Exception {

        startMasterOnlyNode();
        startIndexNode();
        startSearchNode(Settings.builder().put(ShardSizesCollector.PUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1)).build());

        var searchMetricsService = internalCluster().getCurrentMasterNodeInstance(SearchMetricsService.class);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());

        // new documents should count towards non-interactive part
        indexDocumentsWithTimestamp(
            indexName,
            randomIntBetween(1, 100),
            // ensure docs are not propagated to noninteractive during the test runtime
            System.currentTimeMillis() - BOOST_WINDOW + TimeValue.timeValueDays(1).millis(),
            System.currentTimeMillis()
        );
        refresh(indexName);
        assertBusy(() -> {
            var metrics = searchMetricsService.getSearchTierMetrics();
            assertThat(metrics.maxShardCopies(), equalTo(new MaxShardCopies(1, MetricQuality.EXACT)));
            assertThat(metrics.storageMetrics().totalInteractiveDataSizeInBytes(), greaterThan(0L));
            assertThat(metrics.storageMetrics().totalDataSizeInBytes(), greaterThan(0L));
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
        indexDocumentsWithTimestamp(indexName, randomIntBetween(1, 100), 1L, System.currentTimeMillis() - BOOST_WINDOW - 1L);
        refresh(indexName);
        assertBusy(() -> {
            var metrics = searchMetricsService.getSearchTierMetrics();
            assertThat(metrics.maxShardCopies(), equalTo(new MaxShardCopies(1, MetricQuality.EXACT)));
            assertThat(metrics.storageMetrics().totalInteractiveDataSizeInBytes(), equalTo(0L));
            assertThat(metrics.storageMetrics().totalDataSizeInBytes(), greaterThan(0L));
        });
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
