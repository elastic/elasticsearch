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

package co.elastic.elasticsearch.stateless.autoscaling.indexing;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class AutoscalingIndexingMetricsIT extends AbstractStatelessIntegTestCase {
    public void testIndexingMetricsArePublishedEventually() throws Exception {
        startMasterOnlyNode();
        // Reduce the time between publications, so we can expect at least one publication per second.
        startIndexNode(
            Settings.builder()
                .put(IngestLoadSampler.MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.getKey(), TimeValue.timeValueSeconds(1))
                .build()
        );

        final AtomicInteger ingestLoadPublishSent = new AtomicInteger(0);
        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(PublishNodeIngestLoadAction.NAME)) {
                    ingestLoadPublishSent.incrementAndGet();
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        assertBusy(() -> assertThat(ingestLoadPublishSent.get(), equalTo(1)));
        var metrics = internalCluster().getCurrentMasterNodeInstance(IngestMetricsService.class).getIndexTierMetrics();
        assertThat(metrics.toString(), metrics.nodesLoad().size(), equalTo(1));
        assertThat(metrics.toString(), metrics.nodesLoad().get(0).metricQuality(), equalTo(MetricQuality.EXACT));
        assertThat(metrics.toString(), metrics.nodesLoad().get(0).load(), equalTo(0.0));

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        int bulkRequests = randomIntBetween(10, 20);
        for (int i = 0; i < bulkRequests; i++) {
            indexDocs(indexName, randomIntBetween(100, 1000));
        }
        // Wait for at least one more publish
        assertBusy(() -> assertThat(ingestLoadPublishSent.get(), greaterThan(1)));
        // We'd need an assertBusy since the second publish might still miss the recent load.
        assertBusy(() -> {
            var metricsAfter = internalCluster().getCurrentMasterNodeInstance(IngestMetricsService.class).getIndexTierMetrics();
            assertThat(metricsAfter.toString(), metricsAfter.nodesLoad().size(), equalTo(1));
            assertThat(metricsAfter.toString(), metricsAfter.nodesLoad().get(0).metricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(metricsAfter.toString(), metricsAfter.nodesLoad().get(0).load(), greaterThan(0.0));
        });
    }
}
