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

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

public class AutoscalingIndexingMetricsIT extends AbstractStatelessIntegTestCase {
    public void testIndexingMetricsArePublishedEventually() throws Exception {
        startMasterOnlyNode();
        startIndexNodes(1);
        assertBusy(() -> {
            var metrics = internalCluster().getCurrentMasterNodeInstance(IngestMetricsService.class).getIndexTierMetrics();
            assertThat(
                metrics.toString(),
                metrics.nodesLoad().stream().filter(nodeLoad -> nodeLoad.metricQuality() == MetricQuality.EXACT).count(),
                is(greaterThanOrEqualTo(1L))
            );
        });
    }
}
