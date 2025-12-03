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

package co.elastic.elasticsearch.stateless.allocation;

import co.elastic.elasticsearch.stateless.ServerlessStatelessPlugin;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.EstimatedHeapUsageCollector;
import org.elasticsearch.cluster.service.ClusterService;

import java.util.Map;

public class StatelessHeapUsageCollector implements EstimatedHeapUsageCollector {

    private final ServerlessStatelessPlugin stateless;

    public StatelessHeapUsageCollector() {
        throw new IllegalStateException("This no arg constructor only exists for SPI validation");
    }

    public StatelessHeapUsageCollector(ServerlessStatelessPlugin stateless) {
        this.stateless = stateless;
    }

    @Override
    public void collectClusterHeapUsage(ActionListener<Map<String, Long>> listener) {
        MemoryMetricsService memoryMetricsService = stateless.getMemoryMetricsService();
        ClusterService clusterService = stateless.getClusterService();
        ActionListener.completeWith(listener, () -> memoryMetricsService.getPerNodeMemoryMetrics(clusterService.state().nodes()));
    }
}
