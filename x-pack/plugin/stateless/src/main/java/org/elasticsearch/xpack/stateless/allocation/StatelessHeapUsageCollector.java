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

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.EstimatedHeapUsageCollector;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.autoscaling.memory.MemoryMetricsService;

import java.util.Map;

public class StatelessHeapUsageCollector implements EstimatedHeapUsageCollector {

    private final StatelessPlugin stateless;

    public StatelessHeapUsageCollector() {
        throw new IllegalStateException("This no arg constructor only exists for SPI validation");
    }

    public StatelessHeapUsageCollector(StatelessPlugin stateless) {
        this.stateless = stateless;
    }

    @Override
    public void collectClusterHeapUsage(ActionListener<Map<String, Long>> listener) {
        MemoryMetricsService memoryMetricsService = stateless.getMemoryMetricsService();
        ClusterService clusterService = stateless.getClusterService();
        ActionListener.completeWith(listener, () -> memoryMetricsService.getPerNodeMemoryMetrics(clusterService.state().nodes()));
    }
}
