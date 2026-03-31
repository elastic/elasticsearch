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

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.EstimatedHeapUsageCollector;
import org.elasticsearch.cluster.ShardHeapUsageEstimates;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.xpack.stateless.StatelessPlugin;

import java.util.Map;

public class StatelessHeapUsageCollector implements EstimatedHeapUsageCollector {

    private final StatelessPlugin plugin;

    public StatelessHeapUsageCollector() {
        throw new IllegalStateException("This no arg constructor only exists for SPI validation");
    }

    public StatelessHeapUsageCollector(StatelessPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public void collectClusterHeapUsage(ActionListener<Map<String, Long>> listener) {
        StatelessMemoryMetricsService memoryMetricsService = plugin.getStatelessMemoryMetricsService();
        ClusterService clusterService = plugin.getClusterService();
        ActionListener.completeWith(listener, () -> memoryMetricsService.getPerNodeMemoryMetrics(clusterService.state()));
    }

    @Override
    public void collectShardHeapUsage(ActionListener<ShardHeapUsageEstimates> listener) {
        ActionListener.completeWith(listener, () -> plugin.getStatelessMemoryMetricsService().getShardHeapUsageEstimates());
    }
}
