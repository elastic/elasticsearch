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

import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService;

public class SearchTierMetricsService {

    private final MemoryMetricsService memoryMetricsService;

    public SearchTierMetricsService(MemoryMetricsService memoryMetricsService) {
        this.memoryMetricsService = memoryMetricsService;
    }

    public SearchTierMetrics getSearchTierMetrics() {
        var memoryMetrics = memoryMetricsService.getMemoryMetrics();
        return new SearchTierMetrics(
            memoryMetrics,
            new MaxShardCopies(1, MetricQuality.EXACT),
            new StorageMetrics(100, 1000, 10000, MetricQuality.EXACT)
        );
    }
}
