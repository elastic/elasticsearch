/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.node;

import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Collection;
import java.util.Map;

/**
 * APM metrics for adaptive replica selection. Each metric is emitted once per
 * target node that this coordinating node has collected stats for, tagged with
 * the target {@code node_id}.
 */
public class AdaptiveReplicaSelectionMetrics {

    public static final String AVG_QUEUE_SIZE_METRIC_NAME = "es.adaptive_replica_selection.avg_queue_size.gauge";
    public static final String AVG_SERVICE_TIME_NS_METRIC_NAME = "es.adaptive_replica_selection.avg_service_time_ns.gauge";
    public static final String AVG_RESPONSE_TIME_NS_METRIC_NAME = "es.adaptive_replica_selection.avg_response_time_ns.gauge";

    static final String NODE_ID_ATTRIBUTE = "node_id";

    public AdaptiveReplicaSelectionMetrics(MeterRegistry meterRegistry, ResponseCollectorService responseCollectorService) {
        meterRegistry.registerLongsGauge(
            AVG_QUEUE_SIZE_METRIC_NAME,
            "EWMA of the search thread pool queue size per node, as observed by this coordinating node",
            "{task}",
            () -> toQueueSizeMetrics(responseCollectorService.getAllNodeStatistics())
        );
        meterRegistry.registerLongsGauge(
            AVG_SERVICE_TIME_NS_METRIC_NAME,
            "EWMA of the shard query service time per node, as reported by the data node",
            "ns",
            () -> toServiceTimeMetrics(responseCollectorService.getAllNodeStatistics())
        );
        meterRegistry.registerLongsGauge(
            AVG_RESPONSE_TIME_NS_METRIC_NAME,
            "EWMA of the round-trip response time per node, as measured by this coordinating node",
            "ns",
            () -> toResponseTimeMetrics(responseCollectorService.getAllNodeStatistics())
        );
    }

    private static Collection<LongWithAttributes> toQueueSizeMetrics(Map<String, ResponseCollectorService.ComputedNodeStats> nodeStats) {
        return nodeStats.entrySet()
            .stream()
            .map(e -> new LongWithAttributes(e.getValue().queueSize, Map.of(NODE_ID_ATTRIBUTE, e.getKey())))
            .toList();
    }

    private static Collection<LongWithAttributes> toServiceTimeMetrics(Map<String, ResponseCollectorService.ComputedNodeStats> nodeStats) {
        return nodeStats.entrySet()
            .stream()
            .map(e -> new LongWithAttributes((long) e.getValue().serviceTime, Map.of(NODE_ID_ATTRIBUTE, e.getKey())))
            .toList();
    }

    private static Collection<LongWithAttributes> toResponseTimeMetrics(Map<String, ResponseCollectorService.ComputedNodeStats> nodeStats) {
        return nodeStats.entrySet()
            .stream()
            .map(e -> new LongWithAttributes((long) e.getValue().responseTime, Map.of(NODE_ID_ATTRIBUTE, e.getKey())))
            .toList();
    }
}
