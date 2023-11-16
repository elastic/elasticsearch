/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.metrics;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.common.util.SingleObjectCache;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;

public class NodeMetrics {
    private final NodeService nodeService;
    private final NodeStatsCache nodeStatsCache;

    public NodeMetrics(MeterRegistry meterRegistry, NodeService nodeService) {
        this.nodeService = nodeService;
        this.nodeStatsCache = new NodeStatsCache(TimeValue.timeValueSeconds(1));
        registerMetrics(meterRegistry);
    }

    private void registerMetrics(MeterRegistry registry) {
        registry.registerLongGauge(
            "es.node.stats.indices.get.total",
            "Total number of get operations",
            "operation",
            () -> new LongWithAttributes(nodeStatsCache.getOrRefresh().getIndices().getGet().getCount(), Map.of())
        );
        registry.registerLongGauge(
            "es.node.stats.indices.get.time_in_millis",
            "Time in milliseconds spent performing get operations.",
            "time",
            () -> new LongWithAttributes(nodeStatsCache.getOrRefresh().getIndices().getGet().getTimeInMillis(), Map.of())
        );
        registry.registerLongGauge(
            "es.node.stats.indices.search.fetch.total",
            "Total number of fetch operations.",
            "operation",
            () -> new LongWithAttributes(nodeStatsCache.getOrRefresh().getIndices().getSearch().getTotal().getFetchCount(), Map.of())
        );
        registry.registerLongGauge(
            "es.node.stats.indices.search.fetch.time_in_millis",
            "Time in milliseconds spent performing fetch operations.",
            "time",
            () -> new LongWithAttributes(nodeStatsCache.getOrRefresh().getIndices().getSearch().getTotal().getFetchTimeInMillis(), Map.of())
        );
        registry.registerLongGauge(
            "es.node.stats.indices.merge.total",
            "Total number of merge operations.",
            "operation",
            () -> new LongWithAttributes(nodeStatsCache.getOrRefresh().getIndices().getMerge().getTotal(), Map.of())
        );
        registry.registerLongGauge(
            "es.node.stats.indices.merge.time_in_millis",
            "Time in milliseconds spent performing merge operations.",
            "time",
            () -> new LongWithAttributes(nodeStatsCache.getOrRefresh().getIndices().getMerge().getTotalTimeInMillis(), Map.of())
        );
        registry.registerLongGauge(
            "es.node.stats.indices.translog.operations",
            "Number of transaction log operations.",
            "operation",
            () -> new LongWithAttributes(nodeStatsCache.getOrRefresh().getIndices().getTranslog().estimatedNumberOfOperations(), Map.of())
        );
        registry.registerLongGauge(
            "es.node.stats.indices.translog.size",
            "Size, in bytes, of the transaction log.",
            "bytes",
            () -> new LongWithAttributes(nodeStatsCache.getOrRefresh().getIndices().getTranslog().getTranslogSizeInBytes(), Map.of())
        );
        registry.registerLongGauge(
            "es.node.stats.indices.translog.uncommitted_operations",
            "Number of uncommitted transaction log operations.",
            "operations",
            () -> new LongWithAttributes(nodeStatsCache.getOrRefresh().getIndices().getTranslog().getUncommittedOperations(), Map.of())
        );
        registry.registerLongGauge(
            "es.node.stats.indices.translog.uncommitted_size_in_bytes",
            "Size, in bytes, of uncommitted transaction log operations.",
            "bytes",
            () -> new LongWithAttributes(nodeStatsCache.getOrRefresh().getIndices().getTranslog().getUncommittedSizeInBytes(), Map.of())
        );
        registry.registerLongGauge(
            "es.node.stats.indices.translog.earliest_last_modified_age",
            "Earliest last modified age for the transaction log.",
            "time",
            () -> new LongWithAttributes(nodeStatsCache.getOrRefresh().getIndices().getTranslog().getEarliestLastModifiedAge(), Map.of())
        );
        registry.registerLongGauge(
            "es.node.stats.http.current_open",
            "Current number of open HTTP connections for the node.",
            "connections",
            () -> new LongWithAttributes(nodeStatsCache.getOrRefresh().getHttp().getServerOpen(), Map.of())
        );
        registry.registerLongGauge(
            "es.node.stats.transport.rx_size_in_bytes",
            "Size, in bytes, of RX packets received by the node during internal cluster communication.",
            "bytes",
            () -> new LongWithAttributes(nodeStatsCache.getOrRefresh().getTransport().getRxSize().getBytes(), Map.of())
        );
        registry.registerLongGauge(
            "es.node.stats.transport.tx_size_in_bytes",
            "Size, in bytes, of TX packets sent by the node during internal cluster communication.",
            "bytes",
            () -> new LongWithAttributes(nodeStatsCache.getOrRefresh().getTransport().getTxSize().getBytes(), Map.of())
        );
    }

    private NodeStats getNodeStats() {
        CommonStatsFlags flags = new CommonStatsFlags(
            CommonStatsFlags.Flag.Get,
            CommonStatsFlags.Flag.Search,
            CommonStatsFlags.Flag.Merge,
            CommonStatsFlags.Flag.Translog
        );
        return nodeService.stats(
            flags,
            true,
            false,
            false,
            false,
            false,
            false,
            true,
            true,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false
        );
    }

    private class NodeStatsCache extends SingleObjectCache<NodeStats> {

        NodeStatsCache(TimeValue interval) {
            super(interval, getNodeStats());
        }

        @Override
        protected NodeStats refresh() {
            return getNodeStats();
        }
    }
}
