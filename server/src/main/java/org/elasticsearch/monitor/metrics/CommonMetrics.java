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
import java.util.function.Supplier;

public class CommonMetrics {
    private final NodeService nodeService;
    private final NodeStatsCache nodeStatsCache;

    public CommonMetrics(MeterRegistry meterRegistry, NodeService nodeService) {
        this.nodeService = nodeService;
        this.nodeStatsCache = new NodeStatsCache(TimeValue.timeValueSeconds(1));
        registerMetrics(meterRegistry);
    }

    private void registerMetrics(MeterRegistry registry) {
        registry.registerLongGauge(
            "es.indices.get.total",
            "Total number of get operations",
            "operation",
            nodeStatsCache.indicesGetTotalSupplier()
        );
        registry.registerLongGauge(
            "es.indices.get.time_in_millis",
            "Time in milliseconds spent performing get operations.",
            "time",
            nodeStatsCache.indicesGetTimeInMillisSupplier()
        );
        registry.registerLongGauge(
            "es.indices.search.fetch.total",
            "Total number of fetch operations.",
            "operation",
            nodeStatsCache.indicesSearchFetchTotalSupplier()
        );
        registry.registerLongGauge(
            "es.indices.search.fetch.time_in_millis",
            "Time in milliseconds spent performing fetch operations.",
            "time",
            nodeStatsCache.indicesSearchFetchTimeInMillisSupplier()
        );
        registry.registerLongGauge(
            "es.indices.merge.total",
            "Total number of merge operations.",
            "operation",
            nodeStatsCache.indicesMergeTotalSupplier()
        );
        registry.registerLongGauge(
            "es.indices.merge.time_in_millis",
            "Time in milliseconds spent performing merge operations.",
            "time",
            nodeStatsCache.indicesMergeTotalTimeInMillisSupplier()
        );
        registry.registerLongGauge(
            "es.indices.translog.operations",
            "Number of transaction log operations.",
            "operation",
            nodeStatsCache.indicesTranslogOperationSupplier()
        );
        registry.registerLongGauge(
            "es.indices.translog.size",
            "Size, in bytes, of the transaction log.",
            "bytes",
            nodeStatsCache.indicesTranslogSizeInBytesSupplier()
        );
        registry.registerLongGauge(
            "es.indices.translog.uncommitted_operations",
            "Number of uncommitted transaction log operations.",
            "operations",
            nodeStatsCache.indicesTranslogUncommittedOperationsSupplier()
        );
        registry.registerLongGauge(
            "es.indices.translog.uncommitted_size_in_bytes",
            "Size, in bytes, of uncommitted transaction log operations.",
            "bytes",
            nodeStatsCache.indicesTranslogUncommittedSizeInBytesSupplier()
        );
        registry.registerLongGauge(
            "es.indices.translog.earliest_last_modified_age",
            "Earliest last modified age for the transaction log.",
            "time",
            nodeStatsCache.indicesTranslogEariestLastModifiedAgeSupplier()
        );
        registry.registerLongGauge(
            "es.http.current_open",
            "Current number of open HTTP connections for the node.",
            "connections",
            nodeStatsCache.httpCurrentOpenSupplier()
        );
        registry.registerLongGauge(
            "es.transport.rx_size_in_bytes",
            "Size, in bytes, of RX packets received by the node during internal cluster communication.",
            "bytes",
            nodeStatsCache.transportRxSizeInBytesSupplier()
        );
        registry.registerLongGauge(
            "es.transport.tx_size_in_bytes",
            "Size, in bytes, of TX packets sent by the node during internal cluster communication.",
            "bytes",
            nodeStatsCache.transportTxSizeInBytesSupplier()
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

        public Supplier<LongWithAttributes> indicesGetTotalSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getIndices().getGet().getCount(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesGetTimeInMillisSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getIndices().getGet().getTimeInMillis(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesSearchFetchTotalSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getIndices().getSearch().getTotal().getFetchCount(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesSearchFetchTimeInMillisSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getIndices().getSearch().getTotal().getFetchTimeInMillis(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesMergeTotalSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getIndices().getMerge().getTotal(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesMergeTotalTimeInMillisSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getIndices().getMerge().getTotalTimeInMillis(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesTranslogOperationSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getIndices().getTranslog().estimatedNumberOfOperations(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesTranslogSizeInBytesSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getIndices().getTranslog().getTranslogSizeInBytes(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesTranslogUncommittedOperationsSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getIndices().getTranslog().getUncommittedOperations(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesTranslogUncommittedSizeInBytesSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getIndices().getTranslog().getUncommittedSizeInBytes(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesTranslogEariestLastModifiedAgeSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getIndices().getTranslog().getEarliestLastModifiedAge(), Map.of());
        }

        public Supplier<LongWithAttributes> httpCurrentOpenSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getHttp().getServerOpen(), Map.of());
        }

        public Supplier<LongWithAttributes> transportRxSizeInBytesSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getTransport().getRxSize().getBytes(), Map.of());
        }

        public Supplier<LongWithAttributes> transportTxSizeInBytesSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getTransport().getTxSize().getBytes(), Map.of());
        }
    }
}
