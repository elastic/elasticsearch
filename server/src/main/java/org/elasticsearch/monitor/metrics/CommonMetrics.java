/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.metrics;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.util.SingleObjectCache;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;
import java.util.function.Supplier;

public class CommonMetrics {
    private final ClusterAdminClient client;
    private final NodeStatsResponseActionListener actionListener;
    private final CommonStatsCache commonStatsCache;

    public CommonMetrics(MeterRegistry meterRegistry, ClusterAdminClient client) {
        this.client = client;
        this.actionListener = new NodeStatsResponseActionListener();
        this.commonStatsCache = new CommonStatsCache(TimeValue.timeValueSeconds(10)); // TODO change this maybe?
        meterRegistry.registerLongGauge(
            "es.indices.get.total",
            "Total number of get operations",
            "operation",
            commonStatsCache.indicesGetTotalSupplier()
        );
        meterRegistry.registerLongGauge(
            "es.indices.get.time_in_millis",
            "Time in milliseconds spent performing get operations.",
            "time",
            commonStatsCache.indicesGetTimeInMillisSupplier()
        );
        meterRegistry.registerLongGauge(
            "es.indices.search.fetch.total",
            "Total number of fetch operations.",
            "operation",
            commonStatsCache.indicesSearchFetchTotalSupplier()
        );
        meterRegistry.registerLongGauge(
            "es.indices.search.fetch.time_in_millis",
            "Time in milliseconds spent performing fetch operations.",
            "time",
            commonStatsCache.indicesSearchFetchTimeInMillisSupplier()
        );
        meterRegistry.registerLongGauge(
            "es.indices.merge.total",
            "Total number of merge operations.",
            "operation",
            commonStatsCache.indicesMergeTotalSupplier()
        );
        LongGauge lg = meterRegistry.registerLongGauge(
            "es.indices.merge.time_in_millis",
            "Time in milliseconds spent performing merge operations.",
            "time",
            commonStatsCache.indicesMergeTotalTimeInMillisSupplier()
        );
        meterRegistry.registerLongGauge(
            "es.indices.translog.operations",
            "Number of transaction log operations.",
            "operation",
            commonStatsCache.indicesTranslogOperationSupplier()
        );
        meterRegistry.registerLongGauge(
            "es.indices.translog.size",
            "Size, in bytes, of the transaction log.",
            "bytes",
            commonStatsCache.indicesTranslogSizeInBytesSupplier()
        );
        meterRegistry.registerLongGauge(
            "es.indices.translog.uncommitted_operations",
            " Number of uncommitted transaction log operations.",
            "operations",
            commonStatsCache.indicesTranslogUncommittedOperationsSupplier()
        );
        meterRegistry.registerLongGauge(
            "es.indices.translog.uncommitted_size_in_bytes",
            "Size, in bytes, of uncommitted transaction log operations.",
            "bytes",
            commonStatsCache.indicesTranslogUncommittedSizeInBytesSupplier()
        );
        meterRegistry.registerLongGauge(
            "es.indices.translog.earliest_last_modified_age",
            "Earliest last modified age for the transaction log.",
            "time",
            commonStatsCache.indicesTranslogEariestLastModifiedAgeSupplier()
        );
    }

    private class CommonStatsCache extends SingleObjectCache<CommonStats> implements ClusterStateListener {
        private String localNodeId;

        CommonStatsCache(TimeValue interval) {
            super(interval, new CommonStats());
        }

        @Override
        protected CommonStats refresh() {
            if (localNodeId.isEmpty()) {
                return new CommonStats();
            }
            refreshCommonStats(localNodeId);
            return actionListener.commonStats();
        }

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            this.localNodeId = event.state().nodes().getLocalNodeId();
            refreshCommonStats(localNodeId);
        }

        public Supplier<LongWithAttributes> indicesGetTotalSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getGet().getCount(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesGetTimeInMillisSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getGet().getTimeInMillis(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesSearchFetchTotalSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getSearch().getTotal().getFetchCount(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesSearchFetchTimeInMillisSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getSearch().getTotal().getFetchTimeInMillis(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesMergeTotalSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getMerge().getTotal(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesMergeTotalTimeInMillisSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getMerge().getTotalTimeInMillis(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesTranslogOperationSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getTranslog().estimatedNumberOfOperations(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesTranslogSizeInBytesSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getTranslog().getTranslogSizeInBytes(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesTranslogUncommittedOperationsSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getTranslog().getUncommittedOperations(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesTranslogUncommittedSizeInBytesSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getTranslog().getUncommittedSizeInBytes(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesTranslogEariestLastModifiedAgeSupplier() {
            return () -> new LongWithAttributes(getOrRefresh().getTranslog().getEarliestLastModifiedAge(), Map.of());
        }

    }

    private void refreshCommonStats(String localNodeId) {
        final NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(localNodeId);
        this.client.nodesStats(nodesStatsRequest, this.actionListener);
    }

    private static class NodeStatsResponseActionListener implements ActionListener<NodesStatsResponse> {
        private CommonStats commonStats = new CommonStats();

        @Override
        public void onResponse(NodesStatsResponse nodesStatsResponse) {
            CommonStats cs = new CommonStats();
            nodesStatsResponse.getNodes().forEach(nodeStats -> {
                cs.getGet().add(nodeStats.getIndices().getGet());
                cs.getSearch().getTotal().add(nodeStats.getIndices().getSearch().getTotal());
                cs.getMerge().add(nodeStats.getIndices().getMerge());
                cs.getTranslog().add(nodeStats.getIndices().getTranslog());
            });
            commonStats = cs;
        }

        @Override
        public void onFailure(Exception e) {
            // TODO: schedule retry
        }

        public CommonStats commonStats() {
            return commonStats;
        }
    }
}
