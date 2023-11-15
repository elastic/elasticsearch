/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.metrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.util.SingleObjectCache;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;
import java.util.function.Supplier;

public class CommonMetrics implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(CommonMetrics.class);
    private final CommonStats stats;
    private final ClusterAdminClient client;
    private final NodesInfoRequest nodeInfoRequest = new NodesInfoRequest();

    CommonStatsCache commonStatsCache;
    String localNodeId;

    public CommonMetrics(MeterRegistry meterRegistry, ClusterAdminClient client) {
        CommonStatsFlags flags = new CommonStatsFlags(
            CommonStatsFlags.Flag.Get,
            CommonStatsFlags.Flag.Search,
            CommonStatsFlags.Flag.Merge,
            CommonStatsFlags.Flag.Translog
        );
        this.stats = new CommonStats(flags);
        this.client = client;

        IndicesGetStatsCache indicesGetStatsCache = new IndicesGetStatsCache(TimeValue.timeValueSeconds(1), stats.getGet());
        IndicesSearchStatsCache indicesSearchStatsCache = new IndicesSearchStatsCache(
            TimeValue.timeValueSeconds(1),
            stats.getSearch().getTotal()
        );
        IndicesMergeStatsCache indicesMergeStatsCache = new IndicesMergeStatsCache(TimeValue.timeValueSeconds(1), stats.getMerge());
        IndicesTranslogStatsCache indicesTranslogStatsCache = new IndicesTranslogStatsCache(
            TimeValue.timeValueSeconds(1),
            stats.getTranslog()
        );
        meterRegistry.registerLongGauge(
            "es.indices.get.total",
            "Total number of get operations",
            "operation",
            indicesGetStatsCache.indicesGetTotalSupplier()
        );
        meterRegistry.registerLongGauge(
            "es.indices.get.time_in_millis",
            "Time in milliseconds spent performing get operations.",
            "time",
            indicesGetStatsCache.indicesGetTimeInMillisSupplier()
        );
        meterRegistry.registerLongGauge(
            "es.indices.search.fetch.total",
            "Total number of fetch operations.",
            "operation",
            indicesSearchStatsCache.indicesSearchFetchTotalSupplier()
        );
        meterRegistry.registerLongGauge(
            "es.indices.search.fetch.time_in_millis",
            "Time in milliseconds spent performing fetch operations.",
            "time",
            indicesSearchStatsCache.indicesSearchFetchTimeInMillisSupplier()
        );
        meterRegistry.registerLongGauge(
            "es.indices.merge.total",
            "Total number of merge operations.",
            "operation",
            indicesMergeStatsCache.indicesMergeTotalSupplier()
        );
        meterRegistry.registerLongGauge(
            "es.indices.merge.time_in_millis",
            "Time in milliseconds spent performing merge operations.",
            "time",
            indicesMergeStatsCache.indicesMergeTotalTimeInMillisSupplier()
        );
        meterRegistry.registerLongGauge(
            "es.indices.translog.operations",
            "Number of transaction log operations.",
            "operation",
            indicesTranslogStatsCache.indicesTranslogOperationSupplier()
        );
        meterRegistry.registerLongGauge(
            "es.indices.translog.size",
            "Size, in bytes, of the transaction log.",
            "bytes",
            indicesTranslogStatsCache.indicesTranslogSizeInBytesSupplier()
        );

    }

    public void clusterChanged(ClusterChangedEvent event) {
        this.localNodeId = event.state().nodes().getLocalNodeId();
        final NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(this.localNodeId);
        // final NodesStatsRequest nodesStatsRequest = new NodesStatsRequest();

        this.client.nodesStats(nodesStatsRequest, new ActionListener<NodesStatsResponse>() {
            @Override
            public void onResponse(NodesStatsResponse nodesStatsResponse) {

                nodesStatsResponse.getNodes().stream().forEach(nodeStats -> {
                    CommonStats cs = new CommonStats();
                    cs.getGet().add(nodeStats.getIndices().getGet());
                    cs.getSearch().getTotal().add(nodeStats.getIndices().getSearch().getTotal());
                    cs.getMerge().add(nodeStats.getIndices().getMerge());
                    cs.getTranslog().add(nodeStats.getIndices().getTranslog());
                    commonStatsCache = new CommonStatsCache(TimeValue.timeValueSeconds(30), cs);
                });
            }

            @Override
            public void onFailure(Exception e) {
                logger.info(e); // todo schedule retry
            }
        });
    }

    private class CommonStatsCache extends SingleObjectCache<CommonStats> {
        CommonStatsCache(TimeValue interval, CommonStats initValue) {
            super(interval, initValue);
        }

        @Override
        protected CommonStats refresh() {
            final NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(localNodeId);
            // final NodesStatsRequest nodesStatsRequest = new NodesStatsRequest();
            CommonStats refreshedValue;
            client.nodesStats(nodesStatsRequest, new ActionListener<NodesStatsResponse>() {
                CommonStats cs = new CommonStats();

                @Override
                public void onResponse(NodesStatsResponse nodesStatsResponse) {

                    nodesStatsResponse.getNodes().stream().forEach(nodeStats -> {
                        CommonStats cs = new CommonStats();
                        cs.getGet().add(nodeStats.getIndices().getGet());
                        cs.getSearch().getTotal().add(nodeStats.getIndices().getSearch().getTotal());
                        cs.getMerge().add(nodeStats.getIndices().getMerge());
                        cs.getTranslog().add(nodeStats.getIndices().getTranslog());
                        this.cs = cs;
                    });
                }

                @Override
                public void onFailure(Exception e) {
                    logger.info(e); // todo schedule retry
                }
            });
            return null;
        }
    }

    private class IndicesGetStatsCache extends SingleObjectCache<GetStats> {
        private static final Logger logger = LogManager.getLogger(IndicesGetStatsCache.class);

        IndicesGetStatsCache(TimeValue interval, GetStats initValue) {
            super(interval, initValue);
        }

        @Override
        protected GetStats refresh() {
            return stats.getGet();
        }

        public Supplier<LongWithAttributes> indicesGetTotalSupplier() {
            logger.info("MPMPMP: indicesGetTotalSupplier " + getOrRefresh().getCount());
            return () -> new LongWithAttributes(getOrRefresh().getCount(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesGetTimeInMillisSupplier() {
            logger.info("MPMPMP: indicesGetTimeInMillisSupplier " + getOrRefresh().getTimeInMillis());
            return () -> new LongWithAttributes(getOrRefresh().getTimeInMillis(), Map.of());
        }
    }

    private class IndicesSearchStatsCache extends SingleObjectCache<SearchStats.Stats> {
        private static final Logger logger = LogManager.getLogger(IndicesSearchStatsCache.class);

        IndicesSearchStatsCache(TimeValue interval, SearchStats.Stats initValue) {
            super(interval, initValue);
        }

        @Override
        protected SearchStats.Stats refresh() {
            return stats.getSearch().getTotal();
        }

        public Supplier<LongWithAttributes> indicesSearchFetchTotalSupplier() {
            logger.info("MPMPMP: indicesSearchFetchTotalSupplier " + getOrRefresh().getFetchCount());
            return () -> new LongWithAttributes(getOrRefresh().getFetchCount(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesSearchFetchTimeInMillisSupplier() {
            logger.info("MPMPMP: indicesSearchFetchTimeInMillisSupplier " + getOrRefresh().getFetchTimeInMillis());
            return () -> new LongWithAttributes(getOrRefresh().getFetchTimeInMillis(), Map.of());
        }
    }

    private class IndicesMergeStatsCache extends SingleObjectCache<MergeStats> {
        private static final Logger logger = LogManager.getLogger(IndicesTranslogStatsCache.class);

        IndicesMergeStatsCache(TimeValue interval, MergeStats initValue) {
            super(interval, initValue);
        }

        @Override
        protected MergeStats refresh() {
            return stats.getMerge();
        }

        public Supplier<LongWithAttributes> indicesMergeTotalSupplier() {
            logger.info("MPMPMP: indicesMergeTotalSupplier " + getOrRefresh().getTotal());
            return () -> new LongWithAttributes(getOrRefresh().getTotal(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesMergeTotalTimeInMillisSupplier() {
            logger.info("MPMPMP: indicesMergeTotalTimeInMillisSupplier " + getOrRefresh().getTotalTimeInMillis());
            return () -> new LongWithAttributes(getOrRefresh().getTotalTimeInMillis(), Map.of());
        }
    }

    private class IndicesTranslogStatsCache extends SingleObjectCache<TranslogStats> {
        private static final Logger logger = LogManager.getLogger(IndicesTranslogStatsCache.class);

        IndicesTranslogStatsCache(TimeValue interval, TranslogStats initValue) {
            super(interval, initValue);
        }

        @Override
        protected TranslogStats refresh() {
            return stats.getTranslog();
        }

        public Supplier<LongWithAttributes> indicesTranslogOperationSupplier() {
            logger.info("MPMPMP: indicesTranslogOperationSupplier " + getOrRefresh().estimatedNumberOfOperations());
            return () -> new LongWithAttributes(getOrRefresh().estimatedNumberOfOperations(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesTranslogSizeInBytesSupplier() {
            logger.info("MPMPMP: indicesTranslogSizeInBytesSupplier " + getOrRefresh().getTranslogSizeInBytes());
            return () -> new LongWithAttributes(getOrRefresh().getTranslogSizeInBytes(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesTranslogUncommittedOperationsSupplier() {
            logger.info("MPMPMP: indicesTranslogUncommittedOperationsSupplier " + getOrRefresh().getUncommittedOperations());
            return () -> new LongWithAttributes(getOrRefresh().getUncommittedOperations(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesTranslogUncommittedSizeInBytesSupplier() {
            logger.info("MPMPMP: indicesTranslogUncommittedSizeInBytesSupplier " + getOrRefresh().getUncommittedSizeInBytes());
            return () -> new LongWithAttributes(getOrRefresh().getUncommittedSizeInBytes(), Map.of());
        }

        public Supplier<LongWithAttributes> indicesTranslogEariestLastModifiedAgeSupplier() {
            logger.info("MPMPMP: indicesTranslogEariestLastModifiedAgeSupplier " + getOrRefresh().getEarliestLastModifiedAge());
            return () -> new LongWithAttributes(getOrRefresh().getEarliestLastModifiedAge(), Map.of());
        }
    }

    // todo-mp: indices.get.total DONE?
    // todo-mp: indices.get.time_in_millis DONE?
    // todo-mp: indices.search.fetch_total DONE?
    // todo-mp: indices.search.fetch_time_in_millis DONE?
    // todo-mp: indices.merges.total DONE?
    // todo-mp: indices.merges.total_time_in_millis DONE?
    // todo-mp: transport.rx_size_in_bytes
    // todo-mp: transport.tx_size_in_bytes
    // todo-mp: indices.translog.*
}
