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
import org.elasticsearch.monitor.jvm.GcNames;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

/**
 * NodeMetrics monitors various statistics of an Elasticsearch node and exposes them through as metrics
 * the provided MeterRegistry. It includes counters for indices operations, memory usage, transport statistics,
 * and more. The metrics are periodically updated based on a schedule.
 */
public class NodeMetrics {
    private final NodeService nodeService;
    private final NodeStatsCache stats;

    /**
     * Constructs a new NodeMetrics instance.
     *
     * @param meterRegistry The MeterRegistry used to register metrics.
     * @param nodeService   The NodeService for interacting with the Elasticsearch node and extracting statistics.
     */
    public NodeMetrics(MeterRegistry meterRegistry, NodeService nodeService) {
        this.nodeService = nodeService;
        // Agent should poll stats every 4 minutes and being this cache lazy we need a
        // number high enough so that the cache does not update during the same poll
        // period and that expires before a new poll period, therefore we choose 1 minute.
        this.stats = new NodeStatsCache(TimeValue.timeValueMinutes(1));
        registerAsyncMetrics(meterRegistry);
    }

    /**
     * Registers async metrics in the provided MeterRegistry. We are using the lazy NodeStatCache to retrieve
     * the NodeStats once per pool period instead of for every callback if we were not to use it.
     *
     * @param registry The MeterRegistry used to register and collect metrics.
     */
    private void registerAsyncMetrics(MeterRegistry registry) {
        registry.registerLongAsyncCounter(
            "es.node.stats.indices.get.total",
            "Total number of get operations",
            "operation",
            () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getGet().getCount())
        );

        registry.registerLongAsyncCounter(
            "es.node.stats.indices.get.time",
            "Time in milliseconds spent performing get operations.",
            "milliseconds",
            () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getGet().getTimeInMillis())
        );

        registry.registerLongAsyncCounter(
            "es.node.stats.indices.search.fetch.total",
            "Total number of fetch operations.",
            "operation",
            () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getSearch().getTotal().getFetchCount())
        );

        registry.registerLongAsyncCounter(
            "es.node.stats.indices.search.fetch.time",
            "Time in milliseconds spent performing fetch operations.",
            "milliseconds",
            () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getSearch().getTotal().getFetchTimeInMillis())
        );

        registry.registerLongAsyncCounter(
            "es.node.stats.indices.merge.total",
            "Total number of merge operations.",
            "operation",
            () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getMerge().getTotal())
        );

        registry.registerLongAsyncCounter(
            "es.node.stats.indices.merge.time",
            "Time in milliseconds spent performing merge operations.",
            "milliseconds",
            () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getMerge().getTotalTimeInMillis())
        );

        registry.registerLongGauge(
            "es.node.stats.indices.translog.operations",
            "Number of transaction log operations.",
            "operation",
            () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getTranslog().estimatedNumberOfOperations())
        );

        registry.registerLongGauge(
            "es.node.stats.indices.translog.size",
            "Size, in bytes, of the transaction log.",
            "bytes",
            () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getTranslog().getTranslogSizeInBytes())
        );

        registry.registerLongGauge(
            "es.node.stats.indices.translog.uncommitted_operations",
            "Number of uncommitted transaction log operations.",
            "operations",
            () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getTranslog().getUncommittedOperations())
        );

        registry.registerLongGauge(
            "es.node.stats.indices.translog.uncommitted_size",
            "Size, in bytes, of uncommitted transaction log operations.",
            "bytes",
            () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getTranslog().getUncommittedSizeInBytes())
        );

        registry.registerLongAsyncCounter(
            "es.node.stats.indices.translog.earliest_last_modified_age",
            "Earliest last modified age for the transaction log.",
            "time",
            () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getTranslog().getEarliestLastModifiedAge())
        );

        registry.registerLongGauge(
            "es.node.stats.http.current_open",
            "Current number of open HTTP connections for the node.",
            "connections",
            () -> new LongWithAttributes(stats.getOrRefresh().getHttp().getServerOpen())
        );

        registry.registerLongAsyncCounter(
            "es.node.stats.transport.rx_size",
            "Size, in bytes, of RX packets received by the node during internal cluster communication.",
            "bytes",
            () -> new LongWithAttributes(stats.getOrRefresh().getTransport().getRxSize().getBytes())
        );

        registry.registerLongAsyncCounter(
            "es.node.stats.transport.tx_size",
            "Size, in bytes, of TX packets sent by the node during internal cluster communication.",
            "bytes",
            () -> new LongWithAttributes(stats.getOrRefresh().getTransport().getTxSize().getBytes())
        );

        registry.registerLongGauge(
            "es.node.stats.jvm.mem.pools.young.used",
            "Memory, in bytes, used by the young generation heap.",
            "bytes",
            () -> new LongWithAttributes(bytesUsedByGCGen(stats.getOrRefresh().getJvm().getMem(), GcNames.YOUNG))
        );

        registry.registerLongGauge(
            "es.node.stats.jvm.mem.pools.survivor.used",
            "Memory, in bytes, used by the survivor space.",
            "bytes",
            () -> new LongWithAttributes(bytesUsedByGCGen(stats.getOrRefresh().getJvm().getMem(), GcNames.SURVIVOR))
        );

        registry.registerLongGauge(
            "es.node.stats.jvm.mem.pools.old.used",
            "Memory, in bytes, used by the old generation heap.",
            "bytes",
            () -> new LongWithAttributes(bytesUsedByGCGen(stats.getOrRefresh().getJvm().getMem(), GcNames.OLD))
        );

        registry.registerLongAsyncCounter(
            "es.node.stats.fs.io_stats.io_time.total",
            "The total time in millis spent performing I/O operations across all devices used by Elasticsearch.",
            "milliseconds",
            () -> new LongWithAttributes(stats.getOrRefresh().getFs().getIoStats().getTotalIOTimeMillis())
        );
    }

    /**
     * Retrieves the bytes used by a specific garbage collection generation from the provided JvmStats.Mem.
     *
     * @param mem  The JvmStats.Mem containing memory pool information.
     * @param name The name of the garbage collection generation (e.g., "young", "survivor", "old").
     * @return The number of bytes used by the specified garbage collection generation.
     */
    private long bytesUsedByGCGen(JvmStats.Mem mem, String name) {
        long bytesUsed = 0;
        for (JvmStats.MemoryPool pool : mem) {
            if (pool.getName().equals(name)) {
                bytesUsed = pool.getUsed().getBytes();
            }
        }
        return bytesUsed;
    }

    /**
     * Retrieves the current NodeStats for the Elasticsearch node.
     *
     * @return The current NodeStats.
     */
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
            true,
            false,
            true,
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

    /**
     * A very simple NodeStats cache that allows non-blocking refresh calls
     * lazily triggered by expiry time. When getOrRefresh() is called either
     * the cached NodeStats is returned if refreshInterval didn't expire or
     * refresh() is called, cache is updated and the new instance returned.
     */
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
