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
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.util.SingleObjectCache;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.jvm.GcNames;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * NodeMetrics monitors various statistics of an Elasticsearch node and exposes them as metrics through
 * the provided MeterRegistry. It includes counters for indices operations, memory usage, transport statistics,
 * and more. The metrics are periodically updated based on a schedule.
 */
public class NodeMetrics extends AbstractLifecycleComponent {
    private final Logger logger = LogManager.getLogger(NodeMetrics.class);
    private final MeterRegistry registry;
    private final NodeService nodeService;
    private final List<AutoCloseable> metrics;
    private NodeStatsCache stats;

    /**
     * Constructs a new NodeMetrics instance.
     *
     * @param meterRegistry The MeterRegistry used to register metrics.
     * @param nodeService   The NodeService for interacting with the Elasticsearch node and extracting statistics.
     */
    public NodeMetrics(MeterRegistry meterRegistry, NodeService nodeService) {
        this.registry = meterRegistry;
        this.nodeService = nodeService;
        this.metrics = new ArrayList<>(17);
    }

    /**
     * Registers async metrics in the provided MeterRegistry. We are using the lazy NodeStatCache to retrieve
     * the NodeStats once per pool period instead of for every callback if we were not to use it.
     *
     * @param registry The MeterRegistry used to register and collect metrics.
     */
    private void registerAsyncMetrics(MeterRegistry registry) {
        // Agent should poll stats every 4 minutes and being this cache is lazy we need a
        // number high enough so that the cache does not update during the same poll
        // period and that expires before a new poll period, therefore we choose 1 minute.
        this.stats = new NodeStatsCache(TimeValue.timeValueMinutes(1));
        metrics.add(
            registry.registerLongAsyncCounter(
                "es.node.stats.indices.get.total",
                "Total number of get operations",
                "operation",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getGet().getCount())
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.node.stats.indices.get.time",
                "Time in milliseconds spent performing get operations.",
                "milliseconds",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getGet().getTimeInMillis())
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.node.stats.indices.search.fetch.total",
                "Total number of fetch operations.",
                "operation",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getSearch().getTotal().getFetchCount())
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.node.stats.indices.search.fetch.time",
                "Time in milliseconds spent performing fetch operations.",
                "milliseconds",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getSearch().getTotal().getFetchTimeInMillis())
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.node.stats.indices.merge.total",
                "Total number of merge operations.",
                "operation",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getMerge().getTotal())
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.node.stats.indices.merge.time",
                "Time in milliseconds spent performing merge operations.",
                "milliseconds",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getMerge().getTotalTimeInMillis())
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.node.stats.indices.translog.operations",
                "Number of transaction log operations.",
                "operation",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getTranslog().estimatedNumberOfOperations())
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.node.stats.indices.translog.size",
                "Size, in bytes, of the transaction log.",
                "bytes",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getTranslog().getTranslogSizeInBytes())
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.node.stats.indices.translog.uncommitted_operations",
                "Number of uncommitted transaction log operations.",
                "operations",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getTranslog().getUncommittedOperations())
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.node.stats.indices.translog.uncommitted_size",
                "Size, in bytes, of uncommitted transaction log operations.",
                "bytes",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getTranslog().getUncommittedSizeInBytes())
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.node.stats.indices.translog.earliest_last_modified_age",
                "Earliest last modified age for the transaction log.",
                "time",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getTranslog().getEarliestLastModifiedAge())
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.node.stats.transport.rx_size",
                "Size, in bytes, of RX packets received by the node during internal cluster communication.",
                "bytes",
                () -> new LongWithAttributes(stats.getOrRefresh().getTransport().getRxSize().getBytes())
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.node.stats.transport.tx_size",
                "Size, in bytes, of TX packets sent by the node during internal cluster communication.",
                "bytes",
                () -> new LongWithAttributes(stats.getOrRefresh().getTransport().getTxSize().getBytes())
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.node.stats.jvm.mem.pools.young.used",
                "Memory, in bytes, used by the young generation heap.",
                "bytes",
                () -> new LongWithAttributes(bytesUsedByGCGen(stats.getOrRefresh().getJvm().getMem(), GcNames.YOUNG))
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.node.stats.jvm.mem.pools.survivor.used",
                "Memory, in bytes, used by the survivor space.",
                "bytes",
                () -> new LongWithAttributes(bytesUsedByGCGen(stats.getOrRefresh().getJvm().getMem(), GcNames.SURVIVOR))
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.node.stats.jvm.mem.pools.old.used",
                "Memory, in bytes, used by the old generation heap.",
                "bytes",
                () -> new LongWithAttributes(bytesUsedByGCGen(stats.getOrRefresh().getJvm().getMem(), GcNames.OLD))
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.node.stats.fs.io_stats.io_time.total",
                "The total time in millis spent performing I/O operations across all devices used by Elasticsearch.",
                "milliseconds",
                () -> new LongWithAttributes(stats.getOrRefresh().getFs().getIoStats().getTotalIOTimeMillis())
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.node.stats.indices.indexing.docs.total",
                "Total number of indexed documents",
                "documents",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getIndexing().getTotal().getIndexCount())
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.node.stats.indices.indexing.docs.current",
                "Current number of indexing documents",
                "documents",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getIndexing().getTotal().getIndexCurrent())
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.node.stats.indices.indexing.failed.total",
                "Total number of failed indexing operations",
                "operations",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getIndexing().getTotal().getIndexFailedCount())
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.node.stats.indices.deletion.docs.total",
                "Total number of deleted documents",
                "documents",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getIndexing().getTotal().getDeleteCount())
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.node.stats.indices.deletion.docs.current",
                "Current number of deleting documents",
                "documents",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getIndexing().getTotal().getDeleteCurrent())
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.node.stats.indices.indexing.time",
                "Total indices indexing time",
                "milliseconds",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getIndexing().getTotal().getIndexTime().millis())
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.node.stats.indices.deletion.time",
                "Total indices deletion time",
                "milliseconds",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getIndexing().getTotal().getDeleteTime().millis())
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.node.stats.indices.throttle.time",
                "Total indices throttle time",
                "milliseconds",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getIndexing().getTotal().getThrottleTime().millis())
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.node.stats.indices.noop.total",
                "Total number of noop shard operations",
                "operations",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndices().getIndexing().getTotal().getNoopUpdateCount())
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.node.stats.indices.indexing.coordinating_operations.memory.size.total",
                "Total number of memory bytes consumed by coordinating operations",
                "bytes",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndexingPressureStats().getTotalCoordinatingBytes())
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.node.stats.indices.indexing.coordinating_operations.count.total",
                "Total number of coordinating operations",
                "operations",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndexingPressureStats().getTotalCoordinatingOps())
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.node.stats.indices.indexing.coordinating_operations.memory.size.current",
                "Current number of memory bytes consumed by coordinating operations",
                "bytes",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndexingPressureStats().getCurrentCoordinatingBytes())
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.node.stats.indices.indexing.coordinating_operations.count.current",
                "Current number of coordinating operations",
                "operations",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndexingPressureStats().getCurrentCoordinatingOps())
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.node.stats.indices.indexing.coordinating_operations.rejections.total",
                "Total number of coordinating operations rejections",
                "operations",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndexingPressureStats().getCoordinatingRejections())
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.node.stats.indices.indexing.primary_operations.memory.size.total",
                "Total number of memory bytes consumed by primary operations",
                "bytes",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndexingPressureStats().getTotalPrimaryBytes())
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.node.stats.indices.indexing.primary_operations.count.total",
                "Total number of primary operations",
                "operations",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndexingPressureStats().getTotalPrimaryOps())
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.node.stats.indices.indexing.primary_operations.memory.size.current",
                "Current number of memory bytes consumed by primary operations",
                "bytes",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndexingPressureStats().getCurrentPrimaryBytes())
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.node.stats.indices.indexing.primary_operations.count.current",
                "Current number of primary operations",
                "operations",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndexingPressureStats().getCurrentPrimaryOps())
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.node.stats.indices.indexing.primary_operations.rejections.total",
                "Total number of primary operations rejections",
                "operations",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndexingPressureStats().getPrimaryRejections())
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.node.stats.indices.indexing.memory.limit.current",
                "Current memory limit for primary and coordinating operations",
                "bytes",
                () -> new LongWithAttributes(stats.getOrRefresh().getIndexingPressureStats().getMemoryLimit())
            )
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
            CommonStatsFlags.Flag.Indexing,
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
            false,
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

    @Override
    protected void doStart() {
        registerAsyncMetrics(registry);
    }

    @Override
    protected void doStop() {
        stats.stopRefreshing();
    }

    @Override
    protected void doClose() throws IOException {
        metrics.forEach(metric -> {
            try {
                metric.close();
            } catch (Exception e) {
                logger.warn("metrics close() method should not throw Exception", e);
            }
        });
    }

    /**
     * A very simple NodeStats cache that allows non-blocking refresh calls
     * lazily triggered by expiry time. When getOrRefresh() is called either
     * the cached NodeStats is returned if refreshInterval didn't expire or
     * refresh() is called, cache is updated and the new instance returned.
     */
    private class NodeStatsCache extends SingleObjectCache<NodeStats> {
        private boolean refresh;

        NodeStatsCache(TimeValue interval) {
            super(interval, getNodeStats());
            this.refresh = true;
        }

        @Override
        protected NodeStats refresh() {
            return refresh ? getNodeStats() : getNoRefresh();
        }

        public void stopRefreshing() {
            this.refresh = false;
        }
    }
}
