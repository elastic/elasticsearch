/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.metrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.SingleObjectCache;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.stats.IndexingPressureStats;
import org.elasticsearch.monitor.jvm.GcNames;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
    private final TimeValue cacheExpiry;

    /**
     * Constructs a new NodeMetrics instance.
     *
     * @param meterRegistry     The MeterRegistry used to register metrics.
     * @param nodeService       The NodeService for interacting with the Elasticsearch node and extracting statistics.
     * @param metricsInterval   The interval at which the agent sends metrics to the APM Server
     * */
    public NodeMetrics(MeterRegistry meterRegistry, NodeService nodeService, TimeValue metricsInterval) {
        this.registry = meterRegistry;
        this.nodeService = nodeService;
        this.metrics = new ArrayList<>(17);
        // we set the cache to expire after half the interval at which the agent sends
        // metrics to the APM Server so that there is enough time for the cache not
        // update during the same poll period and that expires before a new poll period
        this.cacheExpiry = new TimeValue(metricsInterval.getMillis() / 2);
    }

    /**
     * Registers async metrics in the provided MeterRegistry. We are using the lazy NodeStatCache to retrieve
     * the NodeStats once per pool period instead of for every callback if we were not to use it.
     *
     * @param registry The MeterRegistry used to register and collect metrics.
     */
    private void registerAsyncMetrics(MeterRegistry registry) {
        this.stats = new NodeStatsCache(cacheExpiry);
        metrics.add(
            registry.registerLongAsyncCounter(
                "es.indices.get.total",
                "Total number of get operations",
                "operation",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndices())
                        .map(o -> o.getGet())
                        .map(o -> o.getCount())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.indices.get.time",
                "Time in milliseconds spent performing get operations.",
                "milliseconds",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndices())
                        .map(o -> o.getGet())
                        .map(o -> o.getTimeInMillis())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.indices.search.fetch.total",
                "Total number of fetch operations.",
                "operation",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndices())
                        .map(o -> o.getSearch())
                        .map(o -> o.getTotal())
                        .map(o -> o.getFetchCount())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.indices.search.fetch.time",
                "Time in milliseconds spent performing fetch operations.",
                "milliseconds",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndices())
                        .map(o -> o.getSearch())
                        .map(o -> o.getTotal())
                        .map(o -> o.getFetchTimeInMillis())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.indices.merge.total",
                "Total number of merge operations.",
                "operation",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndices())
                        .map(o -> o.getMerge())
                        .map(o -> o.getTotal())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.indices.merge.time",
                "Time in milliseconds spent performing merge operations.",
                "milliseconds",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndices())
                        .map(o -> o.getMerge())
                        .map(o -> o.getTotalTimeInMillis())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.translog.operations.total",
                "Number of transaction log operations.",
                "operation",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndices())
                        .map(o -> o.getTranslog())
                        .map(o -> o.estimatedNumberOfOperations())
                        .orElse(0)
                )
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.translog.size",
                "Size, in bytes, of the transaction log.",
                "bytes",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndices())
                        .map(o -> o.getTranslog())
                        .map(o -> o.getTranslogSizeInBytes())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.translog.uncommitted_operations.total",
                "Number of uncommitted transaction log operations.",
                "operations",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndices())
                        .map(o -> o.getTranslog())
                        .map(o -> o.getUncommittedOperations())
                        .orElse(0)
                )
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.translog.uncommitted_operations.size",
                "Size, in bytes, of uncommitted transaction log operations.",
                "bytes",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndices())
                        .map(o -> o.getTranslog())
                        .map(o -> o.getUncommittedSizeInBytes())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.translog.earliest_last_modified.time",
                "Earliest last modified age for the transaction log.",
                "time",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndices())
                        .map(o -> o.getTranslog())
                        .map(o -> o.getEarliestLastModifiedAge())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.transport.rx.size",
                "Size, in bytes, of RX packets received by the node during internal cluster communication.",
                "bytes",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getTransport())
                        .map(o -> o.getRxSize())
                        .map(o -> o.getBytes())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.transport.tx.size",
                "Size, in bytes, of TX packets sent by the node during internal cluster communication.",
                "bytes",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getTransport())
                        .map(o -> o.getTxSize())
                        .map(o -> o.getBytes())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.jvm.mem.pools.young.size",
                "Memory, in bytes, used by the young generation heap.",
                "bytes",
                () -> new LongWithAttributes(
                    bytesUsedByGCGen(Optional.ofNullable(stats.getOrRefresh()).map(o -> o.getJvm()).map(o -> o.getMem()), GcNames.YOUNG)
                )
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.jvm.mem.pools.survivor.size",
                "Memory, in bytes, used by the survivor space.",
                "bytes",
                () -> new LongWithAttributes(
                    bytesUsedByGCGen(Optional.ofNullable(stats.getOrRefresh()).map(o -> o.getJvm()).map(o -> o.getMem()), GcNames.SURVIVOR)
                )
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.jvm.mem.pools.old.size",
                "Memory, in bytes, used by the old generation heap.",
                "bytes",
                () -> new LongWithAttributes(
                    bytesUsedByGCGen(Optional.ofNullable(stats.getOrRefresh()).map(o -> o.getJvm()).map(o -> o.getMem()), GcNames.OLD)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.fs.io_stats.time.total",
                "The total time in millis spent performing I/O operations across all devices used by Elasticsearch.",
                "milliseconds",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getFs())
                        .map(o -> o.getIoStats())
                        .map(o -> o.getTotalIOTimeMillis())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.indexing.docs.total",
                "Total number of indexed documents",
                "documents",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndices())
                        .map(o -> o.getIndexing())
                        .map(o -> o.getTotal())
                        .map(o -> o.getIndexCount())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.indexing.docs.current.total",
                "Current number of indexing documents",
                "documents",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndices())
                        .map(o -> o.getIndexing())
                        .map(o -> o.getTotal())
                        .map(o -> o.getIndexCurrent())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.indexing.indexing.failed.total",
                "Total number of failed indexing operations",
                "operations",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndices())
                        .map(o -> o.getIndexing())
                        .map(o -> o.getTotal())
                        .map(o -> o.getIndexFailedCount())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.indexing.indexing.failed.version_conflict.total",
                "Total number of failed indexing operations due to version conflict",
                "operations",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndices())
                        .map(o -> o.getIndexing())
                        .map(o -> o.getTotal())
                        .map(o -> o.getIndexFailedDueToVersionConflictCount())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.indexing.deletion.docs.total",
                "Total number of deleted documents",
                "documents",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndices())
                        .map(o -> o.getIndexing())
                        .map(o -> o.getTotal())
                        .map(o -> o.getDeleteCount())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.indexing.deletion.docs.current.total",
                "Current number of deleting documents",
                "documents",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndices())
                        .map(o -> o.getIndexing())
                        .map(o -> o.getTotal())
                        .map(o -> o.getDeleteCurrent())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.indexing.time",
                "Total indices indexing time",
                "milliseconds",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndices())
                        .map(o -> o.getIndexing())
                        .map(o -> o.getTotal())
                        .map(o -> o.getIndexTime())
                        .map(o -> o.millis())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.deletion.time",
                "Total indices deletion time",
                "milliseconds",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndices())
                        .map(o -> o.getIndexing())
                        .map(o -> o.getTotal())
                        .map(o -> o.getDeleteTime())
                        .map(o -> o.millis())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.indices.throttle.time",
                "Total indices throttle time",
                "milliseconds",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndices())
                        .map(o -> o.getIndexing())
                        .map(o -> o.getTotal())
                        .map(o -> o.getThrottleTime())
                        .map(o -> o.millis())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.indices.noop.total",
                "Total number of noop shard operations",
                "operations",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndices())
                        .map(o -> o.getIndexing())
                        .map(o -> o.getTotal())
                        .map(o -> o.getNoopUpdateCount())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.indexing.coordinating_operations.size",
                "Total number of memory bytes consumed by coordinating operations",
                "bytes",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndexingPressureStats())
                        .map(o -> o.getTotalCoordinatingBytes())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.indexing.coordinating_operations.total",
                "Total number of coordinating operations",
                "operations",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndexingPressureStats())
                        .map(o -> o.getTotalCoordinatingOps())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.indexing.coordinating_operations.current.size",
                "Current number of memory bytes consumed by coordinating operations",
                "bytes",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndexingPressureStats())
                        .map(o -> o.getCurrentCoordinatingBytes())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.indexing.coordinating_operations.current.total",
                "Current number of coordinating operations",
                "operations",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndexingPressureStats())
                        .map(o -> o.getCurrentCoordinatingOps())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.indexing.coordinating_operations.rejections.total",
                "Total number of coordinating operations rejections",
                "operations",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndexingPressureStats())
                        .map(o -> o.getCoordinatingRejections())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.indexing.coordinating_operations.requests.total",
                "Total number of coordinating requests",
                "operations",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(NodeStats::getIndexingPressureStats)
                        .map(IndexingPressureStats::getTotalCoordinatingRequests)
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.indexing.primary_operations.size",
                "Total number of memory bytes consumed by primary operations",
                "bytes",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndexingPressureStats())
                        .map(o -> o.getTotalPrimaryBytes())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.indexing.primary_operations.total",
                "Total number of primary operations",
                "operations",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndexingPressureStats())
                        .map(o -> o.getTotalPrimaryOps())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.indexing.primary_operations.current.size",
                "Current number of memory bytes consumed by primary operations",
                "bytes",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndexingPressureStats())
                        .map(o -> o.getCurrentPrimaryBytes())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.indexing.primary_operations.current.total",
                "Current number of primary operations",
                "operations",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndexingPressureStats())
                        .map(o -> o.getCurrentPrimaryOps())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.indexing.primary_operations.rejections.total",
                "Total number of primary operations rejections",
                "operations",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(o -> o.getIndexingPressureStats())
                        .map(o -> o.getPrimaryRejections())
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.indexing.primary_operations.document.rejections.total",
                "Total number of rejected indexing documents",
                "operations",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(NodeStats::getIndexingPressureStats)
                        .map(IndexingPressureStats::getPrimaryDocumentRejections)
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "es.indexing.memory.limit.size",
                "Current memory limit for primary and coordinating operations",
                "bytes",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh()).map(o -> o.getIndexingPressureStats()).map(o -> o.getMemoryLimit()).orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.indexing.coordinating.low_watermark_splits.total",
                "Total number of times bulk requests are split due to SPLIT_BULK_LOW_WATERMARK",
                "operations",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(NodeStats::getIndexingPressureStats)
                        .map(IndexingPressureStats::getLowWaterMarkSplits)
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.indexing.coordinating.high_watermark_splits.total",
                "Total number of times bulk requests are split due to SPLIT_BULK_HIGH_WATERMARK",
                "operations",
                () -> new LongWithAttributes(
                    Optional.ofNullable(stats.getOrRefresh())
                        .map(NodeStats::getIndexingPressureStats)
                        .map(IndexingPressureStats::getHighWaterMarkSplits)
                        .orElse(0L)
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.flush.total.time",
                "The total time flushes have been executed excluding waiting time on locks",
                "milliseconds",
                () -> new LongWithAttributes(
                    stats.getOrRefresh() != null ? stats.getOrRefresh().getIndices().getFlush().getTotalTimeInMillis() : 0L
                )
            )
        );

        metrics.add(
            registry.registerLongAsyncCounter(
                "es.flush.total_excluding_lock_waiting.time",
                "The total time flushes have been executed excluding waiting time on locks",
                "milliseconds",
                () -> new LongWithAttributes(
                    stats.getOrRefresh() != null
                        ? stats.getOrRefresh().getIndices().getFlush().getTotalTimeExcludingWaitingOnLockMillis()
                        : 0L
                )
            )
        );
    }

    /**
     * Retrieves the bytes used by a specific garbage collection generation from the provided JvmStats.Mem.
     *
     * @param optionalMem The JvmStats.Mem containing memory pool information.
     * @param name        The name of the garbage collection generation (e.g., "young", "survivor", "old").
     * @return The number of bytes used by the specified garbage collection generation.
     */
    private long bytesUsedByGCGen(Optional<JvmStats.Mem> optionalMem, String name) {
        return optionalMem.map(mem -> {
            long bytesUsed = 0;
            for (JvmStats.MemoryPool pool : mem) {
                if (pool.getName().equals(name)) {
                    bytesUsed = pool.getUsed().getBytes();
                }
            }
            return bytesUsed;
        }).orElse(0L);
    }

    /**
     * Retrieves the current NodeStats for the Elasticsearch node.
     *
     * @return The current NodeStats.
     */
    private NodeStats getNodeStats() {
        CommonStatsFlags flags = new CommonStatsFlags(
            CommonStatsFlags.Flag.Indexing,
            CommonStatsFlags.Flag.Flush,
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
            true,
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
        private static final NodeStats MISSING_NODE_STATS = new NodeStats(
            new DiscoveryNode(
                "_na",
                "_na",
                new TransportAddress(InetAddress.getLoopbackAddress(), 0),
                Map.of(),
                Set.of(),
                VersionInformation.CURRENT
            ),
            0,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        private boolean refresh;

        NodeStatsCache(TimeValue interval) {
            super(interval, MISSING_NODE_STATS);
            this.refresh = true;
        }

        @Override
        protected NodeStats refresh() {
            return refresh ? getNodeStats() : getNoRefresh();
        }

        @Override
        protected boolean needsRefresh() {
            return getNoRefresh() == MISSING_NODE_STATS || super.needsRefresh();
        }

        public void stopRefreshing() {
            this.refresh = false;
        }
    }
}
