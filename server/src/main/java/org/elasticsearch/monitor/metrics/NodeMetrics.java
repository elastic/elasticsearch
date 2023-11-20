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
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.monitor.jvm.GcNames;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongUpDownCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * NodeMetrics monitors various statistics of an Elasticsearch node and exposes them through as metrics
 * the provided MeterRegistry. It includes counters for indices operations, memory usage, transport statistics,
 * and more. The metrics are periodically updated based on a schedule.
 */
public class NodeMetrics {
    private final Logger logger = LogManager.getLogger(NodeMetrics.class);
    private final NodeService nodeService;
    private NodeStats stats;
    private LongCounter indicesGetTotal;
    private LongCounter indicesGetTimeInMillis;
    private LongCounter indicesSearchFetchTotal;
    private LongCounter indicesSearchFetchTimeInMillis;
    private LongCounter indicesMergeTotal;
    private LongCounter indicesMergeTimeInMillis;
    private LongCounter indicesTranslogOperations;
    private LongCounter indicesTranslogSize;
    private LongCounter transportRxSize;
    private LongCounter transportTxSize;
    private LongCounter memPoolsYoungUsed;
    private LongCounter memPoolsSurvivorUsed;
    private LongCounter memPoolsOldUsed;
    private LongUpDownCounter indicesTranslogUncommittedOperations;
    private LongUpDownCounter indicesTranslogUncommittedSize;
    private LongCounter indicesTranslogEarliestLastModifiedAge;
    private LongUpDownCounter httpCurrentOpen;

    /**
     * Constructs a new NodeMetrics instance.
     *
     * @param meterRegistry The MeterRegistry used to register metrics.
     * @param nodeService   The NodeService for interacting with the Elasticsearch node.
     */
    public NodeMetrics(MeterRegistry meterRegistry, NodeService nodeService) {
        this.nodeService = nodeService;
        this.stats = getNodeStats();
        registerMetricsAndSetInitialState(meterRegistry, stats);
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "es-node-metrics"));
        executor.scheduleAtFixedRate(() -> {
            NodeStats updatedStats = getNodeStats();
            updateCounters(updatedStats, stats);
            stats = updatedStats;
        }, 0, 60, TimeUnit.SECONDS);
    }

    /**
     * Retrieves the current NodeStats for the Elasticsearch node with specific flags.
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

    /**
     * Registers metrics in the provided MeterRegistry and sets initial state based on the given NodeStats.
     *
     * @param registry The MeterRegistry used to register and collect metrics.
     * @param stats    The initial NodeStats to set the initial state of the metrics.
     */
    private void registerMetricsAndSetInitialState(MeterRegistry registry, NodeStats stats) {
        indicesGetTotal = registry.registerLongCounter("es.node.stats.indices.get.total", "Total number of get operations", "operation");
        indicesGetTotal.incrementBy(stats.getIndices().getGet().getCount());

        indicesGetTimeInMillis = registry.registerLongCounter(
            "es.node.stats.indices.get.time",
            "Time in milliseconds spent performing get operations.",
            "milliseconds"
        );
        indicesGetTimeInMillis.incrementBy(stats.getIndices().getGet().getTimeInMillis());

        indicesSearchFetchTotal = registry.registerLongCounter(
            "es.node.stats.indices.search.fetch.total",
            "Total number of fetch operations.",
            "operation"
        );
        indicesSearchFetchTotal.incrementBy(stats.getIndices().getSearch().getTotal().getFetchCount());

        indicesSearchFetchTimeInMillis = registry.registerLongCounter(
            "es.node.stats.indices.search.fetch.time",
            "Time in milliseconds spent performing fetch operations.",
            "milliseconds"
        );
        indicesSearchFetchTimeInMillis.incrementBy(stats.getIndices().getSearch().getTotal().getFetchTimeInMillis());

        indicesMergeTotal = registry.registerLongCounter(
            "es.node.stats.indices.merge.total",
            "Total number of merge operations.",
            "operation"
        );
        indicesMergeTotal.incrementBy(stats.getIndices().getMerge().getTotal());

        indicesMergeTimeInMillis = registry.registerLongCounter(
            "es.node.stats.indices.merge.time",
            "Time in milliseconds spent performing merge operations.",
            "milliseconds"
        );
        indicesMergeTimeInMillis.incrementBy(stats.getIndices().getMerge().getTotalTimeInMillis());

        indicesTranslogOperations = registry.registerLongCounter(
            "es.node.stats.indices.translog.operations",
            "Number of transaction log operations.",
            "operation"
        );
        indicesTranslogOperations.incrementBy(stats.getIndices().getTranslog().estimatedNumberOfOperations());

        indicesTranslogSize = registry.registerLongCounter(
            "es.node.stats.indices.translog.size",
            "Size, in bytes, of the transaction log.",
            "bytes"
        );
        indicesTranslogSize.incrementBy(stats.getIndices().getTranslog().getTranslogSizeInBytes());

        indicesTranslogUncommittedOperations = registry.registerLongUpDownCounter(
            "es.node.stats.indices.translog.uncommitted_operations",
            "Number of uncommitted transaction log operations.",
            "operations"
        );
        indicesTranslogUncommittedOperations.add(stats.getIndices().getTranslog().getUncommittedOperations());

        indicesTranslogUncommittedSize = registry.registerLongUpDownCounter(
            "es.node.stats.indices.translog.uncommitted_size",
            "Size, in bytes, of uncommitted transaction log operations.",
            "bytes"
        );
        indicesTranslogUncommittedSize.add(stats.getIndices().getTranslog().getUncommittedSizeInBytes());

        indicesTranslogEarliestLastModifiedAge = registry.registerLongCounter(
            "es.node.stats.indices.translog.earliest_last_modified_age",
            "Earliest last modified age for the transaction log.",
            "time"
        );
        indicesTranslogEarliestLastModifiedAge.incrementBy(stats.getIndices().getTranslog().getEarliestLastModifiedAge());

        httpCurrentOpen = registry.registerLongUpDownCounter(
            "es.node.stats.http.current_open",
            "Current number of open HTTP connections for the node.",
            "connections"
        );
        httpCurrentOpen.add(stats.getHttp().getServerOpen());

        transportRxSize = registry.registerLongCounter(
            "es.node.stats.transport.rx_size",
            "Size, in bytes, of RX packets received by the node during internal cluster communication.",
            "bytes"
        );
        transportRxSize.incrementBy(stats.getTransport().getRxSize().getBytes());

        transportTxSize = registry.registerLongCounter(
            "es.node.stats.transport.tx_size",
            "Size, in bytes, of TX packets sent by the node during internal cluster communication.",
            "bytes"
        );
        transportTxSize.incrementBy(stats.getTransport().getTxSize().getBytes());

        memPoolsYoungUsed = registry.registerLongCounter(
            "es.node.stats.jvm.mem.pools.young.used",
            "Memory, in bytes, used by the young generation heap.",
            "bytes"
        );
        memPoolsYoungUsed.incrementBy(bytesUsedByGCGen(stats.getJvm().getMem(), GcNames.YOUNG));

        memPoolsSurvivorUsed = registry.registerLongCounter(
            "es.node.stats.jvm.mem.pools.survivor.used",
            "Memory, in bytes, used by the survivor space.",
            "bytes"
        );
        memPoolsSurvivorUsed.incrementBy(bytesUsedByGCGen(stats.getJvm().getMem(), GcNames.SURVIVOR));

        memPoolsOldUsed = registry.registerLongCounter(
            "es.node.stats.jvm.mem.pools.old.used",
            "Memory, in bytes, used by the old generation heap.",
            "bytes"
        );
        memPoolsOldUsed.incrementBy(bytesUsedByGCGen(stats.getJvm().getMem(), GcNames.OLD));
    }

    /**
     * Updates the metrics counters based on the changes in NodeStats between the new and old statistics.
     *
     * @param newStats The updated NodeStats representing the current state.
     * @param oldStats The previous NodeStats representing the old state.
     */
    private void updateCounters(NodeStats newStats, NodeStats oldStats) {
        incrementIfDeltaGreaterThanZero(
            indicesGetTotal,
            newStats.getIndices().getGet().getCount(),
            oldStats.getIndices().getGet().getCount()
        );
        incrementIfDeltaGreaterThanZero(
            indicesGetTimeInMillis,
            newStats.getIndices().getGet().getTimeInMillis(),
            oldStats.getIndices().getGet().getTimeInMillis()
        );
        incrementIfDeltaGreaterThanZero(
            indicesSearchFetchTotal,
            newStats.getIndices().getSearch().getTotal().getFetchCount(),
            oldStats.getIndices().getSearch().getTotal().getFetchCount()
        );
        incrementIfDeltaGreaterThanZero(
            indicesSearchFetchTimeInMillis,
            newStats.getIndices().getSearch().getTotal().getFetchTimeInMillis(),
            oldStats.getIndices().getSearch().getTotal().getFetchTimeInMillis()
        );
        incrementIfDeltaGreaterThanZero(
            indicesMergeTotal,
            newStats.getIndices().getMerge().getTotal(),
            oldStats.getIndices().getMerge().getTotal()
        );
        incrementIfDeltaGreaterThanZero(
            indicesMergeTimeInMillis,
            newStats.getIndices().getMerge().getTotalTimeInMillis(),
            oldStats.getIndices().getMerge().getTotalTimeInMillis()
        );
        incrementIfDeltaGreaterThanZero(
            indicesTranslogOperations,
            newStats.getIndices().getTranslog().estimatedNumberOfOperations(),
            oldStats.getIndices().getTranslog().estimatedNumberOfOperations()
        );
        incrementIfDeltaGreaterThanZero(
            indicesTranslogSize,
            newStats.getIndices().getTranslog().getTranslogSizeInBytes(),
            oldStats.getIndices().getTranslog().getTranslogSizeInBytes()
        );
        incrementIfDeltaGreaterThanZero(
            transportRxSize,
            newStats.getTransport().getRxSize().getBytes(),
            oldStats.getTransport().getRxSize().getBytes()
        );
        incrementIfDeltaGreaterThanZero(
            transportTxSize,
            newStats.getTransport().getTxSize().getBytes(),
            oldStats.getTransport().getTxSize().getBytes()
        );
        incrementIfDeltaGreaterThanZero(
            memPoolsYoungUsed,
            bytesUsedByGCGen(newStats.getJvm().getMem(), GcNames.YOUNG),
            bytesUsedByGCGen(oldStats.getJvm().getMem(), GcNames.YOUNG)
        );
        incrementIfDeltaGreaterThanZero(
            memPoolsSurvivorUsed,
            bytesUsedByGCGen(newStats.getJvm().getMem(), GcNames.SURVIVOR),
            bytesUsedByGCGen(oldStats.getJvm().getMem(), GcNames.SURVIVOR)
        );
        incrementIfDeltaGreaterThanZero(
            memPoolsOldUsed,
            bytesUsedByGCGen(newStats.getJvm().getMem(), GcNames.OLD),
            bytesUsedByGCGen(oldStats.getJvm().getMem(), GcNames.OLD)
        );
        incrementIfDeltaGreaterThanZero(
            indicesTranslogEarliestLastModifiedAge,
            newStats.getIndices().getTranslog().getEarliestLastModifiedAge(),
            oldStats.getIndices().getTranslog().getEarliestLastModifiedAge()
        );
        updateUpAndDownCounter(
            indicesTranslogUncommittedOperations,
            newStats.getIndices().getTranslog().getUncommittedOperations(),
            oldStats.getIndices().getTranslog().getUncommittedOperations()
        );
        updateUpAndDownCounter(
            indicesTranslogUncommittedSize,
            newStats.getIndices().getTranslog().getUncommittedSizeInBytes(),
            oldStats.getIndices().getTranslog().getUncommittedSizeInBytes()
        );
        updateUpAndDownCounter(httpCurrentOpen, newStats.getHttp().getServerOpen(), oldStats.getHttp().getServerOpen());
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
     * Increments the given LongCounter if the delta (newValue - oldValue) is greater than zero.
     * Logs an error if the delta is negative, indicating unexpected behavior.
     *
     * @param counter   The LongCounter to be incremented.
     * @param newValue  The new value for the counter.
     * @param oldValue  The old value of the counter.
     */
    private void incrementIfDeltaGreaterThanZero(LongCounter counter, long newValue, long oldValue) {
        long delta = newValue - oldValue;
        if (delta > 0) {
            counter.incrementBy(delta);
        } else if (delta < 0) {
            logger.error("Counter should never be incremented by a negative number");
        }
    }

    /**
     * Updates the given LongUpDownCounter based on the difference between the new and old values.
     *
     * @param counter   The LongUpDownCounter to be updated.
     * @param newValue  The new value for the counter.
     * @param oldValue  The old value of the counter.
     */
    private void updateUpAndDownCounter(LongUpDownCounter counter, long newValue, long oldValue) {
        counter.add(newValue - oldValue);
    }
}
