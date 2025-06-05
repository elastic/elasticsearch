/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.compute.lucene.LuceneSliceQueue;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverStatus;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.core.expression.Expression;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Holds the pragmas for an ESQL query. Just a wrapper of settings for now.
 */
public final class QueryPragmas implements Writeable {
    public static final Setting<Integer> EXCHANGE_BUFFER_SIZE = Setting.intSetting("exchange_buffer_size", 10);
    public static final Setting<Integer> EXCHANGE_CONCURRENT_CLIENTS = Setting.intSetting("exchange_concurrent_clients", 2);
    public static final Setting<Integer> ENRICH_MAX_WORKERS = Setting.intSetting("enrich_max_workers", 1);

    private static final Setting<Integer> TASK_CONCURRENCY = Setting.intSetting(
        "task_concurrency",
        ThreadPool.searchOrGetThreadPoolSize(EsExecutors.allocatedProcessors(Settings.EMPTY))
    );

    /**
     * How to cut {@link LuceneSliceQueue slices} to cut each shard into. Is parsed to
     * the enum {@link DataPartitioning} which has more documentation. Not an
     * {@link Setting#enumSetting} because those can't have {@code null} defaults.
     * {@code null} here means "use the default from the cluster setting
     * named {@link EsqlPlugin#DEFAULT_DATA_PARTITIONING}."
     */
    public static final Setting<String> DATA_PARTITIONING = Setting.simpleString("data_partitioning");

    /**
     * Size of a page in entries with {@code 0} being a special value asking
     * to adaptively size based on the number of columns in the page.
     */
    public static final Setting<Integer> PAGE_SIZE = Setting.intSetting("page_size", 0, 0);

    /**
     * The minimum interval between syncs of the {@link DriverStatus}, making
     * the status available to task API.
     */
    public static final Setting<TimeValue> STATUS_INTERVAL = Setting.timeSetting("status_interval", Driver.DEFAULT_STATUS_INTERVAL);

    public static final Setting<Integer> MAX_CONCURRENT_NODES_PER_CLUSTER = //
        Setting.intSetting("max_concurrent_nodes_per_cluster", -1, -1);
    public static final Setting<Integer> MAX_CONCURRENT_SHARDS_PER_NODE = //
        Setting.intSetting("max_concurrent_shards_per_node", 10, 1, 100);

    public static final Setting<Integer> UNAVAILABLE_SHARD_RESOLUTION_ATTEMPTS = //
        Setting.intSetting("unavailable_shard_resolution_attempts", 10, -1);

    public static final Setting<Boolean> NODE_LEVEL_REDUCTION = Setting.boolSetting("node_level_reduction", true);

    public static final Setting<ByteSizeValue> FOLD_LIMIT = Setting.memorySizeSetting("fold_limit", "5%");

    public static final QueryPragmas EMPTY = new QueryPragmas(Settings.EMPTY);

    private final Settings settings;

    public QueryPragmas(Settings settings) {
        this.settings = settings;
    }

    public QueryPragmas(StreamInput in) throws IOException {
        this.settings = Settings.readSettingsFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        settings.writeTo(out);
    }

    public Settings getSettings() {
        return settings;
    }

    public int exchangeBufferSize() {
        return EXCHANGE_BUFFER_SIZE.get(settings);
    }

    public int concurrentExchangeClients() {
        return EXCHANGE_CONCURRENT_CLIENTS.get(settings);
    }

    public DataPartitioning dataPartitioning(DataPartitioning defaultDataPartitioning) {
        String partitioning = DATA_PARTITIONING.get(settings);
        if (partitioning.isEmpty()) {
            return defaultDataPartitioning;
        }
        return DataPartitioning.valueOf(partitioning.toUpperCase(Locale.ROOT));
    }

    public int taskConcurrency() {
        return TASK_CONCURRENCY.get(settings);
    }

    /**
     * Size of a page in entries with {@code 0} being a special value asking
     * to adaptively size based on the number of columns in the page.
     */
    public int pageSize() {
        return PAGE_SIZE.get(settings);
    }

    /**
     * The minimum interval between syncs of the {@link DriverStatus}, making
     * the status available to task API.
     */
    public TimeValue statusInterval() {
        return STATUS_INTERVAL.get(settings);
    }

    /**
     * Returns the maximum number of workers for enrich lookup. A higher number of workers reduces latency but increases cluster load.
     * Defaults to 1.
     */
    public int enrichMaxWorkers() {
        return ENRICH_MAX_WORKERS.get(settings);
    }

    /**
     * The maximum number of nodes to be queried at once by this query. This is safeguard to avoid overloading the cluster.
     */
    public int maxConcurrentNodesPerCluster() {
        return MAX_CONCURRENT_NODES_PER_CLUSTER.get(settings);
    }

    /**
     * The maximum number of shards can be executed concurrently on a single node by this query. This is a safeguard to avoid
     * opening and holding many shards (equivalent to many file descriptors) or having too many field infos created by a single query.
     */
    public int maxConcurrentShardsPerNode() {
        return MAX_CONCURRENT_SHARDS_PER_NODE.get(settings);
    }

    /**
     * Amount of attempts moved shards could be retried.
     * This setting is protecting query from endlessly chasing moving shards.
     */
    public int unavailableShardResolutionAttempts() {
        return UNAVAILABLE_SHARD_RESOLUTION_ATTEMPTS.get(settings);
    }

    /**
     * Returns true if each data node should perform a local reduction for sort, limit, topN, stats or false if the coordinator node
     * will perform the reduction.
     */
    public boolean nodeLevelReduction() {
        return NODE_LEVEL_REDUCTION.get(settings);
    }

    /**
     * The maximum amount of memory we can use for {@link Expression#fold} during planing. This
     * defaults to 5% of memory available on the current node. If this method is called on the
     * coordinating node, this is 5% of the coordinating node's memory. If it's called on a data
     * node, it's 5% of the data node. That's an <strong>exciting</strong> inconsistency. But it's
     * important. Bigger nodes have more space to do folding.
     */
    public ByteSizeValue foldLimit() {
        return FOLD_LIMIT.get(settings);
    }

    public boolean isEmpty() {
        return settings.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryPragmas pragmas = (QueryPragmas) o;
        return settings.equals(pragmas.settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(settings);
    }

    @Override
    public String toString() {
        return settings.toString();
    }
}
