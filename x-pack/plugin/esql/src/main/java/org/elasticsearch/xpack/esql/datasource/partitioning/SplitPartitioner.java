/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.partitioning;

import org.elasticsearch.xpack.esql.datasource.DataSourcePartition;
import org.elasticsearch.xpack.esql.datasource.DataSourcePlan;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Reusable helper that implements the discover → group → wrap pipeline for split-based partitioning.
 *
 * <p>Data sources that discover fine-grained splits (files, key ranges, shards) compose this
 * helper and delegate {@link org.elasticsearch.xpack.esql.datasource.DataSource#planPartitions
 * DataSource.planPartitions()} to {@link #planPartitions}. This avoids spending the single
 * inheritance slot on partitioning behavior.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * private final SplitPartitioner<FileTask> partitioner = new SplitPartitioner<>(
 *     this::discoverFileTasks,
 *     this::createPartition
 * );
 *
 * @Override
 * public List<DataSourcePartition> planPartitions(DataSourcePlan plan, DistributionHints hints) {
 *     return partitioner.planPartitions(plan, hints);
 * }
 * }</pre>
 *
 * <h2>Customization</h2>
 *
 * <p>The two-argument constructor uses {@link SizeAwareBinPacking#groupSplits} for grouping
 * (FFD bin-packing with count-based fallback). Use the three-argument constructor to supply
 * a custom grouping strategy (locality-first, fixed-size, etc.).
 *
 * @param <S> The concrete split type
 * @see DataSourceSplit
 * @see SizeAwareBinPacking
 */
public final class SplitPartitioner<S extends DataSourceSplit> {

    private final Function<DataSourcePlan, List<S>> discover;
    private final BiFunction<List<S>, DistributionHints, List<List<S>>> group;
    private final BiFunction<DataSourcePlan, List<S>, DataSourcePartition> wrap;

    /**
     * Create a partitioner with default grouping ({@link SizeAwareBinPacking#groupSplits}).
     *
     * @param discover Function that discovers splits from a plan
     * @param wrap Function that wraps a group of splits into a partition
     */
    public SplitPartitioner(Function<DataSourcePlan, List<S>> discover, BiFunction<DataSourcePlan, List<S>, DataSourcePartition> wrap) {
        this(discover, SizeAwareBinPacking::groupSplits, wrap);
    }

    /**
     * Create a partitioner with custom grouping.
     *
     * @param discover Function that discovers splits from a plan
     * @param group Function that groups splits into balanced bins
     * @param wrap Function that wraps a group of splits into a partition
     */
    public SplitPartitioner(
        Function<DataSourcePlan, List<S>> discover,
        BiFunction<List<S>, DistributionHints, List<List<S>>> group,
        BiFunction<DataSourcePlan, List<S>, DataSourcePartition> wrap
    ) {
        this.discover = discover;
        this.group = group;
        this.wrap = wrap;
    }

    /**
     * Execute the discover → group → wrap pipeline.
     *
     * @param plan The data source plan to partition
     * @param hints Distribution hints from the physical planner
     * @return List of partitions, or empty if no splits discovered
     */
    public List<DataSourcePartition> planPartitions(DataSourcePlan plan, DistributionHints hints) {
        List<S> splits = discover.apply(plan);
        if (splits.isEmpty()) {
            return List.of();
        }
        List<List<S>> groups = group.apply(splits, hints);
        return groups.stream().map(g -> wrap.apply(plan, g)).toList();
    }
}
