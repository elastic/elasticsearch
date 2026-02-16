/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Split-based partitioning for distributed data source execution.
 *
 * <p>This package provides the machinery for converting discovered splits (files, key ranges,
 * shards) into balanced partitions for parallel execution across cluster data nodes.
 *
 * <h2>Core Types</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.partitioning.DataSourceSplit} — interface
 *       for discovered units of work with optional size estimates and node affinity</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.partitioning.SplitPartitioner} — reusable
 *       discover → group → wrap pipeline helper</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.partitioning.DistributionHints} — planner
 *       hints for partitioning (target parallelism, available nodes, locality preference)</li>
 *   <li>{@link org.elasticsearch.xpack.esql.datasource.partitioning.NodeAffinity} — hard (required)
 *       vs soft (preferred) node affinity for splits and partitions</li>
 * </ul>
 *
 * <h2>Algorithm</h2>
 *
 * <p>{@link org.elasticsearch.xpack.esql.datasource.partitioning.SizeAwareBinPacking} provides
 * the default grouping strategy: first-fit decreasing (FFD) bin-packing when size estimates are
 * available, count-based round-robin fallback otherwise. Required node affinity is always enforced;
 * preferred affinity is honored when
 * {@link org.elasticsearch.xpack.esql.datasource.partitioning.DistributionHints#preferDataLocality()}
 * is true.
 *
 * @see org.elasticsearch.xpack.esql.datasource.DataSource#planPartitions
 * @see org.elasticsearch.xpack.esql.datasource.DataSourcePartition
 */
package org.elasticsearch.xpack.esql.datasource.partitioning;
