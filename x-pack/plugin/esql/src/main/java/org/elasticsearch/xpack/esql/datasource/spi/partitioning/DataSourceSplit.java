/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.spi.partitioning;

import java.util.OptionalLong;

/**
 * A discovered unit of work that can be grouped into partitions for distributed execution.
 *
 * <p>Splits represent the raw units discovered during partition planning — files, key ranges,
 * shards, API handles, etc. They are intermediate planning types: {@link SplitPartitioner}
 * groups them into balanced bins, and the data source then wraps each bin into a
 * {@link org.elasticsearch.xpack.esql.datasource.spi.DataSourcePartition DataSourcePartition}
 * for serialization and execution.
 *
 * <p>All methods have defaults, so minimal implementations only need to implement the marker
 * interface. Providing {@link #estimatedBytes()} enables size-aware bin-packing in
 * {@link SplitPartitioner}; without it, the algorithm falls back to
 * count-based round-robin.
 *
 * @see SplitPartitioner
 * @see org.elasticsearch.xpack.esql.datasource.spi.DataSourcePartition
 */
public interface DataSourceSplit {

    /**
     * Estimated size of this split in bytes.
     *
     * <p>Used by {@link SplitPartitioner} for size-aware bin-packing.
     * When absent across all splits, the algorithm falls back to count-based grouping.
     */
    default OptionalLong estimatedBytes() {
        return OptionalLong.empty();
    }

    /**
     * Estimated number of rows in this split.
     */
    default OptionalLong estimatedRows() {
        return OptionalLong.empty();
    }

    /**
     * Node affinity for this split — where it should or must execute.
     *
     * <p>{@link NodeAffinity#require} splits are always grouped by node (hard constraint).
     * {@link NodeAffinity#prefer} splits are grouped by node only when
     * {@link DistributionHints#preferDataLocality()} is true (soft preference).
     *
     * @see NodeAffinity
     */
    default NodeAffinity nodeAffinity() {
        return NodeAffinity.NONE;
    }
}
