/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.compute.operator.Driver;

import java.util.List;
import java.util.function.Function;

/**
 * How we partition the data across {@link Driver}s. Each request forks into
 * {@code min(1.5 * cpus, partition_count)} threads on the data node. More partitions
 * allow us to bring more threads to bear on CPU intensive data node side tasks.
 */
public enum DataPartitioning {
    /**
     * Automatically select the data partitioning based on the query and index.
     * Usually that's {@link #SEGMENT}, but for small indices it's {@link #SHARD}.
     * When the additional overhead from {@link #DOC} is fairly low then it'll
     * pick {@link #DOC}.
     */
    AUTO,
    /**
     * Make one partition per shard. This is generally the slowest option, but it
     * has the lowest CPU overhead.
     */
    SHARD,
    /**
     * Partition on segment boundaries, this doesn't allow forking to as many CPUs
     * as {@link #DOC} but it has much lower overhead.
     * <p>
     * It packs segments smaller than {@link LuceneSliceQueue#MAX_DOCS_PER_SLICE}
     * docs together into a partition. Larger segments get their own partition.
     * Each slice contains no more than {@link LuceneSliceQueue#MAX_SEGMENTS_PER_SLICE}.
     */
    SEGMENT,
    /**
     * Partitions into dynamic-sized slices to improve CPU utilization while keeping overhead low.
     * This approach is more flexible than {@link #SEGMENT} and works as follows:
     *
     * <ol>
     *   <li>The slice size starts from a desired size based on {@code task_concurrency} but is capped
     *       at around {@link LuceneSliceQueue#MAX_DOCS_PER_SLICE}. This prevents poor CPU usage when
     *       matching documents are clustered together.</li>
     *   <li>For small and medium segments (less than five times the desired slice size), it uses a
     *       slightly different {@link #SEGMENT} strategy, which also splits segments that are larger
     *       than the desired size. See {@link org.apache.lucene.search.IndexSearcher#slices(List, int, int, boolean)}.</li>
     *   <li>For very large segments, multiple segments are not combined into a single slice. This allows
     *       one driver to process an entire large segment until other drivers steal the work after finishing
     *       their own tasks. See {@link LuceneSliceQueue#nextSlice(LuceneSlice)}.</li>
     * </ol>
     */
    DOC;

    @FunctionalInterface
    public interface AutoStrategy {
        Function<Query, LuceneSliceQueue.PartitioningStrategy> pickStrategy(int limit);

        AutoStrategy DEFAULT = LuceneSourceOperator.Factory::autoStrategy;
        AutoStrategy DEFAULT_TIME_SERIES = limit -> {
            if (limit == LuceneOperator.NO_LIMIT) {
                return q -> LuceneSliceQueue.PartitioningStrategy.DOC;
            } else {
                return DEFAULT.pickStrategy(limit);
            }
        };
    }
}
