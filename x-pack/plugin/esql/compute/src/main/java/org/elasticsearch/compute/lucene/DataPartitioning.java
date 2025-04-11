/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.elasticsearch.compute.operator.Driver;

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
     * Partition each shard into {@code task_concurrency} partitions, splitting
     * larger segments into slices. This allows bringing the most CPUs to bear on
     * the problem but adds extra overhead, especially in query preparation.
     */
    DOC,
}
