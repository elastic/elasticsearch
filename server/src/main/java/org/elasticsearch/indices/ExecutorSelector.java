/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;

/**
 * Some operations need to use different executors for different index patterns.
 * Specifically, some operations on system indices are considered critical and
 * should use the "system_critical_read" or "system_critical_write" thread pools
 * rather than the "system_read" or "system_write" thread pools.
 */
public class ExecutorSelector {

    private final SystemIndices systemIndices;

    /**
     * Package-private constructor; in general it's best to get an ExecutorSelector
     * from {@link SystemIndices#getExecutorSelector()}.
     * @param systemIndices A system indices object that this ExecutorSelector will
     *                      use to match system index names to system index descriptors.
     */
    ExecutorSelector(SystemIndices systemIndices) {
        this.systemIndices = systemIndices;
    }

    /**
     * The "get" executor should be used when retrieving documents by ID.
     * @param indexName Name of the index
     * @return Name of the executor to use for a get operation.
     */
    public String executorForGet(String indexName) {
        SystemIndexDescriptor indexDescriptor = systemIndices.findMatchingDescriptor(indexName);
        if (Objects.nonNull(indexDescriptor)) {
            return indexDescriptor.getThreadPoolNames().threadPoolForGet();
        }

        SystemDataStreamDescriptor dataStreamDescriptor = systemIndices.findMatchingDataStreamDescriptor(indexName);
        if (Objects.nonNull(dataStreamDescriptor)) {
            return dataStreamDescriptor.getThreadPoolNames().threadPoolForGet();
        }

        return ThreadPool.Names.GET;
    }

    /**
     * The "search" executor should be used for search or aggregation operations.
     * @param indexName Name of the index
     * @return Name of the executor to use for a search operation.
     */
    public String executorForSearch(String indexName) {
        SystemIndexDescriptor indexDescriptor = systemIndices.findMatchingDescriptor(indexName);
        if (Objects.nonNull(indexDescriptor)) {
            return indexDescriptor.getThreadPoolNames().threadPoolForSearch();
        }

        SystemDataStreamDescriptor dataStreamDescriptor = systemIndices.findMatchingDataStreamDescriptor(indexName);
        if (Objects.nonNull(dataStreamDescriptor)) {
            return dataStreamDescriptor.getThreadPoolNames().threadPoolForSearch();
        }

        return ThreadPool.Names.SEARCH;
    }

    /**
     * The "write" executor should be used for operations that write new documents or
     * update existing ones.
     * @param indexName Name of the index
     * @return Name of the executor to use for a search operation.
     */
    public String executorForWrite(String indexName) {
        SystemIndexDescriptor indexDescriptor = systemIndices.findMatchingDescriptor(indexName);
        if (Objects.nonNull(indexDescriptor)) {
            return indexDescriptor.getThreadPoolNames().threadPoolForWrite();
        }

        SystemDataStreamDescriptor dataStreamDescriptor = systemIndices.findMatchingDataStreamDescriptor(indexName);
        if (Objects.nonNull(dataStreamDescriptor)) {
            return dataStreamDescriptor.getThreadPoolNames().threadPoolForWrite();
        }

        return ThreadPool.Names.WRITE;
    }

    /**
     * This is a convenience method for the case when we need to find an executor for a shard.
     * Note that it can be passed to methods as a {@link java.util.function.BiFunction}.
     * @param executorSelector An executor selector service.
     * @param shard A shard for which we need to find an executor.
     * @return Name of the executor that should be used for write operations on this shard.
     */
    public static String getWriteExecutorForShard(ExecutorSelector executorSelector, IndexShard shard) {
        return executorSelector.executorForWrite(shard.shardId().getIndexName());
    }
}
