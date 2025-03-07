/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search.stats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.ExponentiallyWeightedMovingAverage;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.search.internal.SearchContext;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * This class implements the {@link SearchOperationListener} interface to track the execution time of search operations
 * on a per-index basis. It uses a {@link ConcurrentHashMap} to store the execution times and an
 * {@link ExponentiallyWeightedMovingAverage} to calculate the exponentially weighted moving average (EWMA) of the
 * execution times.
 */
public final class ShardSearchPerIndexTimeTrackingMetrics implements SearchOperationListener {

    private static final Logger logger = LogManager.getLogger(ShardSearchPerIndexTimeTrackingMetrics.class);

    private final ConcurrentHashMap<String, Tuple<LongAdder, ExponentiallyWeightedMovingAverage>> indexExecutionTime;

    private final double ewmaAlpha;

    /**
     * Constructs a new ShardSearchPerIndexTimeTrackingMetrics instance with the specified EWMA alpha value.
     *
     * @param ewmaAlpha the alpha value for the EWMA calculation
     */
    public ShardSearchPerIndexTimeTrackingMetrics(double ewmaAlpha) {
        this.indexExecutionTime = new ConcurrentHashMap<>();
        this.ewmaAlpha = ewmaAlpha;
    }

    /**
     * Tracks the execution time of the query phase of a search operation.
     *
     * @param searchContext the search context
     * @param tookInNanos the time taken in nanoseconds
     */
    @Override
    public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
        trackExecutionTime(searchContext, tookInNanos);
    }

    /**
     * Tracks the execution time of a failed query phase of a search operation.
     *
     * @param searchContext the search context
     * @param tookInNanos the time taken in nanoseconds
     */
    @Override
    public void onFailedQueryPhase(SearchContext searchContext, long tookInNanos) {
        trackExecutionTime(searchContext, tookInNanos);
    }

    /**
     * Tracks the execution time of the fetch phase of a search operation.
     *
     * @param searchContext the search context
     * @param tookInNanos the time taken in nanoseconds
     */
    @Override
    public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
        trackExecutionTime(searchContext, tookInNanos);
    }

    /**
     * Tracks the execution time of a failed fetch phase of a search operation.
     *
     * @param searchContext the search context
     * @param tookInNanos the time taken in nanoseconds
     */
    @Override
    public void onFailedFetchPhase(SearchContext searchContext, long tookInNanos) {
        trackExecutionTime(searchContext, tookInNanos);
    }

    /**
     * Tracks the execution time of a search operation.
     *
     * @param searchContext the search context
     * @param tookInNanos the time taken in nanoseconds
     */
    private void trackExecutionTime(SearchContext searchContext, long tookInNanos) {
        IndexShard indexShard = searchContext.indexShard();
        if (indexShard != null && indexShard.isSystem() == false) {
            String indexName = indexShard.shardId().getIndexName();
            if (indexName != null) {
                Tuple<LongAdder, ExponentiallyWeightedMovingAverage> t = indexExecutionTime.computeIfAbsent(
                    indexName,
                    k -> new Tuple<>(new LongAdder(), new ExponentiallyWeightedMovingAverage(ewmaAlpha, 0))
                );
                t.v1().add(tookInNanos);
                t.v2().addValue(tookInNanos);
            }
        }
    }

    /**
     * Gets the total execution time for tasks associated with a specific index.
     *
     * @param indexName the name of the index
     * @return the total execution time for the index
     */
    public long getSearchLoadPerIndex(String indexName) {
        Tuple<LongAdder, ExponentiallyWeightedMovingAverage> t = indexExecutionTime.get(indexName);
        return (t != null) ? t.v1().sum() : 0;
    }

    /**
     * Gets the exponentially weighted moving average (EWMA) of the execution time for tasks associated with a specific index name.
     *
     * @param indexName the name of the index
     * @return the EWMA of the execution time for the index
     */
    public double getLoadEMWAPerIndex(String indexName) {
        Tuple<LongAdder, ExponentiallyWeightedMovingAverage> t = indexExecutionTime.get(indexName);
        return (t != null) ? t.v2().getAverage() : 0;
    }

    /**
     * Stops tracking the execution time for tasks associated with a specific index.
     *
     * @param indexName the name of the index
     */
    public void stopTrackingIndex(String indexName) {
        if (indexExecutionTime.containsKey(indexName)) {
            indexExecutionTime.remove(indexName);
        } else {
            logger.debug("Trying to stop tracking index [{}] that was never tracked", indexName);
        }
    }
}
