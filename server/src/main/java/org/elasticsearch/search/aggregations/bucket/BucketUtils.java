/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket;

/**
 * Helper functions for common Bucketing functions
 */
public final class BucketUtils {

    private BucketUtils() {}

    /**
     * Heuristic used to determine the size of shard-side PriorityQueues when
     * selecting the top N terms from a distributed index.
     *
     * @param finalSize
     *            The number of terms required in the final reduce phase.
     * @return A suggested default for the size of any shard-side PriorityQueues
     */
    public static int suggestShardSideQueueSize(int finalSize) {
        if (finalSize < 1) {
            throw new IllegalArgumentException("size must be positive, got " + finalSize);
        }
        // Request 50% more buckets on the shards in order to improve accuracy
        // as well as a small constant that should help with small values of 'size'
        final long shardSampleSize = (long) (finalSize * 1.5 + 10);
        return (int) Math.min(Integer.MAX_VALUE, shardSampleSize);
    }
}
