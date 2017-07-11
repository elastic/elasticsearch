/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
     * @param numberOfShards
     *            The number of shards being queried.
     * @return A suggested default for the size of any shard-side PriorityQueues
     */
    public static int suggestShardSideQueueSize(int finalSize, int numberOfShards) {
        if (finalSize < 1) {
            throw new IllegalArgumentException("size must be positive, got " + finalSize);
        }
        if (numberOfShards < 1) {
            throw new IllegalArgumentException("number of shards must be positive, got " + numberOfShards);
        }

        if (numberOfShards == 1) {
            // In the case of a single shard, we do not need to over-request
            return finalSize;
        }

        // Request 50% more buckets on the shards in order to improve accuracy
        // as well as a small constant that should help with small values of 'size'
        final long shardSampleSize = (long) (finalSize * 1.5 + 10);
        return (int) Math.min(Integer.MAX_VALUE, shardSampleSize);
    }
    
}
