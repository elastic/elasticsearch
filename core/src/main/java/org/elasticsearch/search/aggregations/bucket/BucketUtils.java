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
public class BucketUtils {

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
        assert numberOfShards >= 1;
        if (numberOfShards == 1) {
            return finalSize;
        }
        //Cap the multiplier used for shards to avoid excessive data transfer
        final long shardSampleSize = (long) finalSize * Math.min(10, numberOfShards);
        // When finalSize is very small e.g. 1 and there is a low number of
        // shards then we need to ensure we still gather a reasonable sample of statistics from each
        // shard (at low cost) to improve the chances of the final result being accurate.
        return (int) Math.min(Integer.MAX_VALUE, Math.max(10, shardSampleSize));
    }
    
}
