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

package org.elasticsearch.cluster;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static java.lang.Math.floor;
import static java.lang.Math.log10;

/**
 * ClusterInfo contains information like {@link DiskUsage}, shard sizes, and
 * average shard sizes.
 */
public class ClusterInfo {
    private final ImmutableMap<String, DiskUsage> usages;
    private final ImmutableMap<String, Long> shardSizes;
    private final ImmutableMap<String, Long> indexToAverageShardSize;
    private final ShardsBucketedBySize shardsBucketedBySize;

    public ClusterInfo(ImmutableMap<String, DiskUsage> usages, ImmutableMap<String, Long> shardSizes,
            ImmutableMap<String, Long> indexToAverageShardSize, ShardsBucketedBySize shardsBucketedBySize) {
        this.usages = usages;
        this.shardSizes = shardSizes;
        this.indexToAverageShardSize = indexToAverageShardSize;
        this.shardsBucketedBySize = shardsBucketedBySize;

    }

    public Map<String, DiskUsage> getNodeDiskUsages() {
        return usages;
    }

    /**
     * @return map from {@link InternalClusterInfoService#shardIdentifierFromRouting(String)} to shard size in bytes
     */
    public Map<String, Long> getShardSizes() {
        return shardSizes;
    }

    /**
     * @return map from index name to average shard size
     */
    public ImmutableMap<String, Long> getIndexToAverageShardSize() {
        return indexToAverageShardSize;
    }

    /**
     * @return looks up all shards (as
     *         {@link InternalClusterInfoService#shardIdentifierFromRouting(String)}
     *         ) that fall into size range buckets
     */
    public ShardsBucketedBySize getShardsBucketedBySize() {
        return shardsBucketedBySize;
    }

    /**
     * Looks up all shards that fall into size range buckets.
     */
    public static class ShardsBucketedBySize {
        public static Builder builder(long smallShardLimit) {
            return new Builder(smallShardLimit);
        }

        private ImmutableListMultimap<Integer, String> bucketToShard;
        private final long smallShardLimit;

        private ShardsBucketedBySize(ImmutableListMultimap<Integer, String> bucketToShard, long smallShardLimit) {
            this.bucketToShard = bucketToShard;
            this.smallShardLimit = smallShardLimit;
        }

        /**
         * @param size size in bytes
         * @return get all shards in the same bucket as a shard with this size
         */
        public ImmutableList<String> shardsInSameBucket(long size) {
            return bucketToShard.get(bucket(smallShardLimit, size));
        }

        /**
         * @return does this object store no shard information at all?
         */
        public boolean isEmpty() {
            return bucketToShard.isEmpty();
        }

        static int bucket(long smallShardLimit, long size) {
            return size <= smallShardLimit ? 0 : (int)floor(log10(size));
        }

        public static class Builder {
            private final ImmutableListMultimap.Builder<Integer, String> bucketToShard = ImmutableListMultimap.builder();
            private final long smallShardLimit;

            private Builder(long smallShardLimit) {
                this.smallShardLimit = smallShardLimit;
            }

            public void add(long size, String id) {
                bucketToShard.put(bucket(smallShardLimit, size), id);
            }

            public ShardsBucketedBySize build() {
                return new ShardsBucketedBySize(bucketToShard.build(), smallShardLimit);
            }
        }
    }
}
