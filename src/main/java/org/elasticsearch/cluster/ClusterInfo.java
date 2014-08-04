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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;

import java.util.Map;

/**
 * ClusterInfo contains information like {@link DiskUsage}, shard sizes, and
 * average shard sizes.
 */
public class ClusterInfo {
    private final ImmutableMap<String, DiskUsage> usages;
    private final ImmutableMap<String, Long> shardSizes;
    private final ImmutableMap<String, Long> indexToAverageShardSize;
    private final ImmutableSetMultimap<Integer, String> shardSizeBinToShard;

    public ClusterInfo(ImmutableMap<String, DiskUsage> usages, ImmutableMap<String, Long> shardSizes,
            ImmutableMap<String, Long> indexToAverageShardSize, ImmutableSetMultimap<Integer, String> shardSizeBinToShard) {
        this.usages = usages;
        this.shardSizes = shardSizes;
        this.indexToAverageShardSize = indexToAverageShardSize;
        this.shardSizeBinToShard = shardSizeBinToShard;

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
     * @return multimap from a
     *         {@link InternalClusterInfoService#shardBinBySize(long)} to
     *         {@link InternalClusterInfoService#shardIdentifierFromRouting(String)}
     *         .
     */
    public ImmutableSetMultimap<Integer, String> getShardSizeBinToShard() {
        return shardSizeBinToShard;
    }
}
