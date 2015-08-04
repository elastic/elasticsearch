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

import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.ShardRouting;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * ClusterInfo is an object representing a map of nodes to {@link DiskUsage}
 * and a map of shard ids to shard sizes keyed by the {@link AllocationId}
 */
public class ClusterInfo {
    public static final ClusterInfo EMPTY = new ClusterInfo(Collections.EMPTY_MAP, Collections.EMPTY_MAP);
    private final Map<String, DiskUsage> usages;
    private final Map<AllocationId, Long> shardSizes;

    public ClusterInfo(Map<String, DiskUsage> usages, Map<AllocationId, Long> shardSizes) {
        this.usages = usages;
        this.shardSizes = shardSizes;
    }

    public Map<String, DiskUsage> getNodeDiskUsages() {
        return this.usages;
    }

    public Long getShardSize(ShardRouting shardRouting) {
        return shardSizes.get(shardRouting.allocationId());
    }

    final int getNumShardSizes() { // for testing
        return shardSizes.size();
    }

    final Collection<Long> getShardSizeValues() { // for testing
        return shardSizes.values();
    }
}