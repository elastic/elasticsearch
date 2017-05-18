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

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Statistics about usage of shards with phantom engines summarized per index.
 */
public class PhantomIndexStats implements ToXContent {

    final String index;
    final List<PhantomShardStats> shardStats = new ArrayList<>();

    private long usedHeapSize;
    private long actualHeapSize;
    private int loadShardCount;

    public PhantomIndexStats(String index) {
        this.index = index;
    }

    public long usedHeapSize() {
        return usedHeapSize;
    }

    public long actualHeapSize() {
        return actualHeapSize;
    }

    public int loadShardCount() {
        return loadShardCount;
    }

    public void add(PhantomShardStats shard) {
        shardStats.add(shard);
        actualHeapSize += shard.heapSizeInBytes();
        usedHeapSize += shard.isLoaded() ? shard.heapSizeInBytes() : 0;
        loadShardCount += shard.isLoaded() ? 1 : 0;
    }

    Map<Integer, List<PhantomShardStats>> getStatsGroupedByShardId() {
        Map<Integer, List<PhantomShardStats>> groupedStats = new HashMap<>();

        for (PhantomShardStats shardStat : shardStats) {
            List<PhantomShardStats> shardStatsList = groupedStats.get(shardStat.shardId().id());
            if (shardStatsList == null) {
                shardStatsList = new ArrayList<>();
                groupedStats.put(shardStat.shardId().id(), shardStatsList);
            }
            shardStatsList.add(shardStat);
        }

        return groupedStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }
}
