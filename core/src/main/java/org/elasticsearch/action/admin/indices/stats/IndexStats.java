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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class IndexStats implements Iterable<IndexShardStats> {

    private final String index;

    private final ShardStats shards[];

    private final CommonStats total;

    private final CommonStats primary;

    private final Map<Integer, IndexShardStats> indexShards;

    public IndexStats(String index, CommonStatsFlags flags, ShardStats[] shards) {
        this.index = index;
        this.shards = shards;
        this.total = ShardStats.calculateTotalStats(shards, flags);
        this.primary = ShardStats.calculatePrimaryStats(shards, flags);
        Map<Integer, List<ShardStats>> tmpIndexShards = new HashMap<>();
        for (ShardStats shard : shards) {
            List<ShardStats> shardStatList = tmpIndexShards.computeIfAbsent(shard.getShardRouting().id(), integer -> new ArrayList<>());
            shardStatList.add(shard);
        }
        Map<Integer, IndexShardStats> indexShardList = new HashMap<>();
        for (Map.Entry<Integer, List<ShardStats>> entry : tmpIndexShards.entrySet()) {
            indexShardList.put(entry.getKey(), new IndexShardStats(entry.getValue().get(0).getShardRouting().shardId(), flags, entry.getValue().toArray(new ShardStats[entry.getValue().size()])));
        }
        indexShards = indexShardList;
    }

    public String getIndex() {
        return this.index;
    }

    public ShardStats[] getShards() {
        return this.shards;
    }


    public Map<Integer, IndexShardStats> getIndexShards() {
        return indexShards;
    }

    @Override
    public Iterator<IndexShardStats> iterator() {
        return getIndexShards().values().iterator();
    }


    public CommonStats getTotal() {
        return total;
    }

    public CommonStats getPrimaries() {
        return primary;
    }
}
