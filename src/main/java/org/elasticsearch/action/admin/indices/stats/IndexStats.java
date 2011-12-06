/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class IndexStats implements Iterable<IndexShardStats> {

    private final String index;

    private final ShardStats shards[];

    public IndexStats(String index, ShardStats[] shards) {
        this.index = index;
        this.shards = shards;
    }

    public String index() {
        return this.index;
    }

    public ShardStats[] shards() {
        return this.shards;
    }

    private Map<Integer, IndexShardStats> indexShards;

    public Map<Integer, IndexShardStats> indexShards() {
        if (indexShards != null) {
            return indexShards;
        }
        Map<Integer, List<ShardStats>> tmpIndexShards = Maps.newHashMap();
        for (ShardStats shard : shards) {
            List<ShardStats> lst = tmpIndexShards.get(shard.shardRouting().id());
            if (lst == null) {
                lst = Lists.newArrayList();
                tmpIndexShards.put(shard.shardRouting().id(), lst);
            }
            lst.add(shard);
        }
        indexShards = Maps.newHashMap();
        for (Map.Entry<Integer, List<ShardStats>> entry : tmpIndexShards.entrySet()) {
            indexShards.put(entry.getKey(), new IndexShardStats(entry.getValue().get(0).shardRouting().shardId(), entry.getValue().toArray(new ShardStats[entry.getValue().size()])));
        }
        return indexShards;
    }

    @Override
    public Iterator<IndexShardStats> iterator() {
        return indexShards().values().iterator();
    }

    private CommonStats total = null;

    public CommonStats getTotal() {
        return total();
    }

    public CommonStats total() {
        if (total != null) {
            return total;
        }
        CommonStats stats = new CommonStats();
        for (ShardStats shard : shards) {
            stats.add(shard.stats());
        }
        total = stats;
        return stats;
    }

    private CommonStats primary = null;

    public CommonStats getPrimaries() {
        return primaries();
    }

    public CommonStats primaries() {
        if (primary != null) {
            return primary;
        }
        CommonStats stats = new CommonStats();
        for (ShardStats shard : shards) {
            if (shard.shardRouting().primary()) {
                stats.add(shard.stats());
            }
        }
        primary = stats;
        return stats;
    }
}
