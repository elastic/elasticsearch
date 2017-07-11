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

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * Represents snapshot status of all shards in the index
 */
public class SnapshotIndexStatus implements Iterable<SnapshotIndexShardStatus>, ToXContent {

    private final String index;

    private final Map<Integer, SnapshotIndexShardStatus> indexShards;

    private final SnapshotShardsStats shardsStats;

    private final SnapshotStats stats;

    SnapshotIndexStatus(String index, Collection<SnapshotIndexShardStatus> shards) {
        this.index = index;

        Map<Integer, SnapshotIndexShardStatus> indexShards = new HashMap<>();
        stats = new SnapshotStats();
        for (SnapshotIndexShardStatus shard : shards) {
            indexShards.put(shard.getShardId().getId(), shard);
            stats.add(shard.getStats());
        }
        shardsStats = new SnapshotShardsStats(shards);
        this.indexShards = unmodifiableMap(indexShards);
    }

    /**
     * Returns the index name
     */
    public String getIndex() {
        return this.index;
    }

    /**
     * A shard id to index snapshot shard status map
     */
    public Map<Integer, SnapshotIndexShardStatus> getShards() {
        return this.indexShards;
    }

    /**
     * Shards stats
     */
    public SnapshotShardsStats getShardsStats() {
        return shardsStats;
    }

    /**
     * Returns snapshot stats
     */
    public SnapshotStats getStats() {
        return stats;
    }

    @Override
    public Iterator<SnapshotIndexShardStatus> iterator() {
        return indexShards.values().iterator();
    }

    static final class Fields {
        static final String SHARDS = "shards";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getIndex());
        shardsStats.toXContent(builder, params);
        stats.toXContent(builder, params);
        builder.startObject(Fields.SHARDS);
        for (SnapshotIndexShardStatus shard : indexShards.values()) {
            shard.toXContent(builder, params);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }
}
