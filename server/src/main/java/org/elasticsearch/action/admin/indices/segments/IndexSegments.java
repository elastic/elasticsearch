/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.segments;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class IndexSegments implements Iterable<IndexShardSegments> {

    private final String index;

    private final Map<Integer, IndexShardSegments> indexShards;

    IndexSegments(String index, List<ShardSegments> shards) {
        this.index = index;

        final Map<Integer, List<ShardSegments>> segmentsByShardId = new HashMap<>();
        for (ShardSegments shard : shards) {
            segmentsByShardId.computeIfAbsent(shard.getShardRouting().id(), k -> new ArrayList<>()).add(shard);
        }
        indexShards = new HashMap<>();
        for (Map.Entry<Integer, List<ShardSegments>> entry : segmentsByShardId.entrySet()) {
            indexShards.put(
                entry.getKey(),
                new IndexShardSegments(
                    entry.getValue().get(0).getShardRouting().shardId(),
                    entry.getValue().toArray(new ShardSegments[entry.getValue().size()])
                )
            );
        }
    }

    public String getIndex() {
        return this.index;
    }

    /**
     * A shard id to index shard segments map (note, index shard segments is the replication shard group that maps
     * to the shard id).
     */
    public Map<Integer, IndexShardSegments> getShards() {
        return this.indexShards;
    }

    @Override
    public Iterator<IndexShardSegments> iterator() {
        return indexShards.values().iterator();
    }
}
