/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.segments;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.index.shard.ShardId;

import java.util.Iterator;

public record IndexShardSegments(ShardId shardId, ShardSegments[] shards) implements Iterable<ShardSegments> {

    public ShardSegments getAt(int i) {
        return shards[i];
    }

    @Override
    public Iterator<ShardSegments> iterator() {
        return Iterators.forArray(shards);
    }
}
