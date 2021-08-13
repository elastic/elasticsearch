/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.routing;

import org.elasticsearch.common.util.PlainIterator;

import java.util.List;

/**
 * A simple {@link ShardsIterator} that iterates a list or sub-list of
 * {@link ShardRouting shard indexRoutings}.
 */
public class PlainShardsIterator extends PlainIterator<ShardRouting> implements ShardsIterator {
    public PlainShardsIterator(List<ShardRouting> shards) {
        super(shards);
    }

    @Override
    public int sizeActive() {
        return Math.toIntExact(getShardRoutings().stream().filter(ShardRouting::active).count());
    }

    @Override
    public List<ShardRouting> getShardRoutings() {
        return asList();
    }
}
