/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.common.util.CollectionUtils;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Basic {@link ShardShuffler} implementation that uses an {@link AtomicInteger} to generate seeds and uses a rotation to permute shards.
 */
public class RotationShardShuffler extends ShardShuffler {

    private final AtomicInteger seed;

    public RotationShardShuffler(int seed) {
        this.seed = new AtomicInteger(seed);
    }

    @Override
    public int nextSeed() {
        return seed.getAndIncrement();
    }

    @Override
    public List<ShardRouting> shuffle(List<ShardRouting> shards, int seed) {
        return CollectionUtils.rotate(shards, seed);
    }

}
