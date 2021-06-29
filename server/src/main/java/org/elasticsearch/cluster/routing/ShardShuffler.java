/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import java.util.List;

/**
 * A shuffler for shards whose primary goal is to balance load.
 */
public abstract class ShardShuffler {

    /**
     * Return a new seed.
     */
    public abstract int nextSeed();

    /**
     * Return a shuffled view over the list of shards. The behavior of this method must be deterministic: if the same list and the same seed
     * are provided twice, then the result needs to be the same.
     */
    public abstract List<ShardRouting> shuffle(List<ShardRouting> shards, int seed);

    /**
     * Equivalent to calling <code>shuffle(shards, nextSeed())</code>.
     */
    public List<ShardRouting> shuffle(List<ShardRouting> shards) {
        return shuffle(shards, nextSeed());
    }

}
