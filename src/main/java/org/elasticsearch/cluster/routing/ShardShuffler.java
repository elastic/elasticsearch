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
