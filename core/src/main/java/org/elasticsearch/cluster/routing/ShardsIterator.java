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

/**
 * Allows to iterate over unrelated shards.
 */
public interface ShardsIterator {

    /**
     * Resets the iterator to its initial state.
     */
    void reset();

    /**
     * The number of shard routing instances.
     *
     * @return number of shard routing instances in this iterator
     */
    int size();

    /**
     * The number of active shard routing instances
     *
     * @return number of active shard routing instances
     */
    int sizeActive();

    /**
     * Returns the number of replicas in this iterator that are not in the
     * {@link ShardRoutingState#UNASSIGNED}. The returned double-counts replicas
     * that are in the state {@link ShardRoutingState#RELOCATING}
     */
    int assignedReplicasIncludingRelocating();

    /**
     * Returns the next shard, or <tt>null</tt> if none available.
     */
    ShardRouting nextOrNull();

    /**
     * Return the number of shards remaining in this {@link ShardsIterator}
     *
     * @return number of shard remaining
     */
    int remaining();

    @Override
    int hashCode();

    @Override
    boolean equals(Object other);

    Iterable<ShardRouting> asUnordered();
}

