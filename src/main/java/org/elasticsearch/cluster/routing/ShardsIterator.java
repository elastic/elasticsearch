/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
 *
 *
 */
public interface ShardsIterator {

    /**
     * Resets the iterator.
     */
    ShardsIterator reset();

    /**
     * The number of shard routing instances.
     */
    int size();

    int sizeActive();

    int assignedReplicasIncludingRelocating();

    /**
     * Returns the next shard, or <tt>null</tt> if none available.
     */
    ShardRouting nextOrNull();

    /**
     * Returns the first shard, or <tt>null</tt>, without
     * incrementing the iterator.
     *
     * @see ShardRouting#assignedToNode()
     */
    ShardRouting firstOrNull();

    int remaining();

    int hashCode();

    boolean equals(Object other);

    Iterable<ShardRouting> asUnordered();
}

