/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.cluster.routing;

import org.elasticsearch.index.shard.ShardId;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Allows to iterate over a set of shard instances (routing) within a shard id group.
 *
 * @author kimchy (shay.banon)
 */
public interface ShardsIterator extends Iterable<ShardRouting>, Iterator<ShardRouting> {

    /**
     * The shard id this group relates to.
     */
    ShardId shardId();

    /**
     * Resets the iterator.
     */
    ShardsIterator reset();

    /**
     * The number of shard routing instances.
     */
    int size();

    /**
     * The number of active shard routing instances.
     *
     * @see ShardRouting#active()
     */
    int sizeActive();

    /**
     * Is there an active shard we can iterate to.
     *
     * @see ShardRouting#active()
     */
    boolean hasNextActive();

    /**
     * Returns the next active shard, or throws {@link NoSuchElementException}.
     *
     * @see ShardRouting#active()
     */
    ShardRouting nextActive() throws NoSuchElementException;

    /**
     * Returns the next active shard, or <tt>null</tt>.
     *
     * @see ShardRouting#active()
     */
    ShardRouting nextActiveOrNull();

    /**
     * The number of assigned shard routing instances.
     *
     * @see ShardRouting#assignedToNode()
     */
    int sizeAssigned();

    /**
     * Is there an assigned shard we can iterate to.
     *
     * @see ShardRouting#assignedToNode()
     */
    boolean hasNextAssigned();

    /**
     * Returns the next assigned shard, or throws {@link NoSuchElementException}.
     *
     * @see ShardRouting#assignedToNode()
     */
    ShardRouting nextAssigned() throws NoSuchElementException;

    /**
     * Returns the next assigned shard, or <tt>null</tt>.
     *
     * @see ShardRouting#assignedToNode()
     */
    ShardRouting nextAssignedOrNull();

    int hashCode();

    boolean equals(Object other);
}
