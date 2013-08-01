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

import java.util.List;

/**
 * A simple {@link ShardsIterator} that iterates a list or sub-list of
 * {@link ShardRouting shard routings}.
 */
public class PlainShardsIterator implements ShardsIterator {

    private final List<ShardRouting> shards;

    private final int size;

    private final int index;

    private final int limit;

    private volatile int counter;

    public PlainShardsIterator(List<ShardRouting> shards) {
        this(shards, 0);
    }

    public PlainShardsIterator(List<ShardRouting> shards, int index) {
        this.shards = shards;
        this.size = shards.size();
        if (size == 0) {
            this.index = 0;
        } else {
            this.index = Math.abs(index % size);
        }
        this.counter = this.index;
        this.limit = this.index + size;
    }

    @Override
    public void reset() {
        this.counter = this.index;
    }

    @Override
    public int remaining() {
        return limit - counter;
    }

    @Override
    public ShardRouting firstOrNull() {
        if (size == 0) {
            return null;
        }
        return shards.get(index);
    }

    @Override
    public ShardRouting nextOrNull() {
        if (size == 0) {
            return null;
        }
        int counter = (this.counter);
        if (counter >= size) {
            if (counter >= limit) {
                return null;
            }
            this.counter = counter + 1;
            return shards.get(counter - size);
        } else {
            this.counter = counter + 1;
            return shards.get(counter);
        }
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public int sizeActive() {
        int count = 0;
        for (int i = 0; i < size; i++) {
            if (shards.get(i).active()) {
                count++;
            }
        }
        return count;
    }

    @Override
    public int assignedReplicasIncludingRelocating() {
        int count = 0;
        for (int i = 0; i < size; i++) {
            ShardRouting shard = shards.get(i);
            if (shard.unassigned()) {
                continue;
            }
            // if the shard is primary and relocating, add one to the counter since we perform it on the replica as well
            // (and we already did it on the primary)
            if (shard.primary()) {
                if (shard.relocating()) {
                    count++;
                }
            } else {
                count++;
                // if we are relocating the replica, we want to perform the index operation on both the relocating
                // shard and the target shard. This means that we won't loose index operations between end of recovery
                // and reassignment of the shard by the master node
                if (shard.relocating()) {
                    count++;
                }
            }
        }
        return count;
    }

    @Override
    public Iterable<ShardRouting> asUnordered() {
        return shards;
    }
}
