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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * @author kimchy (shay.banon)
 */
public class PlainShardsIterator implements ShardsIterator {

    protected final List<ShardRouting> shards;

    private final int origIndex;

    private volatile int index;

    private volatile int counter = 0;

    public PlainShardsIterator(List<ShardRouting> shards) {
        this(shards, 0);
    }

    public PlainShardsIterator(List<ShardRouting> shards, int index) {
        this.shards = shards;
        this.index = Math.abs(index);
        this.origIndex = this.index;
    }

    @Override public Iterator<ShardRouting> iterator() {
        return this;
    }

    @Override public ShardsIterator reset() {
        counter = 0;
        index = origIndex;
        return this;
    }

    @Override public boolean hasNext() {
        return counter < size();
    }

    @Override public ShardRouting next() throws NoSuchElementException {
        if (!hasNext()) {
            throw new NoSuchElementException("No shard found");
        }
        counter++;
        return shardModulo(index++);
    }

    @Override public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override public int size() {
        return shards.size();
    }

    @Override public int sizeActive() {
        int shardsActive = 0;
        for (ShardRouting shardRouting : shards) {
            if (shardRouting.active()) {
                shardsActive++;
            }
        }
        return shardsActive;
    }

    @Override public boolean hasNextActive() {
        int counter = this.counter;
        int index = this.index;
        while (counter++ < size()) {
            ShardRouting shardRouting = shardModulo(index++);
            if (shardRouting.active()) {
                return true;
            }
        }
        return false;
    }

    @Override public ShardRouting nextActive() throws NoSuchElementException {
        ShardRouting shardRouting = nextActiveOrNull();
        if (shardRouting == null) {
            throw new NoSuchElementException("No active shard found");
        }
        return shardRouting;
    }

    @Override public ShardRouting nextActiveOrNull() throws NoSuchElementException {
        int counter = this.counter;
        int index = this.index;
        while (counter++ < size()) {
            ShardRouting shardRouting = shardModulo(index++);
            if (shardRouting.active()) {
                this.counter = counter;
                this.index = index;
                return shardRouting;
            }
        }
        this.counter = counter;
        this.index = index;
        return null;
    }

    @Override public int sizeAssigned() {
        int shardsAssigned = 0;
        for (ShardRouting shardRouting : shards) {
            if (shardRouting.assignedToNode()) {
                shardsAssigned++;
            }
        }
        return shardsAssigned;
    }

    @Override public boolean hasNextAssigned() {
        int counter = this.counter;
        int index = this.index;
        while (counter++ < size()) {
            ShardRouting shardRouting = shardModulo(index++);
            if (shardRouting.assignedToNode()) {
                return true;
            }
        }
        return false;
    }

    @Override public ShardRouting nextAssigned() throws NoSuchElementException {
        ShardRouting shardRouting = nextAssignedOrNull();
        if (shardRouting == null) {
            throw new NoSuchElementException("No assigned shard found");
        }
        return shardRouting;
    }

    @Override public ShardRouting nextAssignedOrNull() {
        int counter = this.counter;
        int index = this.index;
        while (counter++ < size()) {
            ShardRouting shardRouting = shardModulo(index++);
            if (shardRouting.assignedToNode()) {
                this.counter = counter;
                this.index = index;
                return shardRouting;
            }
        }
        this.counter = counter;
        this.index = index;
        return null;
    }

    ShardRouting shardModulo(int counter) {
        return shards.get((counter % size()));
    }
}
