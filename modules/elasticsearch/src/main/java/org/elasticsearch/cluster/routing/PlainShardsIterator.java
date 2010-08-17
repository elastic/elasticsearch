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
import java.util.List;
import java.util.NoSuchElementException;

/**
 * @author kimchy (shay.banon)
 */
public class PlainShardsIterator implements ShardsIterator {

    private final ShardId shardId;

    private final List<ShardRouting> shards;

    private volatile int counter = 0;

    public PlainShardsIterator(ShardId shardId, List<ShardRouting> shards) {
        this.shardId = shardId;
        this.shards = shards;
    }

    @Override public ShardsIterator reset() {
        this.counter = 0;
        return this;
    }

    @Override public int size() {
        return shards.size();
    }

    @Override public ShardId shardId() {
        return this.shardId;
    }

    @Override public Iterator<ShardRouting> iterator() {
        return this;
    }

    @Override public boolean hasNext() {
        return counter < shards.size();
    }

    @Override public ShardRouting next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No shard found");
        }
        return shards.get(counter++);
    }

    @Override public int sizeActive() {
        int sizeActive = 0;
        for (ShardRouting shardRouting : shards) {
            if (shardRouting.active()) {
                sizeActive++;
            }
        }
        return sizeActive;
    }

    @Override public boolean hasNextActive() {
        int counter = this.counter;
        while (counter < shards.size()) {
            if (shards.get(counter++).active()) {
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
        while (counter < shards.size()) {
            ShardRouting shardRouting = shards.get(counter++);
            if (shardRouting.active()) {
                return shardRouting;
            }
        }
        return null;
    }

    @Override public int sizeAssigned() {
        int sizeAssigned = 0;
        for (ShardRouting shardRouting : shards) {
            if (shardRouting.assignedToNode()) {
                sizeAssigned++;
            }
        }
        return sizeAssigned;
    }

    @Override public boolean hasNextAssigned() {
        int counter = this.counter;
        while (counter < shards.size()) {
            if (shards.get(counter++).assignedToNode()) {
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
        while (counter < shards.size()) {
            ShardRouting shardRouting = shards.get(counter++);
            if (shardRouting.assignedToNode()) {
                return shardRouting;
            }
        }
        return null;
    }

    @Override public void remove() {
        throw new UnsupportedOperationException();
    }
}
