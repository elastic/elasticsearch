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

/**
 * @author kimchy (Shay Banon)
 */
public class PlainShardsIterator implements ShardsIterator {

    private final ShardId shardId;

    private final List<ShardRouting> shards;

    private Iterator<ShardRouting> iterator;

    public PlainShardsIterator(ShardId shardId, List<ShardRouting> shards) {
        this.shardId = shardId;
        this.shards = shards;
        this.iterator = shards.iterator();
    }

    @Override public ShardsIterator reset() {
        this.iterator = shards.iterator();
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
        return iterator.hasNext();
    }

    @Override public ShardRouting next() {
        return iterator.next();
    }

    @Override public void remove() {
        throw new UnsupportedOperationException();
    }
}
