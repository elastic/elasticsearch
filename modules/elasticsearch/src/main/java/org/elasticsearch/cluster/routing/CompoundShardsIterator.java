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
 * @author kimchy (Shay Banon)
 */
public class CompoundShardsIterator implements ShardsIterator, Iterator<ShardRouting> {

    private int index = 0;

    private final List<ShardsIterator> iterators;

    private Iterator<ShardRouting> current;

    public CompoundShardsIterator(List<ShardsIterator> iterators) {
        this.iterators = iterators;
    }

    @Override public ShardsIterator reset() {
        for (ShardsIterator it : iterators) {
            it.reset();
        }
        index = 0;
        current = null;
        return this;
    }

    @Override public int size() {
        int size = 0;
        for (ShardsIterator it : iterators) {
            size += it.size();
        }
        return size;
    }

    @Override public boolean hasNext() {
        if (index == iterators.size()) {
            return false;
        }
        if (current == null) {
            current = iterators.get(index).iterator();
        }
        while (!current.hasNext()) {
            if (++index == iterators.size()) {
                return false;
            }
            current = iterators.get(index).iterator();
        }
        return true;
    }

    @Override public ShardRouting next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return current.next();
    }

    @Override public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override public ShardId shardId() {
        return currentShardsIterator().shardId();
    }

    @Override public Iterator<ShardRouting> iterator() {
        return this;
    }

    private ShardsIterator currentShardsIterator() throws NoSuchElementException {
        if (iterators.size() == 0) {
            throw new NoSuchElementException();
        }
        if (index == iterators.size()) {
            return iterators.get(index - 1);
        }
        return iterators.get(index);

    }
}
