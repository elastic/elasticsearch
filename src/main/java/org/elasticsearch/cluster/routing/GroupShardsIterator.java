/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

import java.util.Collection;
import java.util.Iterator;

/**
 *
 */
public class GroupShardsIterator implements Iterable<ShardIterator> {

    private final Collection<ShardIterator> iterators;

    public GroupShardsIterator(Collection<ShardIterator> iterators) {
        this.iterators = iterators;
    }

    public int totalSize() {
        int size = 0;
        for (ShardIterator shard : iterators) {
            size += shard.size();
        }
        return size;
    }

    public int totalSizeWith1ForEmpty() {
        int size = 0;
        for (ShardIterator shard : iterators) {
            int sizeActive = shard.size();
            if (sizeActive == 0) {
                size += 1;
            } else {
                size += sizeActive;
            }
        }
        return size;
    }

    public int size() {
        return iterators.size();
    }

    public Collection<ShardIterator> iterators() {
        return iterators;
    }

    @Override
    public Iterator<ShardIterator> iterator() {
        return iterators.iterator();
    }
}
