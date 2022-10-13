/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.util.Countable;

import java.util.Iterator;
import java.util.List;

/**
 * This class implements a compilation of {@link ShardIterator}s. Each {@link ShardIterator}
 * iterated by this {@link Iterable} represents a group of shards.
 * ShardsIterators are always returned in ascending order independently of their order at construction
 * time. The incoming iterators are sorted to ensure consistent iteration behavior across Nodes / JVMs.
*/
public final class GroupShardsIterator<ShardIt extends Comparable<ShardIt> & Countable> implements Iterable<ShardIt> {

    private final List<ShardIt> iterators;

    /**
     * Constructs a new sorted GroupShardsIterator from the given list. Items are sorted based on their natural ordering.
     * @see PlainShardIterator#compareTo(ShardIterator)
     */
    public static <ShardIt extends Comparable<ShardIt> & Countable> GroupShardsIterator<ShardIt> sortAndCreate(List<ShardIt> iterators) {
        CollectionUtil.timSort(iterators);
        return new GroupShardsIterator<>(iterators);
    }

    /**
     * Constructs a new GroupShardsIterator from the given list.
     */
    public GroupShardsIterator(List<ShardIt> iterators) {
        this.iterators = iterators;
    }

    /**
     * Returns the total number of shards within all groups
     * @return total number of shards
     */
    public int totalSize() {
        return iterators.stream().mapToInt(Countable::size).sum();
    }

    /**
     * Returns the total number of shards plus the number of empty groups
     * @return number of shards and empty groups
     */
    public int totalSizeWith1ForEmpty() {
        int size = 0;
        for (ShardIt shard : iterators) {
            size += Math.max(1, shard.size());
        }
        return size;
    }

    /**
     * Return the number of groups
     * @return number of groups
     */
    public int size() {
        return iterators.size();
    }

    @Override
    public Iterator<ShardIt> iterator() {
        return iterators.iterator();
    }

    public ShardIt get(int index) {
        return iterators.get(index);
    }
}
