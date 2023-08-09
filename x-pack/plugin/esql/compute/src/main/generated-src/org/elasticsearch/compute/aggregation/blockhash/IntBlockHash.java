/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntArrayVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.MultivalueDedupe;
import org.elasticsearch.compute.operator.MultivalueDedupeInt;

import java.util.BitSet;

/**
 * Maps {@link IntBlock} to group ids.
 * This class is generated. Edit {@code X-BlockHash.java.st} instead.
 */
final class IntBlockHash extends BlockHash {
    private final int channel;
    private final Ordinator64 ordinator;

    /**
     * Have we seen any {@code null} values?
     * <p>
     *     We reserve the 0 ordinal for the {@code null} key so methods like
     *     {@link #nonEmpty} need to skip 0 if we haven't seen any null values.
     * </p>
     */
    private boolean seenNull;

    IntBlockHash(PageCacheRecycler recycler, CircuitBreaker breaker, int channel) {
        this.channel = channel;
        // TODO build and use Ordinator32
        Ordinator64.IdSpace idSpace = new Ordinator64.IdSpace();
        idSpace.next();  // Reserve 0 for nulls.
        this.ordinator = new Ordinator64(recycler, breaker, idSpace);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        IntBlock block = page.getBlock(channel);
        IntVector vector = block.asVector();
        if (vector == null) {
            addInput.add(0, add(block));
        } else {
            addInput.add(0, add(vector));
        }
    }

    private LongVector add(IntVector vector) {
        long[] groups = new long[vector.getPositionCount()];
        // TODO use the array flavored add
        for (int i = 0; i < vector.getPositionCount(); i++) {
            groups[i] = ordinator.add(vector.getInt(i));
        }
        return new LongArrayVector(groups, groups.length);
    }

    private LongBlock add(IntBlock block) {
        MultivalueDedupe.HashResult result = new MultivalueDedupeInt(block).hash(ordinator);
        seenNull |= result.sawNull();
        return result.ords();
    }

    @Override
    public IntBlock[] getKeys() {
        // TODO call something like takeKeyOwnership to claim the keys array directly

        // If we've seen null we'll store it in 0
        if (seenNull) {
            int[] keys = new int[ordinator.currentSize() + 1];
            for (Ordinator64.Itr itr = ordinator.iterator(); itr.next();) {
                // TODO build and use Ordinator32 and drop the cast
                keys[itr.id()] = Math.toIntExact(itr.key());
            }
            BitSet nulls = new BitSet(1);
            nulls.set(0);
            return new IntBlock[] { new IntArrayBlock(keys, keys.length, null, nulls, Block.MvOrdering.ASCENDING) };
        }
        int[] keys = new int[ordinator.currentSize() + (seenNull ? 1 : 0)];
        for (Ordinator64.Itr itr = ordinator.iterator(); itr.next();) {
            // We reserved the id 0 for null but didn't see it.
            // TODO build and use Ordinator32 and drop the cast
            keys[itr.id() - 1] = Math.toIntExact(itr.key());
        }

        return new IntBlock[] { new IntArrayVector(keys, keys.length).asBlock() };
    }

    @Override
    public IntVector nonEmpty() {
        return IntVector.range(seenNull ? 0 : 1, Math.toIntExact(ordinator.currentSize() + 1));
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return new SeenGroupIds.Range(seenNull ? 0 : 1, Math.toIntExact(ordinator.currentSize() + 1)).seenGroupIds(bigArrays);
    }

    @Override
    public void close() {
        ordinator.close();
    }

    @Override
    public String toString() {
        return "IntBlockHash{channel=" + channel + ", entries=" + ordinator.currentSize() + ", seenNull=" + seenNull + '}';
    }

    // TODO plumb ordinator.status
}
