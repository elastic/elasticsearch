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
$if(double)$
import org.elasticsearch.compute.data.DoubleArrayBlock;
import org.elasticsearch.compute.data.DoubleArrayVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
$endif$
$if(int)$
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntArrayVector;
import org.elasticsearch.compute.data.IntBlock;
$endif$
import org.elasticsearch.compute.data.IntVector;
$if(long)$
import org.elasticsearch.compute.data.LongArrayBlock;
$endif$
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.MultivalueDedupe;
import org.elasticsearch.compute.operator.MultivalueDedupe$Type$;

import java.util.BitSet;

/**
 * Maps {@link $Type$Block} to group ids.
 * This class is generated. Edit {@code X-BlockHash.java.st} instead.
 */
final class $Type$BlockHash extends BlockHash {
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

    $Type$BlockHash(PageCacheRecycler recycler, CircuitBreaker breaker, int channel) {
        this.channel = channel;
$if(int)$
        // TODO build and use Ordinator32
$endif$
        Ordinator64.IdSpace idSpace = new Ordinator64.IdSpace();
        idSpace.next();  // Reserve 0 for nulls.
        this.ordinator = new Ordinator64(recycler, breaker, idSpace);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        $Type$Block block = page.getBlock(channel);
        $Type$Vector vector = block.asVector();
        if (vector == null) {
            addInput.add(0, add(block));
        } else {
            addInput.add(0, add(vector));
        }
    }

    private LongVector add($Type$Vector vector) {
        long[] groups = new long[vector.getPositionCount()];
        // TODO use the array flavored add
        for (int i = 0; i < vector.getPositionCount(); i++) {
$if(double)$
            groups[i] = ordinator.add(Double.doubleToLongBits(vector.get$Type$(i)));
$else$
            groups[i] = ordinator.add(vector.get$Type$(i));
$endif$
        }
        return new LongArrayVector(groups, groups.length);
    }

    private LongBlock add($Type$Block block) {
        MultivalueDedupe.HashResult result = new MultivalueDedupe$Type$(block).hash(ordinator);
        seenNull |= result.sawNull();
        return result.ords();
    }

    @Override
    public $Type$Block[] getKeys() {
        // TODO call something like takeKeyOwnership to claim the keys array directly

        // If we've seen null we'll store it in 0
        if (seenNull) {
            $type$[] keys = new $type$[ordinator.currentSize() + 1];
            for (Ordinator64.Itr itr = ordinator.iterator(); itr.next();) {
$if(int)$
                // TODO build and use Ordinator32 and drop the cast
                keys[itr.id()] = Math.toIntExact(itr.key());
$elseif(double)$
                keys[itr.id()] = Double.longBitsToDouble(itr.key());
$else$
                keys[itr.id()] = itr.key();
$endif$
            }
            BitSet nulls = new BitSet(1);
            nulls.set(0);
            return new $Type$Block[] { new $Type$ArrayBlock(keys, keys.length, null, nulls, Block.MvOrdering.ASCENDING) };
        }
        $type$[] keys = new $type$[ordinator.currentSize() + (seenNull ? 1 : 0)];
        for (Ordinator64.Itr itr = ordinator.iterator(); itr.next();) {
            // We reserved the id 0 for null but didn't see it.
$if(int)$
            // TODO build and use Ordinator32 and drop the cast
            keys[itr.id() - 1] = Math.toIntExact(itr.key());
$elseif(double)$
            keys[itr.id() - 1] = Double.longBitsToDouble(itr.key());
$else$
            keys[itr.id() - 1] = itr.key();
$endif$
        }

        return new $Type$Block[] { new $Type$ArrayVector(keys, keys.length).asBlock() };
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
        return "$Type$BlockHash{channel=" + channel + ", entries=" + ordinator.currentSize() + ", seenNull=" + seenNull + '}';
    }

    // TODO plumb ordinator.status
}
