/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.$Type$Array;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBigArrayVector;
import org.elasticsearch.compute.data.BooleanBlock;
$if(double)$
import org.elasticsearch.compute.data.$Type$BigArrayVector;
import org.elasticsearch.compute.data.$Type$Block;
import org.elasticsearch.compute.data.$Type$Vector;
import org.elasticsearch.compute.data.IntRangeVector;
import org.elasticsearch.compute.data.IntVector;
$elseif(int)$
import org.elasticsearch.compute.data.$Type$BigArrayVector;
import org.elasticsearch.compute.data.$Type$Block;
import org.elasticsearch.compute.data.IntRangeVector;
import org.elasticsearch.compute.data.$Type$Vector;
$else$
import org.elasticsearch.compute.data.IntRangeVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.$Type$BigArrayVector;
import org.elasticsearch.compute.data.$Type$Block;
import org.elasticsearch.compute.data.$Type$Vector;
$endif$
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

/**
 * Aggregator state for an array of $type$s.
 * This class is generated. Do not edit it.
 */
final class $Type$ArrayState implements GroupingAggregatorState {
    private final BigArrays bigArrays;
    private final $type$ init;
    private final DriverContext driverContext;

    private $Type$Array values;
    /**
     * Total number of groups {@code <=} values.length.
     */
    private int largestIndex;
    private BitArray nonNulls;

    $Type$ArrayState(BigArrays bigArrays, $type$ init, DriverContext driverContext) {
        this.bigArrays = bigArrays;
        this.values = bigArrays.new$Type$Array(1, false);
        this.values.set(0, init);
        this.init = init;
        this.driverContext = driverContext;
    }

    $type$ get(int index) {
        return values.get(index);
    }

    $type$ getOrDefault(int index) {
        return index <= largestIndex ? values.get(index) : init;
    }

    void set($type$ value, int index) {
        if (index > largestIndex) {
            ensureCapacity(index);
            largestIndex = index;
        }
        values.set(index, value);
        if (nonNulls != null) {
            nonNulls.set(index);
        }
    }

$if(long)$
    void increment(long value, int index) {
        if (index > largestIndex) {
            ensureCapacity(index);
            largestIndex = index;
        }
        values.increment(index, value);
        if (nonNulls != null) {
            nonNulls.set(index);
        }
    }
$endif$

    void putNull(int index) {
        if (index > largestIndex) {
            ensureCapacity(index);
            largestIndex = index;
        }
        if (nonNulls == null) {
            nonNulls = new BitArray(index + 1, bigArrays);
            for (int i = 0; i < index; i++) {
                nonNulls.set(i);
            }
        } else {
            // Do nothing. Null is represented by the default value of false for get(int),
            // and any present value trumps a null value in our aggregations.
        }
    }

    boolean hasValue(int index) {
        return nonNulls == null || nonNulls.get(index);
    }

    Block toValuesBlock(org.elasticsearch.compute.data.IntVector selected) {
        if (nonNulls == null) {
            $Type$Vector.Builder builder = $Type$Vector.newVectorBuilder(selected.getPositionCount());
            for (int i = 0; i < selected.getPositionCount(); i++) {
                builder.append$Type$(values.get(selected.getInt(i)));
            }
            return builder.build().asBlock();
        }
        $Type$Block.Builder builder = $Type$Block.newBlockBuilder(selected.getPositionCount());
        for (int i = 0; i < selected.getPositionCount(); i++) {
            int group = selected.getInt(i);
            if (hasValue(group)) {
                builder.append$Type$(values.get(group));
            } else {
                builder.appendNull();
            }
        }
        return builder.build();
    }

    private void ensureCapacity(int position) {
        if (position >= values.size()) {
            long prevSize = values.size();
            values = bigArrays.grow(values, position + 1);
            values.fill(prevSize, values.size(), init);
        }
    }

    /** Extracts an intermediate view of the contents of this state.  */
    @Override
    public void toIntermediate(Block[] blocks, int offset, IntVector selected) {
        assert blocks.length >= offset + 2;
        blocks[offset + 0] = intermediateValues(selected);
        blocks[offset + 1] = intermediateNonNulls(selected);
    }

    Block intermediateValues(IntVector selected) {
        if (IntRangeVector.isRangeFromMToN(selected, 0, selected.getPositionCount())) {
            $Type$BigArrayVector vector = new $Type$BigArrayVector(values, selected.getPositionCount());
            values = null; // do not release
            driverContext.addReleasable(vector);
            return vector.asBlock();
        } else {
            var valuesBuilder = $Type$Block.newBlockBuilder(selected.getPositionCount());
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                valuesBuilder.append$Type$(values.get(group));
            }
            return valuesBuilder.build();
        }
    }

    Block intermediateNonNulls(IntVector selected) {
        if (nonNulls == null) {
            return BooleanBlock.newConstantBlockWith(true, selected.getPositionCount());
        }
        if (IntRangeVector.isRangeFromMToN(selected, 0, selected.getPositionCount())) {
            BooleanBigArrayVector vector = new BooleanBigArrayVector(nonNulls, selected.getPositionCount());
            nonNulls = null; // do not release
            driverContext.addReleasable(vector);
            return vector.asBlock();
        }
        var nullsBuilder = BooleanBlock.newBlockBuilder(selected.getPositionCount());
        for (int i = 0; i < selected.getPositionCount(); i++) {
            int group = selected.getInt(i);
            nullsBuilder.appendBoolean(hasValue(group));
        }
        return nullsBuilder.build();
    }

    @Override
    public void close() {
        Releasables.close(values, nonNulls);
    }
}
