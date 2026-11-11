/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

/**
 * Sums {@code long} input exactly in a signed 128-bit accumulator and reports the result in the intermediate state of
 * {@link SumDoubleAggregatorFunction}.
 * <p>
 *     This is the replacement for {@code SUM(TO_DOUBLE(long_field))}, which is what {@code AVG(long_field)} is rewritten to.
 *     That plan converts every {@code long} into a {@code double} in a separate operator and then runs a Kahan summation, whose
 *     loop-carried floating point dependency dominates the time of the whole aggregation. Summing the {@code long}s directly
 *     avoids the conversion, the extra block, and the dependency chain.
 * </p>
 * <p>
 *     The intermediate state is deliberately identical to {@link SumDoubleAggregatorFunction}'s: nodes that don't know about
 *     this function keep working, because they exchange the same {@code (value, delta, seen)} columns and only the way the
 *     {@code value} is produced differs. The exact 128-bit sum is rounded to a {@code double} once, and the part that doesn't
 *     fit into the {@code double} is reported in {@code delta}, which {@link CompensatedSum} adds back when partial results
 *     are merged.
 * </p>
 * <p>
 *     Only the non-grouping form exists. Intermediate input is merged with a {@link CompensatedSum}, so this function can
 *     be used for any {@link AggregatorMode}.
 * </p>
 */
public final class SumLongAsDoubleAggregatorFunction implements AggregatorFunction {
    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = SumDoubleAggregatorFunction.intermediateStateDesc();

    private static final long LOW_MASK = 0xFFFF_FFFFL;

    public static List<IntermediateStateDesc> intermediateStateDesc() {
        return INTERMEDIATE_STATE_DESC;
    }

    private final List<Integer> channels;

    /** The exact sum of all raw input as {@code hi * 2^64 + unsigned(lo)}. */
    private long hi;
    private long lo;
    private boolean seen;

    /** Sum of the partial results received as intermediate input. {@code null} until there is any. */
    private CompensatedSum merged;

    SumLongAsDoubleAggregatorFunction(List<Integer> channels) {
        this.channels = channels;
    }

    @Override
    public int intermediateBlockCount() {
        return INTERMEDIATE_STATE_DESC.size();
    }

    @Override
    public void addRawInput(Page page, BooleanVector mask) {
        if (mask.allFalse()) {
            return;
        }
        LongBlock block = page.getBlock(channels.get(0));
        if (block.areAllValuesNull()) {
            return;
        }
        BooleanVector effectiveMask = mask.allTrue() ? null : mask;
        LongVector vector = block.asVector();
        if (vector == null) {
            addBlock(block, effectiveMask);
        } else if (effectiveMask == null) {
            addVector(vector);
        } else {
            addVector(vector, effectiveMask);
        }
    }

    private void addVector(LongVector vector) {
        int positions = vector.getPositionCount();
        if (positions == 0) {
            return;
        }
        seen = true;
        if (vector.getClass() == LongArrayVector.class) {
            addArrayVector((LongArrayVector) vector);
            return;
        }
        for (int p = 0; p < positions; p++) {
            add(vector.getLong(p));
        }
    }

    /**
     * The hot loop. The low and the high 32 bits of every value are summed in separate lanes, so there are no carries to
     * chase inside the loop, there is no loop-carried dependency between the lanes, and the loop is easy for the JIT to
     * vectorize. A lane can't overflow: it adds at most {@code 2^29} values of at most {@code 2^32} each.
     */
    private void addArrayVector(LongArrayVector vector) {
        int positions = vector.getPositionCount();
        long low0 = 0, low1 = 0, low2 = 0, low3 = 0;
        long high0 = 0, high1 = 0, high2 = 0, high3 = 0;
        int p = 0;
        for (; p + 3 < positions; p += 4) {
            long x0 = vector.getLong(p);
            long x1 = vector.getLong(p + 1);
            long x2 = vector.getLong(p + 2);
            long x3 = vector.getLong(p + 3);

            low0 += x0 & LOW_MASK;
            high0 += x0 >> 32;
            low1 += x1 & LOW_MASK;
            high1 += x1 >> 32;
            low2 += x2 & LOW_MASK;
            high2 += x2 >> 32;
            low3 += x3 & LOW_MASK;
            high3 += x3 >> 32;
        }
        addLanes(low0 + low1 + low2 + low3, high0 + high1 + high2 + high3);
        for (; p < positions; p++) {
            add(vector.getLong(p));
        }
    }

    private void addVector(LongVector vector, BooleanVector mask) {
        for (int p = 0; p < vector.getPositionCount(); p++) {
            if (mask.getBoolean(p)) {
                seen = true;
                add(vector.getLong(p));
            }
        }
    }

    /** Handles blocks with nulls or multivalued positions. Every value of a multivalued position is added. */
    private void addBlock(LongBlock block, BooleanVector mask) {
        for (int p = 0; p < block.getPositionCount(); p++) {
            if (mask != null && mask.getBoolean(p) == false) {
                continue;
            }
            int count = block.getValueCount(p);
            if (count == 0) {
                continue;
            }
            seen = true;
            int start = block.getFirstValueIndex(p);
            for (int i = start; i < start + count; i++) {
                add(block.getLong(i));
            }
        }
    }

    /** Adds {@code high * 2^32 + low} where {@code low} is a non-negative sum of 32 bit values and {@code high} is signed. */
    private void addLanes(long low, long high) {
        long mid = high + (low >>> 32);
        add128(mid >> 32, (mid << 32) | (low & LOW_MASK));
    }

    private void add(long x) {
        add128(x >> 63, x);
    }

    private void add128(long addHi, long addLo) {
        long nextLo = lo + addLo;
        long carry = Long.compareUnsigned(nextLo, lo) < 0 ? 1L : 0L;
        hi += addHi + carry;
        lo = nextLo;
    }

    @Override
    public void addIntermediateInput(Page page) {
        assert channels.size() == intermediateBlockCount();
        assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
        Block valueUncast = page.getBlock(channels.get(0));
        Block deltaUncast = page.getBlock(channels.get(1));
        Block seenUncast = page.getBlock(channels.get(2));
        if (valueUncast.areAllValuesNull() || deltaUncast.areAllValuesNull() || seenUncast.areAllValuesNull()) {
            return;
        }
        DoubleVector value = ((DoubleBlock) valueUncast).asVector();
        DoubleVector delta = ((DoubleBlock) deltaUncast).asVector();
        BooleanVector seenVector = ((BooleanBlock) seenUncast).asVector();
        assert value.getPositionCount() == 1;
        assert delta.getPositionCount() == 1;
        assert seenVector.getPositionCount() == 1;
        if (seenVector.getBoolean(0) == false) {
            return;
        }
        if (merged == null) {
            merged = new CompensatedSum();
        }
        merged.add(value.getDouble(0), delta.getDouble(0));
        seen = true;
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
        assert blocks.length >= offset + 3;
        BlockFactory blockFactory = driverContext.blockFactory();
        CompensatedSum total = total();
        blocks[offset + 0] = blockFactory.newConstantDoubleBlockWith(total.value(), 1);
        blocks[offset + 1] = blockFactory.newConstantDoubleBlockWith(total.delta(), 1);
        blocks[offset + 2] = blockFactory.newConstantBooleanBlockWith(seen, 1);
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
        if (seen == false) {
            blocks[offset] = driverContext.blockFactory().newConstantNullBlock(1);
            return;
        }
        blocks[offset] = driverContext.blockFactory().newConstantDoubleBlockWith(total().value(), 1);
    }

    /**
     * Combines the exact 128 bit sum with whatever arrived as intermediate input. The exact sum is rounded to the nearest
     * {@code double} and the rounding error becomes the correction term, so that {@code value + delta} is the exact sum.
     */
    private CompensatedSum total() {
        BigInteger exact = exactSum();
        double value = exact.doubleValue();
        double delta = exact.subtract(new BigDecimal(value).toBigInteger()).doubleValue();
        CompensatedSum total = new CompensatedSum(value, delta);
        if (merged != null) {
            total.add(merged.value(), merged.delta());
        }
        return total;
    }

    /** The exact sum of the raw input as an integer; visible for testing. */
    BigInteger exactSum() {
        // hi * 2^64 + unsigned(lo)
        BigInteger unsignedLo = BigInteger.valueOf(lo & Long.MAX_VALUE).add(lo < 0 ? BigInteger.ONE.shiftLeft(63) : BigInteger.ZERO);
        return BigInteger.valueOf(hi).shiftLeft(64).add(unsignedLo);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append("[");
        sb.append("channels=").append(channels);
        sb.append("]");
        return sb.toString();
    }

    @Override
    public void close() {}
}
