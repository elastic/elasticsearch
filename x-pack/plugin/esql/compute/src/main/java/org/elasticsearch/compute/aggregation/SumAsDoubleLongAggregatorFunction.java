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
import org.elasticsearch.compute.data.ElementType;
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
 * The sum of {@code long}s as a {@code double}, which can't overflow. It is what {@code SUM(TO_DOUBLE(x))} computes for a
 * {@code long} {@code x}, without converting every value in an extra pass and without the Kahan summation whose additions
 * have to wait for each other.
 * <p>
 *     The {@code long}s are added exactly, in 128 bits, and the sum is rounded to a {@code double} once. That is at least as
 *     accurate as the compensated sum of the converted values, which rounds every value above {@code 2^53} before adding it.
 *     The intermediate state is the one of {@link SumDoubleAggregator}, so the partial sums are merged like the ones of a
 *     sum of {@code double}s.
 * </p>
 * <p>
 *     This is written by hand, and not generated from an annotated class, for {@link #addArrayVector the loop over a page}.
 *     A generated aggregator adds the values to its state one by one. Here every addition would wait for the carry of the
 *     one before it, which keeps the jvm from adding many values in one instruction.
 * </p>
 */
public final class SumAsDoubleLongAggregatorFunction implements AggregatorFunction {
    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("value", ElementType.DOUBLE),
        new IntermediateStateDesc("delta", ElementType.DOUBLE),
        new IntermediateStateDesc("seen", ElementType.BOOLEAN)
    );

    private static final long LOW_MASK = 0xFFFF_FFFFL;

    public static List<IntermediateStateDesc> intermediateStateDesc() {
        return INTERMEDIATE_STATE_DESC;
    }

    private final List<Integer> channels;

    /**
     * The sum of the {@code long}s as a 128 bits two's complement number, {@code hi * 2^64 + unsigned(lo)}. It can't
     * overflow: that takes {@code 2^64} values.
     */
    private long hi;
    private long lo;
    /** The partial sums that arrived as intermediate input. */
    private final CompensatedSum merged = new CompensatedSum();
    private boolean seen;

    SumAsDoubleLongAggregatorFunction(List<Integer> channels) {
        this.channels = channels;
    }

    @Override
    public int intermediateBlockCount() {
        return INTERMEDIATE_STATE_DESC.size();
    }

    @Override
    public void addRawInput(Page page, BooleanVector mask) {
        if (mask.allFalse()) {
            // Entire page masked away
            return;
        }
        LongBlock block = page.getBlock(channels.get(0));
        if (block.areAllValuesNull()) {
            return;
        }
        LongVector vector = block.asVector();
        if (mask.allTrue()) {
            if (vector == null) {
                addBlock(block);
            } else {
                addVector(vector);
            }
        } else if (vector == null) {
            addBlock(block, mask);
        } else {
            addVector(vector, mask);
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
     * The hot loop. The low and the high 32 bits of the values are summed apart, each in a {@code long} that they can't
     * overflow: a vector has less than {@code 2^31} values. So there are no carries to chase inside the loop, just two
     * plain sums that the jvm vectorizes. Keep the loop as simple as it is: unrolling it by hand stops that.
     */
    private void addArrayVector(LongArrayVector vector) {
        int positions = vector.getPositionCount();
        long low = 0;
        long high = 0;
        for (int p = 0; p < positions; p++) {
            long v = vector.getLong(p);
            low += v & LOW_MASK;
            high += v >> 32;
        }
        // The sum is high * 2^32 + low. Move what low has above 32 bits into high, then it is mid * 2^32 + the low 32 bits.
        long mid = high + (low >>> 32);
        add128(mid >> 32, (mid << 32) | (low & LOW_MASK));
    }

    private void addVector(LongVector vector, BooleanVector mask) {
        for (int p = 0; p < vector.getPositionCount(); p++) {
            if (mask.getBoolean(p) == false) {
                continue;
            }
            seen = true;
            add(vector.getLong(p));
        }
    }

    private void addBlock(LongBlock block) {
        for (int p = 0; p < block.getPositionCount(); p++) {
            addPosition(block, p);
        }
    }

    private void addBlock(LongBlock block, BooleanVector mask) {
        for (int p = 0; p < block.getPositionCount(); p++) {
            if (mask.getBoolean(p)) {
                addPosition(block, p);
            }
        }
    }

    /**
     * Adds every value of a position, which has none if it is null and many if it is multivalued.
     */
    private void addPosition(LongBlock block, int position) {
        int count = block.getValueCount(position);
        if (count == 0) {
            return;
        }
        seen = true;
        int start = block.getFirstValueIndex(position);
        int end = start + count;
        for (int i = start; i < end; i++) {
            add(block.getLong(i));
        }
    }

    private void add(long v) {
        // The sign of the value extends into the high word.
        add128(v >> 63, v);
    }

    private void add128(long addHi, long addLo) {
        long next = lo + addLo;
        // The low word carries when it wrapped around.
        hi += addHi + (Long.compareUnsigned(next, lo) < 0 ? 1 : 0);
        lo = next;
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
        assert value.getPositionCount() == 1;
        DoubleVector delta = ((DoubleBlock) deltaUncast).asVector();
        assert delta.getPositionCount() == 1;
        BooleanVector inSeen = ((BooleanBlock) seenUncast).asVector();
        assert inSeen.getPositionCount() == 1;
        if (inSeen.getBoolean(0)) {
            merged.add(value.getDouble(0), delta.getDouble(0));
            seen = true;
        }
    }

    /**
     * The sum as a double and what rounding it lost, so that {@code value + delta} is the exact sum of the {@code long}s,
     * plus the partial sums.
     */
    private CompensatedSum total() {
        BigInteger exact = BigInteger.valueOf(hi).shiftLeft(64).add(new BigInteger(Long.toUnsignedString(lo)));
        double value = exact.doubleValue();
        double delta = exact.subtract(new BigDecimal(value).toBigInteger()).doubleValue();
        CompensatedSum total = new CompensatedSum(value, delta);
        total.add(merged.value(), merged.delta());
        return total;
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
