/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.LongVectorBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;

/**
 * {@link AggregatorFunction} implementation for {@link LastValueIntAggregator}.
 * This class is geneLastValued. Do not edit it.
 */
public final class LastValueLongAggregatorFunction implements AggregatorFunction {
    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("LastValue", ElementType.BYTES_REF)
    );

    private final DriverContext driverContext;

    private final LastValueStates.SingleState state;

    private final List<Integer> channels;

    public LastValueLongAggregatorFunction(DriverContext driverContext, List<Integer> channels, LastValueStates.SingleState state) {
        this.driverContext = driverContext;
        this.channels = channels;
        this.state = state;
    }

    public static LastValueLongAggregatorFunction create(DriverContext driverContext, List<Integer> channels) {
        return new LastValueLongAggregatorFunction(driverContext, channels, LastValueIntAggregator.initSingle());
    }

    public static List<IntermediateStateDesc> intermediateStateDesc() {
        return INTERMEDIATE_STATE_DESC;
    }

    @Override
    public int intermediateBlockCount() {
        return INTERMEDIATE_STATE_DESC.size();
    }

    @Override
    public void addRawInput(Page page) {
        Block uncastBlock = page.getBlock(channels.get(0));
        if (uncastBlock.areAllValuesNull()) {
            return;
        }
        LongBlock block = (LongBlock) uncastBlock;
        LongVector vector = block.asVector();
        LongBlock tsBlock = (LongVectorBlock) page.getBlock(1);  // FIXME
        LongVector timestamps = tsBlock.asVector();
        if (vector != null) {
            addRawVector(vector, timestamps);
        } else {
            addRawBlock(block, tsBlock);
        }
    }

    private void addRawVector(LongVector vector, LongVector timestamps) {
        for (int i = 0; i < vector.getPositionCount(); i++) {
            LastValueLongAggregator.combine(state, vector.getLong(i), timestamps.getLong(i));
        }
    }

    private void addRawBlock(LongBlock block, LongBlock timestamps) {
        for (int p = 0; p < block.getPositionCount(); p++) {
            if (block.isNull(p)) {
                continue;
            }
            int start = block.getFirstValueIndex(p);
            int end = start + block.getValueCount(p);
            for (int i = start; i < end; i++) {
                LastValueLongAggregator.combine(state, block.getLong(i), timestamps.getLong(i));
            }
        }
    }

    @Override
    public void addIntermediateInput(Page page) {
        assert channels.size() == intermediateBlockCount();
        assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
        BytesRefVector LastValue = page.<BytesRefBlock>getBlock(channels.get(0)).asVector();
        assert LastValue.getPositionCount() == 1;
        BytesRef scratch = new BytesRef();
        LastValueLongAggregator.combineIntermediate(state, LastValue.getBytesRef(0, scratch));
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset) {
        state.toIntermediate(blocks, offset);
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
        blocks[offset] = LastValueLongAggregator.evaluateFinal(state, driverContext);
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
    public void close() {
        state.close();
    }
}
