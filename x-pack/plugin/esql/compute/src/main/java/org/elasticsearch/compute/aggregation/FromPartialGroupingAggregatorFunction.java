/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.CompositeBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.List;

/**
 * @see ToPartialGroupingAggregatorFunction
 */
public class FromPartialGroupingAggregatorFunction implements GroupingAggregatorFunction {
    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("partial", ElementType.COMPOSITE, "partial_agg")
    );

    public static List<IntermediateStateDesc> intermediateStateDesc() {
        return INTERMEDIATE_STATE_DESC;
    }

    private final GroupingAggregatorFunction delegate;
    private final int inputChannel;

    public FromPartialGroupingAggregatorFunction(GroupingAggregatorFunction delegate, int inputChannel) {
        this.delegate = delegate;
        this.inputChannel = inputChannel;
    }

    @Override
    public AddInput prepareProcessPage(SeenGroupIds seenGroupIds, Page page) {
        return new AddInput() {
            @Override
            public void add(int positionOffset, IntBlock groupIds) {
                assert false : "Intermediate group id must not have nulls";
                throw new IllegalStateException("Intermediate group id must not have nulls");
            }

            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                assert false : "Intermediate group id must not have nulls";
                throw new IllegalStateException("Intermediate group id must not have nulls");
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                assert false : "Intermediate group id must not have nulls";
                throw new IllegalStateException("Intermediate group id must not have nulls");
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                addIntermediateInput(positionOffset, groupIds, page);
            }

            @Override
            public void close() {}
        };
    }

    @Override
    public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
        delegate.selectedMayContainUnseenGroups(seenGroupIds);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntVector groupIdVector, Page page) {
        final CompositeBlock inputBlock = page.getBlock(inputChannel);
        delegate.addIntermediateInput(positionOffset, groupIdVector, inputBlock.asPage());
    }

    @Override
    public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
        if (input instanceof FromPartialGroupingAggregatorFunction toPartial) {
            input = toPartial.delegate;
        }
        delegate.addIntermediateRowInput(groupId, input, position);
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
        Block[] partialBlocks = new Block[delegate.intermediateBlockCount()];
        boolean success = false;
        try {
            delegate.evaluateIntermediate(partialBlocks, 0, selected);
            blocks[offset] = new CompositeBlock(partialBlocks);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(partialBlocks);
            }
        }
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, IntVector selected, GroupingAggregatorEvaluationContext evaluationContext) {
        delegate.evaluateFinal(blocks, offset, selected, evaluationContext);
    }

    @Override
    public int intermediateBlockCount() {
        return INTERMEDIATE_STATE_DESC.size();
    }

    @Override
    public void close() {
        Releasables.close(delegate);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + "channel=" + inputChannel + ",delegate=" + delegate + "]";
    }
}
