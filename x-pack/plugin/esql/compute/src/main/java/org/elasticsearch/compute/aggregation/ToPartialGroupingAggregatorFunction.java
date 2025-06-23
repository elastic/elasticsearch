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
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.List;

/**
 * An internal aggregate function that always emits intermediate (or partial) output regardless of the aggregate mode.
 * The intermediate output should be consumed by {@link FromPartialGroupingAggregatorFunction}, which always receives
 * the intermediate input. Since an intermediate aggregate output can consist of multiple blocks, we wrap these output
 * blocks in a single composite block. The {@link FromPartialGroupingAggregatorFunction} then unwraps this input block
 * into multiple primitive blocks and passes them to the delegating GroupingAggregatorFunction.
 * Both of these commands yield the same result, except the second plan executes aggregates twice:
 * <pre>
 * ```
 * | ... before
 * | af(x) BY g
 * | ... after
 * ```
 * ```
 * | ... before
 * | $x = to_partial(af(x)) BY g
 * | from_partial($x, af(_)) BY g
 * | ...  after
 * </pre>
 * ```
 */
public class ToPartialGroupingAggregatorFunction implements GroupingAggregatorFunction {
    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("partial", ElementType.COMPOSITE, "partial_agg")
    );

    public static List<IntermediateStateDesc> intermediateStateDesc() {
        return INTERMEDIATE_STATE_DESC;
    }

    private final GroupingAggregatorFunction delegate;
    private final List<Integer> channels;

    public ToPartialGroupingAggregatorFunction(GroupingAggregatorFunction delegate, List<Integer> channels) {
        this.delegate = delegate;
        this.channels = channels;
    }

    @Override
    public AddInput prepareProcessPage(SeenGroupIds seenGroupIds, Page page) {
        return delegate.prepareProcessPage(seenGroupIds, page);
    }

    @Override
    public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
        delegate.selectedMayContainUnseenGroups(seenGroupIds);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntVector groupIdVector, Page page) {
        final CompositeBlock inputBlock = page.getBlock(channels.get(0));
        delegate.addIntermediateInput(positionOffset, groupIdVector, inputBlock.asPage());
    }

    @Override
    public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
        if (input instanceof ToPartialGroupingAggregatorFunction toPartial) {
            input = toPartial.delegate;
        }
        delegate.addIntermediateRowInput(groupId, input, position);
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
        final Block[] partialBlocks = new Block[delegate.intermediateBlockCount()];
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
        evaluateIntermediate(blocks, offset, selected);
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
        return getClass().getSimpleName() + "[" + "channels=" + channels + ",delegate=" + delegate + "]";
    }
}
