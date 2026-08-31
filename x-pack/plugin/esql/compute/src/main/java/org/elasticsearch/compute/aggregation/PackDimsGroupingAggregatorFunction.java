/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DimsPacker;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

import java.util.List;

public final class PackDimsGroupingAggregatorFunction implements GroupingAggregatorFunction {
    static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(new IntermediateStateDesc("values", ElementType.BYTES_REF));

    private final int[] channels;
    private final DriverContext driverContext;
    private final DimensionValuesByteRefGroupingAggregatorFunction delegate;

    public PackDimsGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext) {
        this.channels = channels.stream().mapToInt(n -> n).toArray();
        this.driverContext = driverContext;
        this.delegate = new DimensionValuesByteRefGroupingAggregatorFunction(channels, driverContext);
    }

    @Override
    public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
        delegate.selectedMayContainUnseenGroups(seenGroupIds);
    }

    // This should not run on the production, but add these for completeness
    @Override
    public AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds, Page page) {
        final Block[] inputBlocks = new Block[channels.length];
        for (int i = 0; i < channels.length; i++) {
            inputBlocks[i] = page.getBlock(channels[i]);
        }
        final var valuesBlock = DimsPacker.packMultiColumns(driverContext, inputBlocks).asBlock();
        return new AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groupIds, int maxGroupId) {
                delegate.addInputValuesBlock(positionOffset, groupIds, valuesBlock);
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds, int maxGroupId) {
                delegate.addInputValuesBlock(positionOffset, groupIds, valuesBlock);
            }

            @Override
            public void add(int positionOffset, IntVector groupIds, int maxGroupId) {
                var valuesVector = valuesBlock.asVector();
                if (valuesVector != null) {
                    delegate.addInputValuesVector(positionOffset, groupIds, valuesVector);
                } else {
                    delegate.addInputValuesBlock(positionOffset, groupIds, valuesBlock);
                }
            }

            @Override
            public void close() {
                valuesBlock.close();
            }
        };
    }

    @Override
    public int intermediateBlockCount() {
        return INTERMEDIATE_STATE_DESC.size();
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntArrayBlock groups, int maxGroupId, Page page) {
        delegate.addIntermediateInput(positionOffset, groups, maxGroupId, page);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, int maxGroupId, Page page) {
        delegate.addIntermediateInput(positionOffset, groups, maxGroupId, page);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntVector groups, int maxGroupId, Page page) {
        delegate.addIntermediateInput(positionOffset, groups, maxGroupId, page);
    }

    @Override
    public PreparedForEvaluation prepareEvaluateIntermediate(IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return delegate.prepareEvaluateIntermediate(selected, ctx);
    }

    @Override
    public PreparedForEvaluation prepareEvaluateFinal(IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return delegate.prepareEvaluateFinal(selected, ctx);
    }

    @Override
    public void close() {
        Releasables.close(delegate);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append("[");
        sb.append("channels=").append(channels);
        sb.append("]");
        return sb.toString();
    }
}
