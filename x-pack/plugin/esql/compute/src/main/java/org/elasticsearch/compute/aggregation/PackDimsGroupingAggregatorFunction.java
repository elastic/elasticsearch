/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DimsPacker;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
import java.util.List;

public final class PackDimsGroupingAggregatorFunction implements GroupingAggregatorFunction {
    static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(new IntermediateStateDesc("values", ElementType.BYTES_REF));

    private final int[] channels;
    private final DriverContext driverContext;
    private final DimensionValuesByteRefGroupingAggregatorFunction delegate;

    private OrdinalState ordinalState;

    public PackDimsGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext) {
        this.channels = channels.stream().mapToInt(n -> n).toArray();
        this.driverContext = driverContext;
        this.delegate = new DimensionValuesByteRefGroupingAggregatorFunction(channels, driverContext);
        this.ordinalState = new OrdinalState(driverContext);
    }

    @Override
    public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
        delegate.selectedMayContainUnseenGroups(seenGroupIds);
    }

    // This should not run on the production, but add these for completeness
    @Override
    public AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds, Page page) {
        flushOrdinalStateToDelegate();
        final Block[] inputBlocks = new Block[channels.length];
        for (int i = 0; i < channels.length; i++) {
            inputBlocks[i] = page.getBlock(channels[i]);
        }
        final var valuesBlock = DimsPacker.packMultiColumns(driverContext, inputBlocks).asBlock();
        return new AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                delegate.addInputValuesBlock(positionOffset, groupIds, valuesBlock);
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                delegate.addInputValuesBlock(positionOffset, groupIds, valuesBlock);
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
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
    public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
        flushOrdinalStateToDelegate();
        delegate.addIntermediateInput(positionOffset, groups, page);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
        flushOrdinalStateToDelegate();
        delegate.addIntermediateInput(positionOffset, groups, page);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
        BytesRefBlock block = page.getBlock(channels[0]);
        if (block.areAllValuesNull()) {
            return;
        }
        BytesRefVector vector = block.asVector();
        if (vector == null || ordinalState == null) {
            flushOrdinalStateToDelegate();
            delegate.addInputValuesBlock(positionOffset, groups, block);
            return;
        }
        OrdinalBytesRefVector ordinals = vector.asOrdinals();
        if (ordinals != null) {
            ordinalState.addOrdinalVector(groups, ordinals);
        } else {
            ordinalState.addVector(groups, vector);
        }
    }

    private void flushOrdinalStateToDelegate() {
        if (ordinalState != null) {
            var state = ordinalState;
            ordinalState = null;
            try (state) {
                state.fallbackToDelegate();
            }
        }
    }

    @Override
    public PreparedForEvaluation prepareEvaluateIntermediate(IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        if (ordinalState != null) {
            return ordinalState.preparedForEvaluation(selected, ctx);
        } else {
            return delegate.prepareEvaluateIntermediate(selected, ctx);
        }
    }

    @Override
    public PreparedForEvaluation prepareEvaluateFinal(IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return prepareEvaluateIntermediate(selected, ctx);
    }

    private class OrdinalState implements Releasable {
        private BytesRefArray values;
        private int[] ords;
        private int maxGroupId = -1;

        OrdinalState(DriverContext driverContext) {
            this.values = new BytesRefArray(PageCacheRecycler.PAGE_SIZE_IN_BYTES, driverContext.bigArrays());
            this.ords = new int[1024];
            Arrays.fill(this.ords, -1);
        }

        private void ensureOrds(int groupId) {
            if (groupId >= ords.length) {
                int oldLen = ords.length;
                ords = ArrayUtil.grow(ords, groupId + 1);
                Arrays.fill(ords, oldLen, ords.length, -1);
            }
        }

        void addVector(IntVector groupIds, BytesRefVector vector) {
            BytesRef scratch = new BytesRef();
            for (int p = 0; p < groupIds.getPositionCount(); p++) {
                int groupId = groupIds.getInt(p);
                ensureOrds(groupId);
                if (ords[groupId] == -1) {
                    ords[groupId] = Math.toIntExact(values.size());
                    values.append(vector.getBytesRef(p, scratch));
                    maxGroupId = Math.max(maxGroupId, groupId);
                }
            }
        }

        void addOrdinalVector(IntVector groupIds, OrdinalBytesRefVector ordinalVector) {
            BytesRef scratch = new BytesRef();
            int nextOrd = Math.toIntExact(values.size());
            BytesRefVector dict = ordinalVector.getDictionaryVector();
            int[] mappedOrds = new int[dict.getPositionCount()];
            Arrays.fill(mappedOrds, -1);
            IntVector ordinals = ordinalVector.getOrdinalsVector();
            for (int p = 0; p < groupIds.getPositionCount(); p++) {
                int groupId = groupIds.getInt(p);
                ensureOrds(groupId);
                if (ords[groupId] == -1) {
                    int ord = ordinals.getInt(p);
                    int mappedOrd = mappedOrds[ord];
                    if (mappedOrd == -1) {
                        values.append(dict.getBytesRef(ord, scratch));
                        mappedOrds[ord] = mappedOrd = nextOrd++;
                    }
                    ords[groupId] = mappedOrd;
                    maxGroupId = Math.max(maxGroupId, groupId);
                }
            }
        }

        void fallbackToDelegate() {
            if (maxGroupId == -1) {
                return;
            }
            BytesRefVector dict = driverContext.blockFactory().newBytesRefArrayVector(values, Math.toIntExact(values.size()));
            values = null;
            try {
                final IntBlock ordinals;
                try (var builder = driverContext.blockFactory().newIntBlockBuilder(maxGroupId + 1)) {
                    for (int g = 0; g <= maxGroupId; g++) {
                        int ord = ords[g];
                        if (ord == -1) {
                            builder.appendNull();
                        } else {
                            builder.appendInt(ord);
                        }
                    }
                    ordinals = builder.build();
                }
                try (
                    IntVector groupIds = driverContext.blockFactory().newIntRangeVector(0, maxGroupId + 1);
                    OrdinalBytesRefBlock valuesBlock = new OrdinalBytesRefBlock(ordinals, dict)
                ) {
                    dict = null;
                    delegate.addInputValuesBlock(0, groupIds, valuesBlock);
                }
            } finally {
                ords = null;
                Releasables.close(dict);
            }
        }

        PreparedForEvaluation preparedForEvaluation(IntVector selected, GroupingAggregatorEvaluationContext ctx) {
            int valuesSize = Math.toIntExact(values.size());
            return (blocks, offset, selectedInPage) -> {
                int[] mappedOrds = new int[valuesSize];
                Arrays.fill(mappedOrds, -1);
                int nextOrd = 0;
                BytesRef scratch = new BytesRef();
                final int positionCount = selectedInPage.getPositionCount();
                try (
                    var ordBuilder = ctx.blockFactory().newIntBlockBuilder(positionCount);
                    var dictBuilder = ctx.blockFactory().newBytesRefVectorBuilder(positionCount)
                ) {
                    for (int p = 0; p < positionCount; p++) {
                        int groupId = selectedInPage.getInt(p);
                        int ord = groupId < ords.length ? ords[groupId] : -1;
                        if (ord < 0) {
                            ordBuilder.appendNull();
                            continue;
                        }
                        int mappedOrd = mappedOrds[ord];
                        if (mappedOrd == -1) {
                            dictBuilder.appendBytesRef(values.get(ord, scratch));
                            mappedOrd = mappedOrds[ord] = nextOrd++;
                        }
                        ordBuilder.appendInt(mappedOrd);
                    }
                    IntBlock ordsBlock = null;
                    BytesRefVector dictVector = null;
                    try {
                        ordsBlock = ordBuilder.build();
                        dictVector = dictBuilder.build();
                        blocks[offset] = new OrdinalBytesRefBlock(ordsBlock, dictVector);
                        ordsBlock = null;
                        dictVector = null;
                    } finally {
                        Releasables.close(ordsBlock, dictVector);
                    }
                }
            };
        }

        @Override
        public void close() {
            Releasables.close(values);
            values = null;
        }
    }

    @Override
    public void close() {
        Releasables.close(delegate, ordinalState);
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
