/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class FirstDocIdGroupingAggregatorFunction implements GroupingAggregatorFunction {

    public static final class FunctionSupplier implements AggregatorFunctionSupplier {

        @Override
        public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
            throw new UnsupportedOperationException("non-grouping aggregator is not supported");
        }

        @Override
        public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
            return INTERMEDIATE_STATE_DESC;
        }

        @Override
        public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
            throw new UnsupportedOperationException("non-grouping aggregator is not supported");
        }

        @Override
        public FirstDocIdGroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
            return new FirstDocIdGroupingAggregatorFunction(channels, driverContext);
        }

        @Override
        public String describe() {
            return "first_doc_id";
        }
    }

    static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(new IntermediateStateDesc("_doc", ElementType.DOC));

    private final int channel;
    private final DriverContext driverContext;
    private int maxGroupId = -1;
    private final BigArrays bigArrays;
    private IntArray shards;
    private IntArray segments;
    private IntArray docIds;
    private final Map<Integer, RefCounted> contextRefs = new HashMap<>();

    public FirstDocIdGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext) {
        this.channel = channels.get(0);
        this.driverContext = driverContext;
        this.bigArrays = driverContext.bigArrays();
        boolean success = false;
        try {
            this.shards = bigArrays.newIntArray(1024, false);
            this.segments = bigArrays.newIntArray(1024, false);
            this.docIds = bigArrays.newIntArray(1024, false);
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    @Override
    public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {

    }

    @Override
    public AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds, Page page) {
        DocBlock docBlock = page.getBlock(channel);
        if (docBlock.areAllValuesNull()) {
            return new AddInput() {
                @Override
                public void add(int positionOffset, IntArrayBlock groupIds) {

                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groupIds) {

                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {

                }

                @Override
                public void close() {

                }
            };
        }
        DocVector docVector = docBlock.asVector();
        if (docVector == null) {
            assert false : "expected doc vector for first_doc_id";
            throw new IllegalStateException("expected doc vector for first_doc_id");
        }
        return new AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                addRawInput(positionOffset, groupIds, docVector);
            }

            @Override
            public void close() {

            }
        };
    }

    private void addRawInput(int positionOffset, IntVector groups, DocVector docVector) {
        int positionCount = groups.getPositionCount();
        if (groups.isConstant()) {
            int groupId = groups.getInt(0);
            if (groupId > maxGroupId) {
                collectOneDoc(groupId, docVector, positionOffset);
            }
        } else {
            for (int p = 0; p < positionCount; p++) {
                int groupId = groups.getInt(p);
                if (groupId > maxGroupId) {
                    collectOneDoc(groupId, docVector, p + positionOffset);
                }
            }
        }
    }

    private void collectOneDoc(int groupId, DocVector docVector, int valuePosition) {
        maxGroupId = groupId;
        shards = bigArrays.grow(shards, groupId + 1);
        int shard = docVector.shards().getInt(valuePosition);
        shards.set(groupId, shard);
        segments = bigArrays.grow(segments, groupId + 1);
        segments.set(groupId, docVector.segments().getInt(valuePosition));
        docIds = bigArrays.grow(docIds, groupId + 1);
        docIds.set(groupId, docVector.docs().getInt(valuePosition));
        if (contextRefs.containsKey(shard) == false) {
            RefCounted refCounted = docVector.shardRefCounted().get(shard);
            refCounted.incRef();
            contextRefs.put(shard, refCounted);
        }
    }

    @Override
    public int intermediateBlockCount() {
        return INTERMEDIATE_STATE_DESC.size();
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
        throw new UnsupportedOperationException("first_doc_id does not handle intermediate input");
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
        throw new UnsupportedOperationException("first_doc_id does not handle intermediate input");
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
        throw new UnsupportedOperationException("first_doc_id does not handle intermediate input");
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
        final BlockFactory blockFactory = driverContext.blockFactory();
        final int positionCount = selected.getPositionCount();
        try (
            var segmentBuilder = blockFactory.newIntVectorFixedBuilder(positionCount);
            var docBuilder = blockFactory.newIntVectorFixedBuilder(positionCount)
        ) {
            for (int p = 0; p < positionCount; p++) {
                int group = selected.getInt(p);
                segmentBuilder.appendInt(segments.get(group));
                docBuilder.appendInt(docIds.get(group));
            }
            final IntVector shardVector;
            if (contextRefs.size() == 1) {
                shardVector = blockFactory.newConstantIntVector(Iterables.get(contextRefs.keySet(), 0), positionCount);
            } else {
                try (var shardBuilder = blockFactory.newIntVectorFixedBuilder(positionCount)) {
                    for (int p = 0; p < positionCount; p++) {
                        int group = selected.getInt(p);
                        shardBuilder.appendInt(shards.get(group));
                    }
                    shardVector = shardBuilder.build();
                }
            }
            IntVector segmentVector = null;
            IntVector docVector = null;
            try {
                segmentVector = segmentBuilder.build();
                docVector = docBuilder.build();
                var unmodifiedContextRefs = Collections.unmodifiableMap(contextRefs);
                blocks[offset] = new DocVector(unmodifiedContextRefs::get, shardVector, segmentVector, docVector, null).asBlock();
            } finally {
                if (blocks[offset] == null) {
                    Releasables.closeExpectNoException(shardVector, segmentVector, docVector);
                }
            }
        }
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(shards, segments, docIds, () -> {
            for (RefCounted ref : contextRefs.values()) {
                ref.decRef();
            }
        });
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, IntVector selected, GroupingAggregatorEvaluationContext evalContext) {
        evaluateIntermediate(blocks, offset, selected);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append("[");
        sb.append("channel=").append(channel);
        sb.append("]");
        return sb.toString();
    }
}
