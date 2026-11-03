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
import org.elasticsearch.compute.data.DocOrigin;
import org.elasticsearch.compute.data.DocOriginTable;
import org.elasticsearch.compute.data.DocOrigins;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.AlwaysReferencedIndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Keeps the first document each group sees. The documents can still be on the node that read them, and the output then
 * references their shards. Or they left that node, like the rows a {@code FORK} merges on the coordinator, and the output
 * carries their origins instead. One aggregation sees one kind only, because a vector can't hold both.
 */
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
    /**
     * Three ints per group: the shard of the document, or the number of its origin once it left its node, then its
     * segment and its doc.
     */
    private IntArray docs;
    private final Map<Integer, RefCounted> contextRefs = new HashMap<>();
    /**
     * The origins of the documents that left their node, {@code null} until the first such page.
     */
    @Nullable
    private DocOriginTable origins;
    private boolean sawDocumentsOfThisNode;

    public FirstDocIdGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext) {
        this.channel = channels.getFirst();
        this.driverContext = driverContext;
        this.bigArrays = driverContext.bigArrays();
        boolean success = false;
        try {
            this.docs = bigArrays.newIntArray(1024, false);
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
        boolean detached = docVector.origins() != null;
        if (detached ? sawDocumentsOfThisNode : origins != null) {
            throw new IllegalStateException("first_doc_id can't mix documents of this node with documents that left their node");
        }
        if (detached && origins == null) {
            origins = new DocOriginTable(driverContext.breaker());
        }
        sawDocumentsOfThisNode |= detached == false;
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
        int shard = origins == null ? docVector.shards().getInt(valuePosition) : origins.ordinal(docVector.origin(valuePosition));
        int segment = docVector.segments().getInt(valuePosition);
        int doc = docVector.docs().getInt(valuePosition);
        docs = bigArrays.grow(docs, 3L * groupId + 3);
        docs.set(3L * groupId, shard);
        docs.set(3L * groupId + 1, segment);
        docs.set(3L * groupId + 2, doc);
        if (origins == null && contextRefs.containsKey(shard) == false) {
            var refCounted = docVector.shardRefCounted(valuePosition);
            refCounted.mustIncRef();
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
    public GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateIntermediate(
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        return this::evaluate;
    }

    private void evaluate(Block[] blocks, int offset, IntVector selectedInPage) {
        final BlockFactory blockFactory = driverContext.blockFactory();
        final int positionCount = selectedInPage.getPositionCount();
        try (
            var segmentBuilder = blockFactory.newIntVectorFixedBuilder(positionCount);
            var docBuilder = blockFactory.newIntVectorFixedBuilder(positionCount)
        ) {
            for (int p = 0; p < positionCount; p++) {
                int group = selectedInPage.getInt(p);
                segmentBuilder.appendInt(docs.get(3L * group + 1));
                docBuilder.appendInt(docs.get(3L * group + 2));
            }
            DocVector.Config config = DocVector.config().mayContainDuplicates();
            final IndexedByShardId<? extends RefCounted> refs;
            final IntVector shardVector;
            if (origins != null) {
                List<DocOrigin> pageOrigins = new ArrayList<>();
                shardVector = pageOrdinals(selectedInPage, pageOrigins);
                config.origins(DocOrigins.of(pageOrigins));
                refs = AlwaysReferencedIndexedByShardId.INSTANCE;
            } else if (contextRefs.size() == 1) {
                shardVector = blockFactory.newConstantIntVector(Iterables.get(contextRefs.keySet(), 0), positionCount);
                refs = new MappedShardRefs<>(contextRefs);
            } else {
                try (var shardBuilder = blockFactory.newIntVectorFixedBuilder(positionCount)) {
                    for (int p = 0; p < positionCount; p++) {
                        int group = selectedInPage.getInt(p);
                        shardBuilder.appendInt(docs.get(3L * group));
                    }
                    shardVector = shardBuilder.build();
                }
                refs = new MappedShardRefs<>(contextRefs);
            }
            IntVector segmentVector = null;
            IntVector docVector = null;
            try {
                segmentVector = segmentBuilder.build();
                docVector = docBuilder.build();
                blocks[offset] = new DocVector(refs, shardVector, segmentVector, docVector, config).asBlock();
            } finally {
                if (blocks[offset] == null) {
                    Releasables.closeExpectNoException(shardVector, segmentVector, docVector);
                }
            }
        }
    }

    /**
     * The shard vector of an output page of documents that left their node: for each selected group, the ordinal of its
     * origin in {@code pageOrigins}, which numbers them in the order the page first references them.
     */
    private IntVector pageOrdinals(IntVector selectedInPage, List<DocOrigin> pageOrigins) {
        final int positionCount = selectedInPage.getPositionCount();
        Map<Integer, Integer> pageOrdinalOfTableOrdinal = new HashMap<>();
        try (var shardBuilder = driverContext.blockFactory().newIntVectorFixedBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                int tableOrdinal = docs.get(3L * selectedInPage.getInt(p));
                int pageOrdinal = pageOrdinalOfTableOrdinal.computeIfAbsent(tableOrdinal, ordinal -> {
                    pageOrigins.add(origins.get(ordinal));
                    return pageOrigins.size() - 1;
                });
                shardBuilder.appendInt(pageOrdinal);
            }
            return shardBuilder.build();
        }
    }

    public record MappedShardRefs<T>(Map<Integer, T> refs) implements IndexedByShardId<T> {
        @Override
        public T get(int shardId) {
            return refs.get(shardId);
        }

        @Override
        public Iterable<? extends T> iterable() {
            return refs.values();
        }

        @Override
        public int size() {
            return refs.size();
        }

        @Override
        public <S> IndexedByShardId<S> map(Function<T, S> mapper) {
            Map<Integer, S> newMap = new HashMap<>();
            for (var entry : refs.entrySet()) {
                newMap.put(entry.getKey(), mapper.apply(entry.getValue()));
            }
            return new MappedShardRefs<>(newMap);
        }
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(docs, origins, () -> {
            for (RefCounted ref : contextRefs.values()) {
                ref.decRef();
            }
        });
    }

    @Override
    public GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateFinal(
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        return this::evaluate;
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
