/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregator.Factory;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.HashAggregationOperator.GroupSpec;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Unlike {@link HashAggregationOperator}, this hash operator also extracts values or ordinals of the input documents.
 */
public class OrdinalsGroupingOperator implements Operator {
    public record OrdinalsGroupingOperatorFactory(
        List<BlockLoader> blockLoaders,
        List<ValuesSourceReaderOperator.ShardContext> shardContexts,
        ElementType groupingElementType,
        int docChannel,
        String groupingField,
        List<Factory> aggregators,
        int maxPageSize,
        BigArrays bigArrays
    ) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new OrdinalsGroupingOperator(
                blockLoaders,
                shardContexts,
                groupingElementType,
                docChannel,
                groupingField,
                aggregators,
                maxPageSize,
                bigArrays,
                driverContext
            );
        }

        @Override
        public String describe() {
            return "OrdinalsGroupingOperator(aggs = " + aggregators.stream().map(Describable::describe).collect(joining(", ")) + ")";
        }
    }

    private final List<BlockLoader> blockLoaders;
    private final List<ValuesSourceReaderOperator.ShardContext> shardContexts;
    private final int docChannel;
    private final String groupingField;

    private final List<Factory> aggregatorFactories;
    private final ElementType groupingElementType;
    private final Map<SegmentID, OrdinalSegmentAggregator> ordinalAggregators;
    private final BigArrays bigArrays;

    private final DriverContext driverContext;

    private boolean finished = false;

    // used to extract and aggregate values
    private final int maxPageSize;
    private ValuesAggregator valuesAggregator;

    public OrdinalsGroupingOperator(
        List<BlockLoader> blockLoaders,
        List<ValuesSourceReaderOperator.ShardContext> shardContexts,
        ElementType groupingElementType,
        int docChannel,
        String groupingField,
        List<GroupingAggregator.Factory> aggregatorFactories,
        int maxPageSize,
        BigArrays bigArrays,
        DriverContext driverContext
    ) {
        Objects.requireNonNull(aggregatorFactories);
        this.blockLoaders = blockLoaders;
        this.shardContexts = shardContexts;
        this.groupingElementType = groupingElementType;
        this.docChannel = docChannel;
        this.groupingField = groupingField;
        this.aggregatorFactories = aggregatorFactories;
        this.ordinalAggregators = new HashMap<>();
        this.maxPageSize = maxPageSize;
        this.bigArrays = bigArrays;
        this.driverContext = driverContext;
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
        checkState(needsInput(), "Operator is already finishing");
        requireNonNull(page, "page is null");
        DocVector docVector = page.<DocBlock>getBlock(docChannel).asVector();
        final int shardIndex = docVector.shards().getInt(0);
        final var blockLoader = blockLoaders.get(shardIndex);
        boolean pagePassed = false;
        try {
            if (docVector.singleSegmentNonDecreasing() && blockLoader.supportsOrdinals()) {
                final IntVector segmentIndexVector = docVector.segments();
                assert segmentIndexVector.isConstant();
                final OrdinalSegmentAggregator ordinalAggregator = this.ordinalAggregators.computeIfAbsent(
                    new SegmentID(shardIndex, segmentIndexVector.getInt(0)),
                    k -> {
                        try {
                            return new OrdinalSegmentAggregator(
                                driverContext.blockFactory(),
                                this::createGroupingAggregators,
                                () -> blockLoader.ordinals(shardContexts.get(k.shardIndex).reader().leaves().get(k.segmentIndex)),
                                bigArrays
                            );
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                );
                pagePassed = true;
                ordinalAggregator.addInput(docVector.docs(), page);
            } else {
                if (valuesAggregator == null) {
                    int channelIndex = page.getBlockCount(); // extractor will append a new block at the end
                    valuesAggregator = new ValuesAggregator(
                        blockLoaders,
                        shardContexts,
                        groupingElementType,
                        docChannel,
                        groupingField,
                        channelIndex,
                        aggregatorFactories,
                        maxPageSize,
                        driverContext
                    );
                }
                pagePassed = true;
                valuesAggregator.addInput(page);
            }
        } finally {
            if (pagePassed == false) {
                Releasables.closeExpectNoException(page::releaseBlocks);
            }
        }
    }

    private List<GroupingAggregator> createGroupingAggregators() {
        boolean success = false;
        List<GroupingAggregator> aggregators = new ArrayList<>(aggregatorFactories.size());
        try {
            for (GroupingAggregator.Factory aggregatorFactory : aggregatorFactories) {
                aggregators.add(aggregatorFactory.apply(driverContext));
            }
            success = true;
            return aggregators;
        } finally {
            if (success == false) {
                Releasables.close(aggregators);
            }
        }
    }

    @Override
    public Page getOutput() {
        if (finished == false) {
            return null;
        }
        if (valuesAggregator != null) {
            try {
                return valuesAggregator.getOutput();
            } finally {
                final ValuesAggregator aggregator = this.valuesAggregator;
                this.valuesAggregator = null;
                Releasables.close(aggregator);
            }
        }
        if (ordinalAggregators.isEmpty() == false) {
            try {
                return mergeOrdinalsSegmentResults();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                Releasables.close(() -> Releasables.close(ordinalAggregators.values()), ordinalAggregators::clear);
            }
        }
        return null;
    }

    @Override
    public void finish() {
        finished = true;
        if (valuesAggregator != null) {
            valuesAggregator.finish();
        }
    }

    private Page mergeOrdinalsSegmentResults() throws IOException {
        // TODO: Should we also combine from the results from ValuesAggregator
        final PriorityQueue<AggregatedResultIterator> pq = new PriorityQueue<>(ordinalAggregators.size()) {
            @Override
            protected boolean lessThan(AggregatedResultIterator a, AggregatedResultIterator b) {
                return a.currentTerm.compareTo(b.currentTerm) < 0;
            }
        };
        final List<GroupingAggregator> aggregators = createGroupingAggregators();
        try {
            boolean seenNulls = false;
            for (OrdinalSegmentAggregator agg : ordinalAggregators.values()) {
                if (agg.seenNulls()) {
                    seenNulls = true;
                    for (int i = 0; i < aggregators.size(); i++) {
                        aggregators.get(i).addIntermediateRow(0, agg.aggregators.get(i), 0);
                    }
                }
            }
            for (OrdinalSegmentAggregator agg : ordinalAggregators.values()) {
                final AggregatedResultIterator it = agg.getResultIterator();
                if (it.next()) {
                    pq.add(it);
                }
            }
            final int startPosition = seenNulls ? 0 : -1;
            int position = startPosition;
            final BytesRefBuilder lastTerm = new BytesRefBuilder();
            final Block[] blocks;
            final int[] aggBlockCounts;
            try (var keysBuilder = driverContext.blockFactory().newBytesRefBlockBuilder(1)) {
                if (seenNulls) {
                    keysBuilder.appendNull();
                }
                while (pq.size() > 0) {
                    final AggregatedResultIterator top = pq.top();
                    if (position == startPosition || lastTerm.get().equals(top.currentTerm) == false) {
                        position++;
                        lastTerm.copyBytes(top.currentTerm);
                        keysBuilder.appendBytesRef(top.currentTerm);
                    }
                    for (int i = 0; i < top.aggregators.size(); i++) {
                        aggregators.get(i).addIntermediateRow(position, top.aggregators.get(i), top.currentPosition());
                    }
                    if (top.next()) {
                        pq.updateTop();
                    } else {
                        pq.pop();
                    }
                }
                aggBlockCounts = aggregators.stream().mapToInt(GroupingAggregator::evaluateBlockCount).toArray();
                blocks = new Block[1 + Arrays.stream(aggBlockCounts).sum()];
                blocks[0] = keysBuilder.build();
            }
            boolean success = false;
            try {
                try (IntVector selected = IntVector.range(0, blocks[0].getPositionCount(), driverContext.blockFactory())) {
                    int offset = 1;
                    for (int i = 0; i < aggregators.size(); i++) {
                        aggregators.get(i).evaluate(blocks, offset, selected, driverContext);
                        offset += aggBlockCounts[i];
                    }
                }
                success = true;
                return new Page(blocks);
            } finally {
                if (success == false) {
                    Releasables.closeExpectNoException(blocks);
                }
            }
        } finally {
            Releasables.close(() -> Releasables.close(aggregators));
        }
    }

    @Override
    public boolean isFinished() {
        return finished && valuesAggregator == null && ordinalAggregators.isEmpty();
    }

    @Override
    public void close() {
        Releasables.close(() -> Releasables.close(ordinalAggregators.values()), valuesAggregator);
    }

    private static void checkState(boolean condition, String msg) {
        if (condition == false) {
            throw new IllegalArgumentException(msg);
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[" + "aggregators=" + aggregatorFactories + "]";
    }

    record SegmentID(int shardIndex, int segmentIndex) {

    }

    static final class OrdinalSegmentAggregator implements Releasable, SeenGroupIds {
        private final BlockFactory blockFactory;
        private final List<GroupingAggregator> aggregators;
        private final CheckedSupplier<SortedSetDocValues, IOException> docValuesSupplier;
        private final BitArray visitedOrds;
        private BlockOrdinalsReader currentReader;

        OrdinalSegmentAggregator(
            BlockFactory blockFactory,
            Supplier<List<GroupingAggregator>> aggregatorsSupplier,
            CheckedSupplier<SortedSetDocValues, IOException> docValuesSupplier,
            BigArrays bigArrays
        ) throws IOException {
            boolean success = false;
            List<GroupingAggregator> groupingAggregators = null;
            BitArray bitArray = null;
            try {
                final SortedSetDocValues sortedSetDocValues = docValuesSupplier.get();
                bitArray = new BitArray(sortedSetDocValues.getValueCount(), bigArrays);
                groupingAggregators = aggregatorsSupplier.get();
                this.currentReader = new BlockOrdinalsReader(sortedSetDocValues, blockFactory);
                this.blockFactory = blockFactory;
                this.docValuesSupplier = docValuesSupplier;
                this.aggregators = groupingAggregators;
                this.visitedOrds = bitArray;
                success = true;
            } finally {
                if (success == false) {
                    if (bitArray != null) Releasables.close(bitArray);
                    if (groupingAggregators != null) Releasables.close(groupingAggregators);
                }
            }
        }

        void addInput(IntVector docs, Page page) {
            try {
                GroupingAggregatorFunction.AddInput[] prepared = new GroupingAggregatorFunction.AddInput[aggregators.size()];
                for (int i = 0; i < prepared.length; i++) {
                    prepared[i] = aggregators.get(i).prepareProcessPage(this, page);
                }

                if (BlockOrdinalsReader.canReuse(currentReader, docs.getInt(0)) == false) {
                    currentReader = new BlockOrdinalsReader(docValuesSupplier.get(), blockFactory);
                }
                try (IntBlock ordinals = currentReader.readOrdinalsAdded1(docs)) {
                    for (int p = 0; p < ordinals.getPositionCount(); p++) {
                        int start = ordinals.getFirstValueIndex(p);
                        int end = start + ordinals.getValueCount(p);
                        for (int i = start; i < end; i++) {
                            long ord = ordinals.getInt(i);
                            visitedOrds.set(ord);
                        }
                    }
                    for (GroupingAggregatorFunction.AddInput addInput : prepared) {
                        addInput.add(0, ordinals);
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                page.releaseBlocks();
            }
        }

        AggregatedResultIterator getResultIterator() throws IOException {
            return new AggregatedResultIterator(aggregators, visitedOrds, docValuesSupplier.get());
        }

        boolean seenNulls() {
            return visitedOrds.get(0);
        }

        @Override
        public BitArray seenGroupIds(BigArrays bigArrays) {
            final BitArray seen = new BitArray(0, bigArrays);
            boolean success = false;
            try {
                // the or method can grow the `seen` bits
                seen.or(visitedOrds);
                success = true;
                return seen;
            } finally {
                if (success == false) {
                    Releasables.close(seen);
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(visitedOrds, () -> Releasables.close(aggregators));
        }
    }

    private static class AggregatedResultIterator {
        private BytesRef currentTerm;
        private long currentOrd = 0;
        private final List<GroupingAggregator> aggregators;
        private final BitArray ords;
        private final SortedSetDocValues dv;

        AggregatedResultIterator(List<GroupingAggregator> aggregators, BitArray ords, SortedSetDocValues dv) {
            this.aggregators = aggregators;
            this.ords = ords;
            this.dv = dv;
        }

        int currentPosition() {
            assert currentOrd != Long.MAX_VALUE : "Must not read position when iterator is exhausted";
            return Math.toIntExact(currentOrd);
        }

        boolean next() throws IOException {
            currentOrd = ords.nextSetBit(currentOrd + 1);
            assert currentOrd > 0 : currentOrd;
            if (currentOrd < Long.MAX_VALUE) {
                currentTerm = dv.lookupOrd(currentOrd - 1);
                return true;
            } else {
                currentTerm = null;
                return false;
            }
        }
    }

    private static class ValuesAggregator implements Releasable {
        private final ValuesSourceReaderOperator extractor;
        private final HashAggregationOperator aggregator;

        ValuesAggregator(
            List<BlockLoader> blockLoaders,
            List<ValuesSourceReaderOperator.ShardContext> shardContexts,
            ElementType groupingElementType,
            int docChannel,
            String groupingField,
            int channelIndex,
            List<GroupingAggregator.Factory> aggregatorFactories,
            int maxPageSize,
            DriverContext driverContext
        ) {
            this.extractor = new ValuesSourceReaderOperator(
                BlockFactory.getNonBreakingInstance(),
                List.of(new ValuesSourceReaderOperator.FieldInfo(groupingField, blockLoaders)),
                shardContexts,
                docChannel
            );
            this.aggregator = new HashAggregationOperator(
                aggregatorFactories,
                () -> BlockHash.build(List.of(new GroupSpec(channelIndex, groupingElementType)), driverContext, maxPageSize, false),
                driverContext
            );
        }

        void addInput(Page page) {
            extractor.addInput(page);
            Page out = extractor.getOutput();
            if (out != null) {
                aggregator.addInput(out);
            }
        }

        void finish() {
            aggregator.finish();
        }

        Page getOutput() {
            return aggregator.getOutput();
        }

        @Override
        public void close() {
            Releasables.close(extractor, aggregator);
        }
    }

    static final class BlockOrdinalsReader {
        private final SortedSetDocValues sortedSetDocValues;
        private final Thread creationThread;
        private final BlockFactory blockFactory;

        BlockOrdinalsReader(SortedSetDocValues sortedSetDocValues, BlockFactory blockFactory) {
            this.sortedSetDocValues = sortedSetDocValues;
            this.blockFactory = blockFactory;
            this.creationThread = Thread.currentThread();
        }

        IntBlock readOrdinalsAdded1(IntVector docs) throws IOException {
            final int positionCount = docs.getPositionCount();
            try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    int doc = docs.getInt(p);
                    if (false == sortedSetDocValues.advanceExact(doc)) {
                        builder.appendInt(0);
                        continue;
                    }
                    int count = sortedSetDocValues.docValueCount();
                    if (count == 1) {
                        builder.appendInt(Math.toIntExact(sortedSetDocValues.nextOrd() + 1));
                        continue;
                    }
                    builder.beginPositionEntry();
                    for (int i = 0; i < count; i++) {
                        builder.appendInt(Math.toIntExact(sortedSetDocValues.nextOrd() + 1));
                    }
                    builder.endPositionEntry();
                }
                return builder.build();
            }
        }

        int docID() {
            return sortedSetDocValues.docID();
        }

        /**
         * Checks if the reader can be used to read a range documents starting with the given docID by the current thread.
         */
        static boolean canReuse(BlockOrdinalsReader reader, int startingDocID) {
            return reader != null && reader.creationThread == Thread.currentThread() && reader.docID() <= startingDocID;
        }
    }
}
