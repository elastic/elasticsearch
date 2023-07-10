/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregator.Factory;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.BlockOrdinalsReader;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.ValueSourceInfo;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.HashAggregationOperator.GroupSpec;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Unlike {@link HashAggregationOperator}, this hash operator also extracts values or ordinals of the input documents.
 */
@Experimental
public class OrdinalsGroupingOperator implements Operator {
    public record OrdinalsGroupingOperatorFactory(
        List<ValueSourceInfo> sources,
        int docChannel,
        String groupingField,
        List<Factory> aggregators,
        BigArrays bigArrays
    ) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new OrdinalsGroupingOperator(sources, docChannel, groupingField, aggregators, bigArrays, driverContext);
        }

        @Override
        public String describe() {
            return "OrdinalsGroupingOperator(aggs = " + aggregators.stream().map(Describable::describe).collect(joining(", ")) + ")";
        }
    }

    private final List<ValueSourceInfo> sources;
    private final int docChannel;
    private final String groupingField;

    private final List<Factory> aggregatorFactories;
    private final Map<SegmentID, OrdinalSegmentAggregator> ordinalAggregators;
    private final BigArrays bigArrays;

    private final DriverContext driverContext;

    private boolean finished = false;

    // used to extract and aggregate values
    private ValuesAggregator valuesAggregator;

    public OrdinalsGroupingOperator(
        List<ValueSourceInfo> sources,
        int docChannel,
        String groupingField,
        List<GroupingAggregator.Factory> aggregatorFactories,
        BigArrays bigArrays,
        DriverContext driverContext
    ) {
        Objects.requireNonNull(aggregatorFactories);
        boolean bytesValues = sources.get(0).source() instanceof ValuesSource.Bytes;
        for (int i = 1; i < sources.size(); i++) {
            if (sources.get(i).source() instanceof ValuesSource.Bytes != bytesValues) {
                throw new IllegalStateException("ValuesSources are mismatched");
            }
        }
        this.sources = sources;
        this.docChannel = docChannel;
        this.groupingField = groupingField;
        this.aggregatorFactories = aggregatorFactories;
        this.ordinalAggregators = new HashMap<>();
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
        if (docVector.getPositionCount() == 0) {
            return;
        }
        final int shardIndex = docVector.shards().getInt(0);
        final var source = sources.get(shardIndex);
        if (docVector.singleSegmentNonDecreasing() && source.source() instanceof ValuesSource.Bytes.WithOrdinals withOrdinals) {
            final IntVector segmentIndexVector = docVector.segments();
            assert segmentIndexVector.isConstant();
            final OrdinalSegmentAggregator ordinalAggregator = this.ordinalAggregators.computeIfAbsent(
                new SegmentID(shardIndex, segmentIndexVector.getInt(0)),
                k -> {
                    final List<GroupingAggregator> groupingAggregators = createGroupingAggregators();
                    boolean success = false;
                    try {
                        final LeafReaderContext leafReaderContext = source.reader().leaves().get(k.segmentIndex);
                        final OrdinalSegmentAggregator ordinalSegmentAggregator = new OrdinalSegmentAggregator(
                            groupingAggregators,
                            withOrdinals,
                            leafReaderContext,
                            bigArrays
                        );
                        success = true;
                        return ordinalSegmentAggregator;
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    } finally {
                        if (success == false) {
                            Releasables.close(groupingAggregators);
                        }
                    }
                }
            );
            ordinalAggregator.addInput(docVector.docs(), page);
        } else {
            if (valuesAggregator == null) {
                int channelIndex = page.getBlockCount(); // extractor will append a new block at the end
                valuesAggregator = new ValuesAggregator(
                    sources,
                    docChannel,
                    groupingField,
                    channelIndex,
                    aggregatorFactories,
                    bigArrays,
                    driverContext
                );
            }
            valuesAggregator.addInput(page);
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
            for (OrdinalSegmentAggregator agg : ordinalAggregators.values()) {
                final AggregatedResultIterator it = agg.getResultIterator();
                if (it.next()) {
                    pq.add(it);
                }
            }
            int position = -1;
            final BytesRefBuilder lastTerm = new BytesRefBuilder();
            var blockBuilder = BytesRefBlock.newBlockBuilder(1);
            while (pq.size() > 0) {
                final AggregatedResultIterator top = pq.top();
                if (position == -1 || lastTerm.get().equals(top.currentTerm) == false) {
                    position++;
                    lastTerm.copyBytes(top.currentTerm);
                    blockBuilder.appendBytesRef(top.currentTerm);
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
            int[] aggBlockCounts = aggregators.stream().mapToInt(GroupingAggregator::evaluateBlockCount).toArray();
            Block[] blocks = new Block[1 + Arrays.stream(aggBlockCounts).sum()];
            blocks[0] = blockBuilder.build();
            IntVector selected = IntVector.range(0, blocks[0].getPositionCount());
            int offset = 1;
            for (int i = 0; i < aggregators.size(); i++) {
                aggregators.get(i).evaluate(blocks, offset, selected);
                offset += aggBlockCounts[i];
            }
            return new Page(blocks);
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

    static final class OrdinalSegmentAggregator implements Releasable {
        private final List<GroupingAggregator> aggregators;
        private final ValuesSource.Bytes.WithOrdinals withOrdinals;
        private final LeafReaderContext leafReaderContext;
        private final BitArray visitedOrds;
        private BlockOrdinalsReader currentReader;

        OrdinalSegmentAggregator(
            List<GroupingAggregator> aggregators,
            ValuesSource.Bytes.WithOrdinals withOrdinals,
            LeafReaderContext leafReaderContext,
            BigArrays bigArrays
        ) throws IOException {
            this.aggregators = aggregators;
            this.withOrdinals = withOrdinals;
            this.leafReaderContext = leafReaderContext;
            final SortedSetDocValues sortedSetDocValues = withOrdinals.ordinalsValues(leafReaderContext);
            this.currentReader = new BlockOrdinalsReader(sortedSetDocValues);
            this.visitedOrds = new BitArray(sortedSetDocValues.getValueCount(), bigArrays);
        }

        void addInput(IntVector docs, Page page) {
            try {
                if (BlockOrdinalsReader.canReuse(currentReader, docs.getInt(0)) == false) {
                    currentReader = new BlockOrdinalsReader(withOrdinals.ordinalsValues(leafReaderContext));
                }
                final LongBlock ordinals = currentReader.readOrdinals(docs);
                for (int p = 0; p < ordinals.getPositionCount(); p++) {
                    if (ordinals.isNull(p)) {
                        continue;
                    }
                    int start = ordinals.getFirstValueIndex(p);
                    int end = start + ordinals.getValueCount(p);
                    for (int i = start; i < end; i++) {
                        long ord = ordinals.getLong(i);
                        visitedOrds.set(ord);
                    }
                }
                for (GroupingAggregator aggregator : aggregators) {
                    aggregator.prepareProcessPage(page).add(0, ordinals);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        AggregatedResultIterator getResultIterator() throws IOException {
            return new AggregatedResultIterator(aggregators, visitedOrds, withOrdinals.ordinalsValues(leafReaderContext));
        }

        @Override
        public void close() {
            Releasables.close(visitedOrds, () -> Releasables.close(aggregators));
        }
    }

    private static class AggregatedResultIterator {
        private BytesRef currentTerm;
        private long currentOrd = -1;
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
            if (currentOrd < Long.MAX_VALUE) {
                currentTerm = dv.lookupOrd(currentOrd);
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
            List<ValueSourceInfo> sources,
            int docChannel,
            String groupingField,
            int channelIndex,
            List<GroupingAggregator.Factory> aggregatorFactories,
            BigArrays bigArrays,
            DriverContext driverContext
        ) {
            this.extractor = new ValuesSourceReaderOperator(sources, docChannel, groupingField);
            this.aggregator = new HashAggregationOperator(
                aggregatorFactories,
                () -> BlockHash.build(
                    List.of(new GroupSpec(channelIndex, sources.get(0).elementType())),
                    bigArrays,
                    LuceneSourceOperator.PAGE_SIZE
                ),
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
}
