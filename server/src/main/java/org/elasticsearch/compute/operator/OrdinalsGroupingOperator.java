/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.aggregation.BlockHash;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregator.GroupingAggregatorFactory;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefArrayBlock;
import org.elasticsearch.compute.data.ConstantIntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.BlockOrdinalsReader;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
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
    private boolean finished = false;
    private final String fieldName;
    private final int shardIndexChannel;
    private final int segmentIndexChannel;
    private final int docIDChannel;
    private final List<ValuesSource> valuesSources;
    private final List<ValuesSourceType> valuesSourceTypes;
    private final List<IndexReader> indexReaders;

    private final List<GroupingAggregatorFactory> aggregatorFactories;
    private final Map<SegmentID, OrdinalSegmentAggregator> ordinalAggregators;
    private final BigArrays bigArrays;

    // used to extract and aggregate values
    private ValuesAggregator valuesAggregator;

    public record OrdinalsGroupingOperatorFactory(
        String fieldName,
        int shardIndexChannel,
        int segmentIndexChannel,
        int docIDChannel,
        List<SearchContext> searchContexts,
        List<GroupingAggregatorFactory> aggregators,
        BigArrays bigArrays
    ) implements OperatorFactory {

        @Override
        public Operator get() {
            List<ValuesSource> valuesSources = new ArrayList<>(searchContexts.size());
            List<ValuesSourceType> valuesSourceTypes = new ArrayList<>(searchContexts.size());
            List<IndexReader> indexReaders = new ArrayList<>(searchContexts.size());
            for (SearchContext searchContext : searchContexts) {
                SearchExecutionContext ctx = searchContext.getSearchExecutionContext();
                MappedFieldType fieldType = ctx.getFieldType(fieldName);
                IndexFieldData<?> fieldData = ctx.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
                FieldContext fieldContext = new FieldContext(fieldName, fieldData, fieldType);
                ValuesSourceType vsType = fieldData.getValuesSourceType();
                valuesSourceTypes.add(vsType);
                ValuesSource vs = vsType.getField(fieldContext, null);
                valuesSources.add(vs);
                indexReaders.add(ctx.getIndexReader());
            }
            return new OrdinalsGroupingOperator(
                fieldName,
                shardIndexChannel,
                segmentIndexChannel,
                docIDChannel,
                valuesSources,
                valuesSourceTypes,
                indexReaders,
                aggregators,
                bigArrays
            );
        }

        @Override
        public String describe() {
            return "HashAggregationSourceOperator(aggs = " + aggregators.stream().map(Describable::describe).collect(joining(", ")) + ")";
        }
    }

    public OrdinalsGroupingOperator(
        String fieldName,
        int shardIndexChannel,
        int segmentIndexChannel,
        int docIDChannel,
        List<ValuesSource> valuesSources,
        List<ValuesSourceType> valuesSourceTypes,
        List<IndexReader> indexReaders,
        List<GroupingAggregatorFactory> aggregatorFactories,
        BigArrays bigArrays
    ) {
        Objects.requireNonNull(aggregatorFactories);
        boolean bytesValues = valuesSources.get(0) instanceof ValuesSource.Bytes;
        for (int i = 1; i < valuesSources.size(); i++) {
            if (valuesSources.get(i) instanceof ValuesSource.Bytes != bytesValues) {
                throw new IllegalStateException("ValuesSources are mismatched");
            }
        }
        this.fieldName = fieldName;
        this.shardIndexChannel = shardIndexChannel;
        this.segmentIndexChannel = segmentIndexChannel;
        this.docIDChannel = docIDChannel;
        this.valuesSources = valuesSources;
        this.valuesSourceTypes = valuesSourceTypes;
        this.indexReaders = indexReaders;
        this.aggregatorFactories = aggregatorFactories;
        this.ordinalAggregators = new HashMap<>();
        this.bigArrays = bigArrays;
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
        checkState(needsInput(), "Operator is already finishing");
        requireNonNull(page, "page is null");
        Block docs = page.getBlock(docIDChannel);
        if (docs.getPositionCount() == 0) {
            return;
        }
        final ConstantIntBlock shardIndexBlock = (ConstantIntBlock) page.getBlock(shardIndexChannel);
        final int shardIndex = shardIndexBlock.getInt(0);
        if (valuesSources.get(shardIndex)instanceof ValuesSource.Bytes.WithOrdinals withOrdinals) {
            final ConstantIntBlock segmentIndexBlock = (ConstantIntBlock) page.getBlock(segmentIndexChannel);
            final OrdinalSegmentAggregator ordinalAggregator = this.ordinalAggregators.computeIfAbsent(
                new SegmentID(shardIndex, segmentIndexBlock.getInt(0)),
                k -> {
                    final List<GroupingAggregator> groupingAggregators = createGroupingAggregators();
                    boolean success = false;
                    try {
                        final LeafReaderContext leafReaderContext = indexReaders.get(shardIndex).leaves().get(k.segmentIndex);
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
            ordinalAggregator.addInput(docs, page);
        } else {
            if (valuesAggregator == null) {
                int channelIndex = page.getBlockCount(); // extractor will append a new block at the end
                valuesAggregator = new ValuesAggregator(
                    fieldName,
                    shardIndexChannel,
                    segmentIndexChannel,
                    docIDChannel,
                    channelIndex,
                    valuesSources,
                    valuesSourceTypes,
                    indexReaders,
                    aggregatorFactories,
                    bigArrays
                );
            }
            valuesAggregator.addInput(page);
        }
    }

    private List<GroupingAggregator> createGroupingAggregators() {
        boolean success = false;
        List<GroupingAggregator> aggregators = new ArrayList<>(aggregatorFactories.size());
        try {
            for (GroupingAggregatorFactory aggregatorFactory : aggregatorFactories) {
                aggregators.add(aggregatorFactory.get());
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
        BytesRefArray keys = null;
        try {
            for (OrdinalSegmentAggregator agg : ordinalAggregators.values()) {
                final AggregatedResultIterator it = agg.getResultIterator();
                if (it.next()) {
                    pq.add(it);
                }
            }
            int position = -1;
            final BytesRefBuilder lastTerm = new BytesRefBuilder();
            // Use NON_RECYCLING_INSTANCE as we don't have a lifecycle for pages/block yet
            keys = new BytesRefArray(1, BigArrays.NON_RECYCLING_INSTANCE);
            while (pq.size() > 0) {
                final AggregatedResultIterator top = pq.top();
                if (position == -1 || lastTerm.get().equals(top.currentTerm) == false) {
                    position++;
                    lastTerm.copyBytes(top.currentTerm);
                    keys.append(top.currentTerm);
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
            final Block[] blocks = new Block[aggregators.size() + 1];
            blocks[0] = new BytesRefArrayBlock(position + 1, keys);
            keys = null;
            for (int i = 0; i < aggregators.size(); i++) {
                blocks[i + 1] = aggregators.get(i).evaluate();
            }
            return new Page(blocks);
        } finally {
            Releasables.close(keys, () -> Releasables.close(aggregators));
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

        void addInput(Block docs, Page page) {
            try {
                if (BlockOrdinalsReader.canReuse(currentReader, docs.getInt(0)) == false) {
                    currentReader = new BlockOrdinalsReader(withOrdinals.ordinalsValues(leafReaderContext));
                }
                final Block ordinals = currentReader.readOrdinals(docs);
                for (int i = 0; i < ordinals.getPositionCount(); i++) {
                    long ord = ordinals.getLong(i);
                    visitedOrds.set(ord);
                }
                for (GroupingAggregator aggregator : aggregators) {
                    aggregator.processPage(ordinals, page);
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
            String fieldName,
            int shardIndexChannel,
            int segmentIndexChannel,
            int docIDChannel,
            int channelIndex,
            List<ValuesSource> valuesSources,
            List<ValuesSourceType> valuesSourceTypes,
            List<IndexReader> indexReaders,
            List<GroupingAggregatorFactory> aggregatorFactories,
            BigArrays bigArrays
        ) {
            this.extractor = new ValuesSourceReaderOperator(
                valuesSourceTypes,
                valuesSources,
                indexReaders,
                docIDChannel,
                segmentIndexChannel,
                shardIndexChannel,
                fieldName
            );
            boolean bytesValues = valuesSources.get(0) instanceof ValuesSource.Bytes;
            this.aggregator = new HashAggregationOperator(
                channelIndex,
                aggregatorFactories,
                bytesValues ? () -> BlockHash.newBytesRefHash(bigArrays) : () -> BlockHash.newLongHash(bigArrays)
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
