/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregator.Factory;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash.GroupSpec;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Unlike {@link HashAggregationOperator}, this hash operator also extracts values or ordinals of the input documents.
 */
public class OrdinalsGroupingOperator implements Operator {
    public record OrdinalsGroupingOperatorFactory(
        IntFunction<BlockLoader> blockLoaders,
        List<ValuesSourceReaderOperator.ShardContext> shardContexts,
        ElementType groupingElementType,
        int docChannel,
        String groupingField,
        List<Factory> aggregators,
        int maxPageSize
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
                driverContext
            );
        }

        @Override
        public String describe() {
            return "OrdinalsGroupingOperator(aggs = " + aggregators.stream().map(Describable::describe).collect(joining(", ")) + ")";
        }
    }

    private final IntFunction<BlockLoader> blockLoaders;
    private final List<ValuesSourceReaderOperator.ShardContext> shardContexts;
    private final int docChannel;
    private final String groupingField;

    private final List<Factory> aggregatorFactories;
    private final ElementType groupingElementType;
    private final Map<SegmentID, OrdinalSegmentAggregator> ordinalAggregators;

    private final DriverContext driverContext;

    private boolean finished = false;

    /**
     * Nanoseconds this operator has spent processing the rows.
     */
    private long totalProcessNanos;
    private long ordinalsProcessNanos;
    private long valuesProcessNanos;
    /**
     * Count of pages this operator has processed.
     */
    private int pagesProcessed;
    /**
     * Count of rows this operator has received.
     */
    private long rowsReceived;
    /**
     * Count of rows this operator has emitted.
     */
    private long rowsEmitted;

    /**
     * Nanoseconds this operator has spent emitting the results.
     */
    private long totalEmitNanos;
    private long ordinalsEmitNanos;
    private long valuesEmitNanos;

    // used to extract and aggregate values
    private final int maxPageSize;
    private ValuesAggregator valuesAggregator;

    public OrdinalsGroupingOperator(
        IntFunction<BlockLoader> blockLoaders,
        List<ValuesSourceReaderOperator.ShardContext> shardContexts,
        ElementType groupingElementType,
        int docChannel,
        String groupingField,
        List<GroupingAggregator.Factory> aggregatorFactories,
        int maxPageSize,
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
        long startNanos = System.nanoTime();
        DocVector docVector = page.<DocBlock>getBlock(docChannel).asVector();
        final int shardIndex = docVector.shards().getInt(0);
        RefCounted shardRefCounter = docVector.shardRefCounted().get(shardIndex);
        final var blockLoader = blockLoaders.apply(shardIndex);
        boolean pagePassed = false;
        try {
            if (docVector.singleSegmentNonDecreasing() && blockLoader.supportsOrdinals()) {
                long ordinalsStartNanos = System.nanoTime();
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
                                driverContext.bigArrays(),
                                shardRefCounter
                            );
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                );
                pagePassed = true;
                ordinalAggregator.addInput(docVector.docs(), page);
                ordinalsProcessNanos += System.nanoTime() - ordinalsStartNanos;
            } else {
                long valuesStartNanos = System.nanoTime();
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
                valuesProcessNanos += System.nanoTime() - valuesStartNanos;
            }
        } finally {
            if (pagePassed == false) {
                Releasables.closeExpectNoException(page::releaseBlocks);
            }
            pagesProcessed++;
            rowsReceived += page.getPositionCount();
            totalProcessNanos += System.nanoTime() - startNanos;
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
        long startNanos = System.nanoTime();
        Page page = null;
        if (valuesAggregator != null) {
            try {
                page = valuesAggregator.getOutput();
            } finally {
                final ValuesAggregator aggregator = this.valuesAggregator;
                this.valuesAggregator = null;
                Releasables.close(aggregator);
                valuesEmitNanos += System.nanoTime() - startNanos;
            }
        } else if (ordinalAggregators.isEmpty() == false) {
            try {
                page = mergeOrdinalsSegmentResults();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                Releasables.close(() -> Releasables.close(ordinalAggregators.values()), ordinalAggregators::clear);
                ordinalsEmitNanos += System.nanoTime() - startNanos;
            }
        }
        if (page != null) {
            rowsEmitted += page.getPositionCount();
        }
        totalEmitNanos += System.nanoTime() - startNanos;
        return page;
    }

    @Override
    public void finish() {
        finished = true;
        if (valuesAggregator != null) {
            long startNanos = System.nanoTime();
            valuesAggregator.finish();
            valuesEmitNanos += System.nanoTime() - startNanos;
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
                        aggregators.get(i).evaluate(blocks, offset, selected, new GroupingAggregatorEvaluationContext(driverContext));
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

    @Override
    public Operator.Status status() {
        return new Status(
            totalProcessNanos,
            ordinalsProcessNanos,
            valuesProcessNanos,
            totalEmitNanos,
            ordinalsEmitNanos,
            valuesEmitNanos,
            pagesProcessed,
            rowsReceived,
            rowsEmitted
        );
    }

    private static void checkState(boolean condition, String msg) {
        if (condition == false) {
            throw new IllegalArgumentException(msg);
        }
    }

    @Override
    public String toString() {
        String aggregatorDescriptions = aggregatorFactories.stream()
            .map(factory -> "\"" + factory.describe() + "\"")
            .collect(Collectors.joining(", "));

        return this.getClass().getSimpleName() + "[" + "aggregators=[" + aggregatorDescriptions + "]]";
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "ordinals_grouping",
            Status::new
        );

        private final long totalProcessNanos;
        private final long ordinalsProcessNanos;
        private final long valuesProcessNanos;
        private final long totalEmitNanos;
        private final long ordinalsEmitNanos;
        private final long valuesEmitNanos;
        private final int pagesProcessed;
        private final long rowsReceived;
        private final long rowsEmitted;

        /**
         * Build.
         * @param totalProcessNanos Nanoseconds this operator has spent processing the rows.
         * @param pagesProcessed Count of pages this operator has processed.
         * @param rowsReceived Count of rows this operator has received.
         * @param rowsEmitted Count of rows this operator has emitted.
         */
        public Status(
            long totalProcessNanos,
            long ordinalsProcessNanos,
            long valuesProcessNanos,
            long totalEmitNanos,
            long ordinalsEmitNanos,
            long valuesEmitNanos,
            int pagesProcessed,
            long rowsReceived,
            long rowsEmitted
        ) {
            this.totalProcessNanos = totalProcessNanos;
            this.ordinalsProcessNanos = ordinalsProcessNanos;
            this.valuesProcessNanos = valuesProcessNanos;

            this.totalEmitNanos = totalEmitNanos;
            this.ordinalsEmitNanos = ordinalsEmitNanos;
            this.valuesEmitNanos = valuesEmitNanos;

            this.pagesProcessed = pagesProcessed;
            this.rowsReceived = rowsReceived;
            this.rowsEmitted = rowsEmitted;
        }

        protected Status(StreamInput in) throws IOException {
            totalProcessNanos = in.readVLong();
            ordinalsProcessNanos = in.readVLong();
            valuesProcessNanos = in.readVLong();

            totalEmitNanos = in.readVLong();
            ordinalsEmitNanos = in.readVLong();
            valuesEmitNanos = in.readVLong();

            pagesProcessed = in.readVInt();
            rowsReceived = in.readVLong();
            rowsEmitted = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(totalProcessNanos);
            out.writeVLong(ordinalsProcessNanos);
            out.writeVLong(valuesProcessNanos);

            out.writeVLong(totalEmitNanos);
            out.writeVLong(ordinalsEmitNanos);
            out.writeVLong(valuesEmitNanos);

            out.writeVInt(pagesProcessed);
            out.writeVLong(rowsReceived);
            out.writeVLong(rowsEmitted);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public long totalProcessNanos() {
            return totalProcessNanos;
        }

        public long ordinalsProcessNanos() {
            return ordinalsProcessNanos;
        }

        public long valuesProcessNanos() {
            return valuesProcessNanos;
        }

        public long totalEmitNanos() {
            return totalEmitNanos;
        }

        public long ordinalsEmitNanos() {
            return ordinalsEmitNanos;
        }

        public long valuesEmitNanos() {
            return valuesEmitNanos;
        }

        public int pagesProcessed() {
            return pagesProcessed;
        }

        public long rowsReceived() {
            return rowsReceived;
        }

        public long rowsEmitted() {
            return rowsEmitted;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            // Process nanos
            builder.field("total_process_nanos", totalProcessNanos);
            if (builder.humanReadable()) {
                builder.field("total_process_time", TimeValue.timeValueNanos(totalProcessNanos));
            }
            builder.field("ordinals_process_nanos", ordinalsProcessNanos);
            if (builder.humanReadable()) {
                builder.field("ordinals_process_time", TimeValue.timeValueNanos(ordinalsProcessNanos));
            }
            builder.field("values_process_nanos", valuesProcessNanos);
            if (builder.humanReadable()) {
                builder.field("values_process_time", TimeValue.timeValueNanos(valuesProcessNanos));
            }

            // Emit nanos
            builder.field("total_emit_nanos", totalEmitNanos);
            if (builder.humanReadable()) {
                builder.field("total_emit_time", TimeValue.timeValueNanos(totalEmitNanos));
            }
            builder.field("ordinals_emit_nanos", ordinalsEmitNanos);
            if (builder.humanReadable()) {
                builder.field("ordinals_emit_time", TimeValue.timeValueNanos(ordinalsEmitNanos));
            }
            builder.field("values_emit_nanos", valuesEmitNanos);
            if (builder.humanReadable()) {
                builder.field("values_emit_time", TimeValue.timeValueNanos(valuesEmitNanos));
            }

            // Page/row counts
            builder.field("pages_processed", pagesProcessed);
            builder.field("rows_received", rowsReceived);
            builder.field("rows_emitted", rowsEmitted);
            return builder.endObject();

        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return totalProcessNanos == status.totalProcessNanos
                && ordinalsProcessNanos == status.ordinalsProcessNanos
                && valuesProcessNanos == status.valuesProcessNanos
                && totalEmitNanos == status.totalEmitNanos
                && ordinalsEmitNanos == status.ordinalsEmitNanos
                && valuesEmitNanos == status.valuesEmitNanos
                && pagesProcessed == status.pagesProcessed
                && rowsReceived == status.rowsReceived
                && rowsEmitted == status.rowsEmitted;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                totalProcessNanos,
                ordinalsProcessNanos,
                valuesProcessNanos,
                totalEmitNanos,
                ordinalsEmitNanos,
                valuesEmitNanos,
                pagesProcessed,
                rowsReceived,
                rowsEmitted
            );
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            assert false : "should never be called when supportsVersion is used";
            return TransportVersions.ESQL_ORDINALS_OPERATOR_STATUS;
        }

        @Override
        public boolean supportsVersion(TransportVersion version) {
            return version.onOrAfter(TransportVersions.ESQL_ORDINALS_OPERATOR_STATUS)
                || version.isPatchFrom(TransportVersions.ESQL_ORDINALS_OPERATOR_STATUS_9_1)
                || version.isPatchFrom(TransportVersions.ESQL_ORDINALS_OPERATOR_STATUS_8_19);
        }
    }

    record SegmentID(int shardIndex, int segmentIndex) {

    }

    static final class OrdinalSegmentAggregator implements Releasable, SeenGroupIds {
        private final BlockFactory blockFactory;
        private final List<GroupingAggregator> aggregators;
        private final CheckedSupplier<SortedSetDocValues, IOException> docValuesSupplier;
        private final BitArray visitedOrds;
        private final RefCounted shardRefCounted;
        private BlockOrdinalsReader currentReader;

        OrdinalSegmentAggregator(
            BlockFactory blockFactory,
            Supplier<List<GroupingAggregator>> aggregatorsSupplier,
            CheckedSupplier<SortedSetDocValues, IOException> docValuesSupplier,
            BigArrays bigArrays,
            RefCounted shardRefCounted
        ) throws IOException {
            boolean success = false;
            this.shardRefCounted = shardRefCounted;
            this.shardRefCounted.mustIncRef();
            List<GroupingAggregator> groupingAggregators = null;
            BitArray bitArray = null;
            try {
                final SortedSetDocValues sortedSetDocValues = docValuesSupplier.get();
                bitArray = new BitArray(sortedSetDocValues.getValueCount(), bigArrays);
                groupingAggregators = aggregatorsSupplier.get();
                this.currentReader = BlockOrdinalsReader.newReader(blockFactory, sortedSetDocValues);
                this.blockFactory = blockFactory;
                this.docValuesSupplier = docValuesSupplier;
                this.aggregators = groupingAggregators;
                this.visitedOrds = bitArray;
                success = true;
            } finally {
                if (success == false) {
                    if (bitArray != null) Releasables.close(bitArray);
                    if (groupingAggregators != null) Releasables.close(groupingAggregators);
                    // There is no danger of double decRef here, since this decRef is called only if the constructor throws, so it would be
                    // impossible to call close on the instance.
                    shardRefCounted.decRef();
                }
            }
        }

        void addInput(IntVector docs, Page page) {
            GroupingAggregatorFunction.AddInput[] prepared = new GroupingAggregatorFunction.AddInput[aggregators.size()];
            try {
                for (int i = 0; i < prepared.length; i++) {
                    prepared[i] = aggregators.get(i).prepareProcessPage(this, page);
                }

                if (BlockOrdinalsReader.canReuse(currentReader, docs.getInt(0)) == false) {
                    currentReader = BlockOrdinalsReader.newReader(blockFactory, docValuesSupplier.get());
                }
                try (IntBlock ordinals = currentReader.readOrdinalsAdded1(docs)) {
                    final IntVector ordinalsVector = ordinals.asVector();
                    if (ordinalsVector != null) {
                        addOrdinalsInput(ordinalsVector, prepared);
                    } else {
                        addOrdinalsInput(ordinals, prepared);
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                Releasables.close(page::releaseBlocks, Releasables.wrap(prepared));
            }
        }

        void addOrdinalsInput(IntBlock ordinals, GroupingAggregatorFunction.AddInput[] prepared) {
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

        void addOrdinalsInput(IntVector ordinals, GroupingAggregatorFunction.AddInput[] prepared) {
            for (int p = 0; p < ordinals.getPositionCount(); p++) {
                long ord = ordinals.getInt(p);
                visitedOrds.set(ord);
            }
            for (GroupingAggregatorFunction.AddInput addInput : prepared) {
                addInput.add(0, ordinals);
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
            Releasables.close(visitedOrds, () -> Releasables.close(aggregators), Releasables.fromRefCounted(shardRefCounted));
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
            IntFunction<BlockLoader> blockLoaders,
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
                driverContext.blockFactory(),
                List.of(new ValuesSourceReaderOperator.FieldInfo(groupingField, groupingElementType, blockLoaders)),
                shardContexts,
                docChannel
            );
            this.aggregator = new HashAggregationOperator(
                aggregatorFactories,
                () -> BlockHash.build(
                    List.of(new GroupSpec(channelIndex, groupingElementType)),
                    driverContext.blockFactory(),
                    maxPageSize,
                    false
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

    abstract static class BlockOrdinalsReader {
        protected final Thread creationThread;
        protected final BlockFactory blockFactory;

        BlockOrdinalsReader(BlockFactory blockFactory) {
            this.blockFactory = blockFactory;
            this.creationThread = Thread.currentThread();
        }

        static BlockOrdinalsReader newReader(BlockFactory blockFactory, SortedSetDocValues sortedSetDocValues) {
            SortedDocValues singleValues = DocValues.unwrapSingleton(sortedSetDocValues);
            if (singleValues != null) {
                return new SortedDocValuesBlockOrdinalsReader(blockFactory, singleValues);
            } else {
                return new SortedSetDocValuesBlockOrdinalsReader(blockFactory, sortedSetDocValues);
            }
        }

        abstract IntBlock readOrdinalsAdded1(IntVector docs) throws IOException;

        abstract int docID();

        /**
         * Checks if the reader can be used to read a range documents starting with the given docID by the current thread.
         */
        static boolean canReuse(BlockOrdinalsReader reader, int startingDocID) {
            return reader != null && reader.creationThread == Thread.currentThread() && reader.docID() <= startingDocID;
        }
    }

    private static class SortedSetDocValuesBlockOrdinalsReader extends BlockOrdinalsReader {
        private final SortedSetDocValues sortedSetDocValues;

        SortedSetDocValuesBlockOrdinalsReader(BlockFactory blockFactory, SortedSetDocValues sortedSetDocValues) {
            super(blockFactory);
            this.sortedSetDocValues = sortedSetDocValues;
        }

        @Override
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

        @Override
        int docID() {
            return sortedSetDocValues.docID();
        }
    }

    private static class SortedDocValuesBlockOrdinalsReader extends BlockOrdinalsReader {
        private final SortedDocValues sortedDocValues;

        SortedDocValuesBlockOrdinalsReader(BlockFactory blockFactory, SortedDocValues sortedDocValues) {
            super(blockFactory);
            this.sortedDocValues = sortedDocValues;
        }

        @Override
        IntBlock readOrdinalsAdded1(IntVector docs) throws IOException {
            final int positionCount = docs.getPositionCount();
            try (IntVector.FixedBuilder builder = blockFactory.newIntVectorFixedBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    if (sortedDocValues.advanceExact(docs.getInt(p))) {
                        builder.appendInt(p, sortedDocValues.ordValue() + 1);
                    } else {
                        builder.appendInt(p, 0);
                    }
                }
                return builder.build().asBlock();
            }
        }

        @Override
        int docID() {
            return sortedDocValues.docID();
        }
    }
}
