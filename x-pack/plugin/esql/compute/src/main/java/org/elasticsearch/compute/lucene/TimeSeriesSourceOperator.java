/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockLoaderStoredFieldsFromLeafLoader;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class TimeSeriesSourceOperator extends LuceneOperator {

    private final boolean emitDocIds;
    private final int maxPageSize;
    private final BlockFactory blockFactory;
    private final LuceneSliceQueue sliceQueue;
    private int currentPagePos = 0;
    private int remainingDocs;
    private boolean doneCollecting;

    private LongVector.Builder timestampsBuilder;
    private TsidBuilder tsHashesBuilder;
    private SegmentsIterator iterator;
    private final List<ValuesSourceReaderOperator.FieldInfo> fieldsToExtracts;
    private ShardLevelFieldsReader fieldsReader;
    private DocIdCollector docCollector;
    private long tsidsLoaded;

    TimeSeriesSourceOperator(
        BlockFactory blockFactory,
        boolean emitDocIds,
        List<ValuesSourceReaderOperator.FieldInfo> fieldsToExtract,
        LuceneSliceQueue sliceQueue,
        int maxPageSize,
        int limit
    ) {
        super(blockFactory, maxPageSize, sliceQueue);
        this.maxPageSize = maxPageSize;
        this.blockFactory = blockFactory;
        this.fieldsToExtracts = fieldsToExtract;
        this.emitDocIds = emitDocIds;
        this.remainingDocs = limit;
        this.timestampsBuilder = blockFactory.newLongVectorBuilder(Math.min(limit, maxPageSize));
        this.tsHashesBuilder = new TsidBuilder(blockFactory, Math.min(limit, maxPageSize));
        this.sliceQueue = sliceQueue;
    }

    @Override
    public void finish() {
        this.doneCollecting = true;
    }

    @Override
    public boolean isFinished() {
        return doneCollecting;
    }

    @Override
    public Page getCheckedOutput() throws IOException {
        if (isFinished()) {
            return null;
        }

        if (remainingDocs <= 0) {
            doneCollecting = true;
            return null;
        }

        Page page = null;
        Block[] blocks = new Block[(emitDocIds ? 3 : 2) + fieldsToExtracts.size()];
        long startInNanos = System.nanoTime();
        try {
            if (iterator == null) {
                var slice = sliceQueue.nextSlice();
                if (slice == null) {
                    doneCollecting = true;
                    return null;
                }
                if (slice.tags().isEmpty() == false) {
                    throw new UnsupportedOperationException("tags not supported by " + getClass());
                }
                Releasables.close(fieldsReader);
                fieldsReader = new ShardLevelFieldsReader(blockFactory, slice.shardContext(), fieldsToExtracts);
                iterator = new SegmentsIterator(slice);
                if (emitDocIds) {
                    docCollector = new DocIdCollector(blockFactory, slice.shardContext());
                }
            }
            if (docCollector != null) {
                docCollector.prepareForCollecting(Math.min(remainingDocs, maxPageSize));
            }
            fieldsReader.prepareForReading(Math.min(remainingDocs, maxPageSize));
            iterator.readDocsForNextPage();
            if (currentPagePos > 0) {
                int blockIndex = 0;
                if (docCollector != null) {
                    blocks[blockIndex++] = docCollector.build().asBlock();
                }
                OrdinalBytesRefVector tsidVector = tsHashesBuilder.build();
                blocks[blockIndex++] = tsidVector.asBlock();
                tsHashesBuilder = new TsidBuilder(blockFactory, Math.min(remainingDocs, maxPageSize));
                blocks[blockIndex++] = timestampsBuilder.build().asBlock();
                timestampsBuilder = blockFactory.newLongVectorBuilder(Math.min(remainingDocs, maxPageSize));
                System.arraycopy(fieldsReader.buildBlocks(tsidVector.getOrdinalsVector()), 0, blocks, blockIndex, fieldsToExtracts.size());
                page = new Page(currentPagePos, blocks);
                currentPagePos = 0;
            }
            if (iterator.completed()) {
                processedShards.add(iterator.luceneSlice.shardContext().shardIdentifier());
                processedSlices++;
                Releasables.close(docCollector, fieldsReader);
                iterator = null;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            if (page == null) {
                Releasables.closeExpectNoException(blocks);
            }
            processingNanos += System.nanoTime() - startInNanos;
        }
        return page;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(timestampsBuilder, tsHashesBuilder, docCollector, fieldsReader);
    }

    class SegmentsIterator {
        private final PriorityQueue<LeafIterator> mainQueue;
        private final PriorityQueue<LeafIterator> oneTsidQueue;
        final LuceneSlice luceneSlice;

        SegmentsIterator(LuceneSlice luceneSlice) throws IOException {
            this.luceneSlice = luceneSlice;
            this.mainQueue = new PriorityQueue<>(luceneSlice.numLeaves()) {
                @Override
                protected boolean lessThan(LeafIterator a, LeafIterator b) {
                    return a.timeSeriesHash.compareTo(b.timeSeriesHash) < 0;
                }
            };
            Weight weight = luceneSlice.weight();
            processedQueries.add(weight.getQuery());
            int maxSegmentOrd = 0;
            for (var leafReaderContext : luceneSlice.leaves()) {
                LeafIterator leafIterator = new LeafIterator(weight, leafReaderContext.leafReaderContext());
                leafIterator.nextDoc();
                if (leafIterator.docID != DocIdSetIterator.NO_MORE_DOCS) {
                    mainQueue.add(leafIterator);
                    maxSegmentOrd = Math.max(maxSegmentOrd, leafIterator.segmentOrd);
                }
            }
            this.oneTsidQueue = new PriorityQueue<>(mainQueue.size()) {
                @Override
                protected boolean lessThan(LeafIterator a, LeafIterator b) {
                    return a.timestamp > b.timestamp;
                }
            };
        }

        // TODO: add optimize for one leaf?
        void readDocsForNextPage() throws IOException {
            Thread executingThread = Thread.currentThread();
            for (LeafIterator leaf : mainQueue) {
                leaf.reinitializeIfNeeded(executingThread);
            }
            for (LeafIterator leaf : oneTsidQueue) {
                leaf.reinitializeIfNeeded(executingThread);
            }
            do {
                PriorityQueue<LeafIterator> sub = subQueueForNextTsid();
                if (sub.size() == 0) {
                    break;
                }
                tsHashesBuilder.appendNewTsid(sub.top().timeSeriesHash);
                if (readValuesForOneTsid(sub)) {
                    break;
                }
            } while (mainQueue.size() > 0);
        }

        private boolean readValuesForOneTsid(PriorityQueue<LeafIterator> sub) throws IOException {
            boolean first = true;
            do {
                LeafIterator top = sub.top();
                currentPagePos++;
                remainingDocs--;
                if (docCollector != null) {
                    docCollector.collect(top.segmentOrd, top.docID);
                }
                tsHashesBuilder.appendOrdinal();
                timestampsBuilder.appendLong(top.timestamp);
                fieldsReader.readValues(top.segmentOrd, top.docID, first == false);
                first = false;
                if (top.nextDoc()) {
                    sub.updateTop();
                } else if (top.docID == DocIdSetIterator.NO_MORE_DOCS) {
                    sub.pop();
                } else {
                    mainQueue.add(sub.pop());
                }
                if (remainingDocs <= 0 || currentPagePos >= maxPageSize) {
                    return true;
                }
            } while (sub.size() > 0);
            return false;
        }

        private PriorityQueue<LeafIterator> subQueueForNextTsid() {
            if (oneTsidQueue.size() == 0 && mainQueue.size() > 0) {
                LeafIterator last = mainQueue.pop();
                oneTsidQueue.add(last);
                while (mainQueue.size() > 0) {
                    var top = mainQueue.top();
                    if (top.timeSeriesHash.equals(last.timeSeriesHash)) {
                        oneTsidQueue.add(mainQueue.pop());
                    } else {
                        break;
                    }
                }
                if (oneTsidQueue.size() > 0) {
                    ++tsidsLoaded;
                }
            }
            return oneTsidQueue;
        }

        boolean completed() {
            return mainQueue.size() == 0 && oneTsidQueue.size() == 0;
        }
    }

    static class LeafIterator {
        private final int segmentOrd;
        private final Weight weight;
        private final LeafReaderContext leafContext;
        private SortedDocValues tsids;
        private NumericDocValues timestamps;
        private DocIdSetIterator disi;
        private Thread createdThread;

        private long timestamp;
        private int lastTsidOrd = -1;
        private BytesRef timeSeriesHash;
        private int docID = -1;

        LeafIterator(Weight weight, LeafReaderContext leafContext) throws IOException {
            this.segmentOrd = leafContext.ord;
            this.weight = weight;
            this.leafContext = leafContext;
            this.createdThread = Thread.currentThread();
            tsids = leafContext.reader().getSortedDocValues("_tsid");
            timestamps = DocValues.unwrapSingleton(leafContext.reader().getSortedNumericDocValues("@timestamp"));
            final Scorer scorer = weight.scorer(leafContext);
            disi = scorer != null ? scorer.iterator() : DocIdSetIterator.empty();
        }

        boolean nextDoc() throws IOException {
            docID = disi.nextDoc();
            if (docID == DocIdSetIterator.NO_MORE_DOCS) {
                return false;
            }
            boolean advanced = timestamps.advanceExact(docID);
            assert advanced;
            timestamp = timestamps.longValue();
            advanced = tsids.advanceExact(docID);
            assert advanced;

            int ord = tsids.ordValue();
            if (ord != lastTsidOrd) {
                timeSeriesHash = tsids.lookupOrd(ord);
                lastTsidOrd = ord;
                return false;
            } else {
                return true;
            }
        }

        void reinitializeIfNeeded(Thread executingThread) throws IOException {
            if (executingThread != createdThread) {
                tsids = leafContext.reader().getSortedDocValues("_tsid");
                timestamps = DocValues.unwrapSingleton(leafContext.reader().getSortedNumericDocValues("@timestamp"));
                final Scorer scorer = weight.scorer(leafContext);
                disi = scorer != null ? scorer.iterator() : DocIdSetIterator.empty();
                if (docID != -1) {
                    disi.advance(docID);
                }
                createdThread = executingThread;
            }
        }
    }

    static class BlockLoaderFactory extends ValuesSourceReaderOperator.DelegatingBlockLoaderFactory {
        BlockLoaderFactory(BlockFactory factory) {
            super(factory);
        }

        @Override
        public BlockLoader.Block constantNulls() {
            throw new UnsupportedOperationException("must not be used by column readers");
        }

        @Override
        public BlockLoader.Block constantBytes(BytesRef value) {
            throw new UnsupportedOperationException("must not be used by column readers");
        }

        @Override
        public BlockLoader.SingletonOrdinalsBuilder singletonOrdinalsBuilder(SortedDocValues ordinals, int count) {
            throw new UnsupportedOperationException("must not be used by column readers");
        }
    }

    static final class ShardLevelFieldsReader implements Releasable {
        private final BlockLoaderFactory blockFactory;
        private final SegmentLevelFieldsReader[] segments;
        private final BlockLoader[] loaders;
        private final boolean[] dimensions;
        private final Block.Builder[] builders;
        private final StoredFieldsSpec storedFieldsSpec;
        private final SourceLoader sourceLoader;

        ShardLevelFieldsReader(BlockFactory blockFactory, ShardContext shardContext, List<ValuesSourceReaderOperator.FieldInfo> fields) {
            this.blockFactory = new BlockLoaderFactory(blockFactory);
            final IndexReader indexReader = shardContext.searcher().getIndexReader();
            this.segments = new SegmentLevelFieldsReader[indexReader.leaves().size()];
            this.loaders = new BlockLoader[fields.size()];
            this.builders = new Block.Builder[loaders.length];
            StoredFieldsSpec storedFieldsSpec = StoredFieldsSpec.NO_REQUIREMENTS;
            for (int i = 0; i < fields.size(); i++) {
                BlockLoader loader = fields.get(i).blockLoader().apply(shardContext.index());
                storedFieldsSpec = storedFieldsSpec.merge(loader.rowStrideStoredFieldSpec());
                loaders[i] = loader;
            }
            for (int i = 0; i < indexReader.leaves().size(); i++) {
                LeafReaderContext leafReaderContext = indexReader.leaves().get(i);
                segments[i] = new SegmentLevelFieldsReader(leafReaderContext, loaders);
            }
            if (storedFieldsSpec.requiresSource()) {
                sourceLoader = shardContext.newSourceLoader();
                storedFieldsSpec = storedFieldsSpec.merge(new StoredFieldsSpec(false, false, sourceLoader.requiredStoredFields()));
            } else {
                sourceLoader = null;
            }
            this.storedFieldsSpec = storedFieldsSpec;
            this.dimensions = new boolean[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                dimensions[i] = shardContext.fieldType(fields.get(i).name()).isDimension();
            }
        }

        /**
         * For dimension fields, skips reading them when {@code nonDimensionFieldsOnly} is true,
         * since they only need to be read once per tsid.
         */
        void readValues(int segment, int docID, boolean nonDimensionFieldsOnly) throws IOException {
            segments[segment].read(docID, builders, nonDimensionFieldsOnly, dimensions);
        }

        void prepareForReading(int estimatedSize) throws IOException {
            if (this.builders.length > 0 && this.builders[0] == null) {
                for (int f = 0; f < builders.length; f++) {
                    builders[f] = (Block.Builder) loaders[f].builder(blockFactory, estimatedSize);
                }
            }
            for (SegmentLevelFieldsReader segment : segments) {
                if (segment != null) {
                    segment.reinitializeIfNeeded(sourceLoader, storedFieldsSpec);
                }
            }
        }

        Block[] buildBlocks(IntVector tsidOrdinals) {
            final Block[] blocks = new Block[loaders.length];
            try {
                for (int i = 0; i < builders.length; i++) {
                    if (dimensions[i]) {
                        blocks[i] = buildBlockForDimensionField(builders[i], tsidOrdinals);
                    } else {
                        blocks[i] = builders[i].build();
                    }
                }
                Arrays.fill(builders, null);
            } finally {
                if (blocks.length > 0 && blocks[blocks.length - 1] == null) {
                    Releasables.close(blocks);
                }
            }
            return blocks;
        }

        private Block buildBlockForDimensionField(Block.Builder builder, IntVector tsidOrdinals) {
            try (var values = builder.build()) {
                if (values.asVector() instanceof BytesRefVector bytes) {
                    tsidOrdinals.incRef();
                    values.incRef();
                    return new OrdinalBytesRefVector(tsidOrdinals, bytes).asBlock();
                } else if (values.areAllValuesNull()) {
                    return blockFactory.factory.newConstantNullBlock(tsidOrdinals.getPositionCount());
                } else {
                    final int positionCount = tsidOrdinals.getPositionCount();
                    try (var newBuilder = values.elementType().newBlockBuilder(positionCount, blockFactory.factory)) {
                        for (int p = 0; p < positionCount; p++) {
                            int pos = tsidOrdinals.getInt(p);
                            newBuilder.copyFrom(values, pos, pos + 1);
                        }
                        return newBuilder.build();
                    }
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(builders);
        }
    }

    static final class SegmentLevelFieldsReader {
        private final BlockLoader.RowStrideReader[] rowStride;
        private final BlockLoader[] loaders;
        private final LeafReaderContext leafContext;
        private BlockLoaderStoredFieldsFromLeafLoader storedFields;
        private Thread loadedThread = null;

        SegmentLevelFieldsReader(LeafReaderContext leafContext, BlockLoader[] loaders) {
            this.leafContext = leafContext;
            this.loaders = loaders;
            this.rowStride = new BlockLoader.RowStrideReader[loaders.length];
        }

        private void reinitializeIfNeeded(SourceLoader sourceLoader, StoredFieldsSpec storedFieldsSpec) throws IOException {
            final Thread currentThread = Thread.currentThread();
            if (loadedThread != currentThread) {
                loadedThread = currentThread;
                for (int f = 0; f < loaders.length; f++) {
                    rowStride[f] = loaders[f].rowStrideReader(leafContext);
                }
                storedFields = new BlockLoaderStoredFieldsFromLeafLoader(
                    StoredFieldLoader.fromSpec(storedFieldsSpec).getLoader(leafContext, null),
                    sourceLoader != null ? sourceLoader.leaf(leafContext.reader(), null) : null
                );
            }
        }

        void read(int docId, Block.Builder[] builder, boolean nonDimensionFieldsOnly, boolean[] dimensions) throws IOException {
            storedFields.advanceTo(docId);
            if (nonDimensionFieldsOnly) {
                for (int i = 0; i < rowStride.length; i++) {
                    if (dimensions[i] == false) {
                        rowStride[i].read(docId, storedFields, builder[i]);
                    }
                }
            } else {
                for (int i = 0; i < rowStride.length; i++) {
                    rowStride[i].read(docId, storedFields, builder[i]);
                }
            }
        }
    }

    /**
     * Collect tsids then build a {@link OrdinalBytesRefVector}
     */
    static final class TsidBuilder implements Releasable {
        private int currentOrd = -1;
        private final BytesRefVector.Builder dictBuilder;
        private final IntVector.Builder ordinalsBuilder;

        TsidBuilder(BlockFactory blockFactory, int estimatedSize) {
            final var dictBuilder = blockFactory.newBytesRefVectorBuilder(estimatedSize);
            boolean success = false;
            try {
                this.dictBuilder = dictBuilder;
                this.ordinalsBuilder = blockFactory.newIntVectorBuilder(estimatedSize);
                success = true;
            } finally {
                if (success == false) {
                    dictBuilder.close();
                }
            }
        }

        void appendNewTsid(BytesRef tsid) {
            currentOrd++;
            dictBuilder.appendBytesRef(tsid);
        }

        void appendOrdinal() {
            assert currentOrd >= 0;
            ordinalsBuilder.appendInt(currentOrd);
        }

        @Override
        public void close() {
            Releasables.close(dictBuilder, ordinalsBuilder);
        }

        OrdinalBytesRefVector build() throws IOException {
            BytesRefVector dict = null;
            OrdinalBytesRefVector result = null;
            IntVector ordinals = null;
            try {
                dict = dictBuilder.build();
                ordinals = ordinalsBuilder.build();
                result = new OrdinalBytesRefVector(ordinals, dict);
            } finally {
                if (result == null) {
                    Releasables.close(dict, ordinals);
                }
            }
            return result;
        }
    }

    static final class DocIdCollector implements Releasable {
        private final BlockFactory blockFactory;
        private final ShardContext shardContext;
        private IntVector.Builder docsBuilder;
        private IntVector.Builder segmentsBuilder;
        private final int[] docPerSegments;

        DocIdCollector(BlockFactory blockFactory, ShardContext shardContext) {
            this.blockFactory = blockFactory;
            this.shardContext = shardContext;
            docPerSegments = new int[shardContext.searcher().getIndexReader().leaves().size()];
        }

        void prepareForCollecting(int estimatedSize) {
            assert docsBuilder == null;
            docsBuilder = blockFactory.newIntVectorBuilder(estimatedSize);
            segmentsBuilder = blockFactory.newIntVectorBuilder(estimatedSize);
            Arrays.fill(docPerSegments, 0);
        }

        void collect(int segment, int docId) {
            docsBuilder.appendInt(docId);
            segmentsBuilder.appendInt(segment);
            docPerSegments[segment]++;
        }

        DocVector build() {
            IntVector shards = null;
            IntVector segments = null;
            IntVector docs = null;
            DocVector docVector = null;
            try {
                docs = docsBuilder.build();
                docsBuilder = null;
                segments = segmentsBuilder.build();
                segmentsBuilder = null;
                shards = blockFactory.newConstantIntVector(shardContext.index(), docs.getPositionCount());
                docVector = buildDocVector(shards, segments, docs, docPerSegments);
                return docVector;
            } finally {
                if (docVector == null) {
                    Releasables.close(docs, segments, shards);
                }
            }
        }

        private DocVector buildDocVector(IntVector shards, IntVector segments, IntVector docs, int[] docPerSegments) {
            if (segments.isConstant()) {
                // DocIds are sorted in each segment. Hence, if docIds come from a single segment, we can mark this DocVector
                // as singleSegmentNonDecreasing to enable optimizations in the ValuesSourceReaderOperator.
                return new DocVector(shards, segments, docs, true);
            }
            boolean success = false;
            int positionCount = shards.getPositionCount();
            long estimatedSize = DocVector.sizeOfSegmentDocMap(positionCount);
            blockFactory.adjustBreaker(estimatedSize);
            // Use docPerSegments to build a forward/backward docMap in O(N)
            // instead of O(N*log(N)) in DocVector#buildShardSegmentDocMapIfMissing.
            try {
                final int[] forwards = new int[positionCount];
                final int[] starts = new int[docPerSegments.length];
                for (int i = 1; i < starts.length; i++) {
                    starts[i] = starts[i - 1] + docPerSegments[i - 1];
                }
                for (int i = 0; i < segments.getPositionCount(); i++) {
                    final int segment = segments.getInt(i);
                    assert forwards[starts[segment]] == 0 : "must not set";
                    forwards[starts[segment]++] = i;
                }
                final int[] backwards = new int[forwards.length];
                for (int p = 0; p < forwards.length; p++) {
                    backwards[forwards[p]] = p;
                }
                final DocVector docVector = new DocVector(shards, segments, docs, forwards, backwards);
                success = true;
                return docVector;
            } finally {
                if (success == false) {
                    blockFactory.adjustBreaker(-estimatedSize);
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(docsBuilder, segmentsBuilder);
        }
    }

    @Override
    protected void describe(StringBuilder sb) {
        sb.append("[" + "maxPageSize=").append(maxPageSize).append(", remainingDocs=").append(remainingDocs).append("]");
    }

    @Override
    public Operator.Status status() {
        final long valuesLoaded = rowsEmitted * (1 + fieldsToExtracts.size()); // @timestamp and other fields
        return new Status(this, tsidsLoaded, valuesLoaded);
    }

    public static class Status extends LuceneOperator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "time_series_source",
            Status::new
        );

        private final long tsidLoaded;
        private final long valuesLoaded;

        Status(TimeSeriesSourceOperator operator, long tsidLoaded, long valuesLoaded) {
            super(operator);
            this.tsidLoaded = tsidLoaded;
            this.valuesLoaded = valuesLoaded;
        }

        Status(
            int processedSlices,
            Set<String> processedQueries,
            Set<String> processedShards,
            long processNanos,
            int sliceIndex,
            int totalSlices,
            int pagesEmitted,
            int sliceMin,
            int sliceMax,
            int current,
            long rowsEmitted,
            Map<String, LuceneSliceQueue.PartitioningStrategy> partitioningStrategies,
            long tsidLoaded,
            long valuesLoaded
        ) {
            super(
                processedSlices,
                processedQueries,
                processedShards,
                processNanos,
                sliceIndex,
                totalSlices,
                pagesEmitted,
                sliceMin,
                sliceMax,
                current,
                rowsEmitted,
                partitioningStrategies
            );
            this.tsidLoaded = tsidLoaded;
            this.valuesLoaded = valuesLoaded;
        }

        Status(StreamInput in) throws IOException {
            super(in);
            this.tsidLoaded = in.readVLong();
            this.valuesLoaded = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(tsidLoaded);
            out.writeVLong(valuesLoaded);
        }

        @Override
        protected void toXContentFields(XContentBuilder builder, Params params) throws IOException {
            super.toXContentFields(builder, params);
            builder.field("tsid_loaded", tsidLoaded);
            builder.field("values_loaded", valuesLoaded);
        }

        public long tsidLoaded() {
            return tsidLoaded;
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        @Override
        public boolean supportsVersion(TransportVersion version) {
            return version.onOrAfter(TransportVersions.ESQL_TIME_SERIES_SOURCE_STATUS);
        }

        @Override
        public long valuesLoaded() {
            return valuesLoaded;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            if (super.equals(o) == false) return false;
            Status status = (Status) o;
            return tsidLoaded == status.tsidLoaded && valuesLoaded == status.valuesLoaded;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), tsidLoaded, valuesLoaded);
        }
    }
}
