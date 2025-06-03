/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.DocValues;
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
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class TimeSeriesSourceOperator extends LuceneOperator {

    private final int maxPageSize;
    private final BlockFactory blockFactory;
    private final LuceneSliceQueue sliceQueue;
    private int currentPagePos = 0;
    private int remainingDocs;
    private boolean doneCollecting;

    private LongVector.Builder timestampsBuilder;
    private TsidBuilder tsHashesBuilder;
    private SegmentsIterator iterator;
    private DocIdCollector docCollector;
    private long tsidsLoaded;

    TimeSeriesSourceOperator(BlockFactory blockFactory, LuceneSliceQueue sliceQueue, int maxPageSize, int limit) {
        super(blockFactory, maxPageSize, sliceQueue);
        this.maxPageSize = maxPageSize;
        this.blockFactory = blockFactory;
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
        Block[] blocks = new Block[3];
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
                iterator = new SegmentsIterator(slice);
                docCollector = new DocIdCollector(blockFactory, slice.shardContext());
            }
            iterator.readDocsForNextPage();
            if (currentPagePos > 0) {
                blocks[0] = docCollector.build().asBlock();
                OrdinalBytesRefVector tsidVector = tsHashesBuilder.build();
                blocks[1] = tsidVector.asBlock();
                tsHashesBuilder = new TsidBuilder(blockFactory, Math.min(remainingDocs, maxPageSize));
                blocks[2] = timestampsBuilder.build().asBlock();
                timestampsBuilder = blockFactory.newLongVectorBuilder(Math.min(remainingDocs, maxPageSize));
                page = new Page(currentPagePos, blocks);
                currentPagePos = 0;
            }
            if (iterator.completed()) {
                processedShards.add(iterator.luceneSlice.shardContext().shardIdentifier());
                processedSlices++;
                Releasables.close(docCollector);
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
        Releasables.closeExpectNoException(timestampsBuilder, tsHashesBuilder, docCollector);
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
            docCollector.prepareForCollecting(Math.min(remainingDocs, maxPageSize));
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
            do {
                LeafIterator top = sub.top();
                currentPagePos++;
                remainingDocs--;
                docCollector.collect(top.segmentOrd, top.docID);
                tsHashesBuilder.appendOrdinal();
                timestampsBuilder.appendLong(top.timestamp);
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

        DocIdCollector(BlockFactory blockFactory, ShardContext shardContext) {
            this.blockFactory = blockFactory;
            this.shardContext = shardContext;
        }

        void prepareForCollecting(int estimatedSize) {
            assert docsBuilder == null;
            docsBuilder = blockFactory.newIntVectorBuilder(estimatedSize);
            segmentsBuilder = blockFactory.newIntVectorBuilder(estimatedSize);
        }

        void collect(int segment, int docId) {
            docsBuilder.appendInt(docId);
            segmentsBuilder.appendInt(segment);
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
                docVector = new DocVector(shards, segments, docs, segments.isConstant());
                return docVector;
            } finally {
                if (docVector == null) {
                    Releasables.close(docs, segments, shards);
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
        final long valuesLoaded = rowsEmitted; // @timestamp field
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
