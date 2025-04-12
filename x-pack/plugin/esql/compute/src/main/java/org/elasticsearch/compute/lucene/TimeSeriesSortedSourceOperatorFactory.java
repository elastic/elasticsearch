/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Function;

/**
 * Creates a source operator that takes advantage of the natural sorting of segments in a tsdb index.
 * <p>
 * This source operator loads the _tsid and @timestamp fields, which is used for emitting documents in the correct order. These field values
 * are included in the page as seperate blocks and downstream operators can make use of these loaded time series ids and timestamps.
 * <p>
 * The source operator includes all documents of a time serie with the same page. So the same time series never exists in multiple pages.
 * Downstream operators can make use of this implementation detail.
 * <p>
 * This operator currently only supports shard level concurrency. A new concurrency mechanism should be introduced at the time serie level
 * in order to read tsdb indices in parallel.
 */
public class TimeSeriesSortedSourceOperatorFactory extends LuceneOperator.Factory {

    private final int maxPageSize;

    private TimeSeriesSortedSourceOperatorFactory(
        List<? extends ShardContext> contexts,
        Function<ShardContext, Query> queryFunction,
        int taskConcurrency,
        int maxPageSize,
        int limit
    ) {
        super(
            contexts,
            queryFunction,
            DataPartitioning.SHARD,
            query -> { throw new UnsupportedOperationException("locked to SHARD partitioning"); },
            taskConcurrency,
            limit,
            false,
            ScoreMode.COMPLETE_NO_SCORES
        );
        this.maxPageSize = maxPageSize;
    }

    @Override
    public SourceOperator get(DriverContext driverContext) {
        return new Impl(driverContext.blockFactory(), sliceQueue, maxPageSize, limit);
    }

    @Override
    public String describe() {
        return "TimeSeriesSortedSourceOperator[maxPageSize = " + maxPageSize + ", limit = " + limit + "]";
    }

    public static TimeSeriesSortedSourceOperatorFactory create(
        int limit,
        int maxPageSize,
        int taskConcurrency,
        List<? extends ShardContext> searchContexts,
        Function<ShardContext, Query> queryFunction
    ) {
        return new TimeSeriesSortedSourceOperatorFactory(searchContexts, queryFunction, taskConcurrency, maxPageSize, limit);
    }

    static final class Impl extends SourceOperator {

        private final int maxPageSize;
        private final BlockFactory blockFactory;
        private final LuceneSliceQueue sliceQueue;
        private int currentPagePos = 0;
        private int remainingDocs;
        private boolean doneCollecting;
        private IntVector.Builder docsBuilder;
        private IntVector.Builder segmentsBuilder;
        private LongVector.Builder timestampsBuilder;
        // TODO: add an ordinal block for tsid hashes
        // (This allows for efficiently grouping by tsid locally, no need to use bytes representation of tsid hash)
        private BytesRefVector.Builder tsHashesBuilder;
        private TimeSeriesIterator iterator;

        Impl(BlockFactory blockFactory, LuceneSliceQueue sliceQueue, int maxPageSize, int limit) {
            this.maxPageSize = maxPageSize;
            this.blockFactory = blockFactory;
            this.remainingDocs = limit;
            this.docsBuilder = blockFactory.newIntVectorBuilder(Math.min(limit, maxPageSize));
            this.segmentsBuilder = null;
            this.timestampsBuilder = blockFactory.newLongVectorBuilder(Math.min(limit, maxPageSize));
            this.tsHashesBuilder = blockFactory.newBytesRefVectorBuilder(Math.min(limit, maxPageSize));
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
        public Page getOutput() {
            if (isFinished()) {
                return null;
            }

            if (remainingDocs <= 0) {
                doneCollecting = true;
                return null;
            }

            Page page = null;
            IntBlock shard = null;
            IntVector leaf = null;
            IntVector docs = null;
            LongVector timestamps = null;
            BytesRefVector tsids = null;
            try {
                if (iterator == null) {
                    var slice = sliceQueue.nextSlice();
                    if (slice == null) {
                        doneCollecting = true;
                        return null;
                    }
                    if (segmentsBuilder == null && slice.numLeaves() > 1) {
                        segmentsBuilder = blockFactory.newIntVectorBuilder(Math.min(remainingDocs, maxPageSize));
                    }
                    iterator = new TimeSeriesIterator(slice);
                }
                iterator.consume();
                shard = blockFactory.newConstantIntBlockWith(iterator.slice.shardContext().index(), currentPagePos);
                if (iterator.slice.numLeaves() == 1) {
                    int segmentOrd = iterator.slice.getLeaf(0).leafReaderContext().ord;
                    leaf = blockFactory.newConstantIntBlockWith(segmentOrd, currentPagePos).asVector();
                } else {
                    // Due to the multi segment nature of time series source operator singleSegmentNonDecreasing must be false
                    leaf = segmentsBuilder.build();
                    segmentsBuilder = blockFactory.newIntVectorBuilder(Math.min(remainingDocs, maxPageSize));
                }
                docs = docsBuilder.build();
                docsBuilder = blockFactory.newIntVectorBuilder(Math.min(remainingDocs, maxPageSize));

                timestamps = timestampsBuilder.build();
                timestampsBuilder = blockFactory.newLongVectorBuilder(Math.min(remainingDocs, maxPageSize));
                tsids = tsHashesBuilder.build();
                tsHashesBuilder = blockFactory.newBytesRefVectorBuilder(Math.min(remainingDocs, maxPageSize));
                page = new Page(
                    currentPagePos,
                    new DocVector(shard.asVector(), leaf, docs, leaf.isConstant()).asBlock(),
                    tsids.asBlock(),
                    timestamps.asBlock()
                );

                currentPagePos = 0;
                if (iterator.completed()) {
                    iterator = null;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                if (page == null) {
                    Releasables.closeExpectNoException(shard, leaf, docs, timestamps, tsids);
                }
            }
            return page;
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(docsBuilder, segmentsBuilder, timestampsBuilder, tsHashesBuilder);
        }

        class TimeSeriesIterator {

            final LuceneSlice slice;
            final Leaf leaf;
            final PriorityQueue<Leaf> queue;
            int globalTsidOrd;
            BytesRef currentTsid = new BytesRef();

            TimeSeriesIterator(LuceneSlice slice) throws IOException {
                this.slice = slice;
                Weight weight = slice.weight();
                if (slice.numLeaves() == 1) {
                    queue = null;
                    leaf = new Leaf(weight, slice.getLeaf(0).leafReaderContext());
                } else {
                    queue = new PriorityQueue<>(slice.numLeaves()) {
                        @Override
                        protected boolean lessThan(Leaf a, Leaf b) {
                            // tsid hash in ascending order:
                            int cmp = a.timeSeriesHash.compareTo(b.timeSeriesHash);
                            if (cmp == 0) {
                                // timestamp in descending order:
                                cmp = -Long.compare(a.timestamp, b.timestamp);
                            }
                            return cmp < 0;
                        }
                    };
                    leaf = null;
                    for (var leafReaderContext : slice.leaves()) {
                        Leaf leaf = new Leaf(weight, leafReaderContext.leafReaderContext());
                        if (leaf.nextDoc()) {
                            queue.add(leaf);
                        }
                    }
                }
            }

            void consume() throws IOException {
                if (queue != null) {
                    if (queue.size() > 0) {
                        currentTsid = BytesRef.deepCopyOf(queue.top().timeSeriesHash);
                        queue.top().reinitializeIfNeeded(Thread.currentThread());
                    }
                    while (queue.size() > 0) {
                        if (remainingDocs <= 0 || currentPagePos >= maxPageSize) {
                            break;
                        }
                        currentPagePos++;
                        remainingDocs--;
                        Leaf leaf = queue.top();
                        segmentsBuilder.appendInt(leaf.segmentOrd);
                        docsBuilder.appendInt(leaf.iterator.docID());
                        timestampsBuilder.appendLong(leaf.timestamp);
                        tsHashesBuilder.appendBytesRef(currentTsid);
                        final Leaf newTop;
                        if (leaf.nextDoc()) {
                            // TODO: updating the top is one of the most expensive parts of this operation.
                            // Ideally we would do this a less as possible. Maybe the top can be updated every N docs?
                            newTop = queue.updateTop();
                        } else {
                            queue.pop();
                            newTop = queue.size() > 0 ? queue.top() : null;
                        }
                        if (newTop != null) {
                            if (newTop != leaf) {
                                newTop.reinitializeIfNeeded(Thread.currentThread());
                            }
                            if (newTop.timeSeriesHash.equals(currentTsid) == false) {
                                globalTsidOrd++;
                                currentTsid = BytesRef.deepCopyOf(newTop.timeSeriesHash);
                            }
                        }
                    }
                } else {
                    // Only one segment, so no need to use priority queue and use segment ordinals as tsid ord.
                    leaf.reinitializeIfNeeded(Thread.currentThread());
                    while (leaf.nextDoc()) {
                        tsHashesBuilder.appendBytesRef(leaf.timeSeriesHash);
                        timestampsBuilder.appendLong(leaf.timestamp);
                        // Don't append segment ord, because there is only one segment.
                        docsBuilder.appendInt(leaf.iterator.docID());
                        currentPagePos++;
                        remainingDocs--;
                        if (remainingDocs <= 0 || currentPagePos >= maxPageSize) {
                            break;
                        }
                    }
                }
            }

            boolean completed() {
                if (queue != null) {
                    return iterator.queue.size() == 0;
                } else {
                    return leaf.iterator.docID() == DocIdSetIterator.NO_MORE_DOCS;
                }
            }

            static class Leaf {

                private final int segmentOrd;
                private final Weight weight;
                private final LeafReaderContext leaf;
                private SortedDocValues tsids;
                private SortedNumericDocValues timestamps;
                private DocIdSetIterator iterator;
                private Thread createdThread;

                private long timestamp;
                private int timeSeriesHashOrd;
                private BytesRef timeSeriesHash;
                private int docID = -1;

                Leaf(Weight weight, LeafReaderContext leaf) throws IOException {
                    this.segmentOrd = leaf.ord;
                    this.weight = weight;
                    this.leaf = leaf;
                    this.createdThread = Thread.currentThread();
                    tsids = leaf.reader().getSortedDocValues("_tsid");
                    timestamps = leaf.reader().getSortedNumericDocValues("@timestamp");
                    final Scorer scorer = weight.scorer(leaf);
                    iterator = scorer != null ? scorer.iterator() : DocIdSetIterator.empty();
                }

                boolean nextDoc() throws IOException {
                    docID = iterator.nextDoc();
                    if (docID == DocIdSetIterator.NO_MORE_DOCS) {
                        return false;
                    }

                    boolean advanced = tsids.advanceExact(docID);
                    assert advanced;
                    timeSeriesHashOrd = tsids.ordValue();
                    timeSeriesHash = tsids.lookupOrd(timeSeriesHashOrd);
                    advanced = timestamps.advanceExact(docID);
                    assert advanced;
                    timestamp = timestamps.nextValue();
                    return true;
                }

                void reinitializeIfNeeded(Thread executingThread) throws IOException {
                    if (executingThread != createdThread) {
                        tsids = leaf.reader().getSortedDocValues("_tsid");
                        timestamps = leaf.reader().getSortedNumericDocValues("@timestamp");
                        final Scorer scorer = weight.scorer(leaf);
                        iterator = scorer != null ? scorer.iterator() : DocIdSetIterator.empty();
                        if (docID != -1) {
                            iterator.advance(docID);
                        }
                        createdThread = executingThread;
                    }
                }
            }

        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "[" + "maxPageSize=" + maxPageSize + ", remainingDocs=" + remainingDocs + "]";
        }

    }
}
