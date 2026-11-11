/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.lucene.read.SumCountLongBlockLoaderFactory;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Sums, exactly in 128 bits, and counts the values of a {@code long} field without ever building a page of them,
 * and for the segments that have no deleted docs and that the query matches entirely, without ever looking at a doc id: that is
 * {@code STATS SUM(x), COUNT(x)} (and so {@code AVG(x)}) for a {@code long} {@code x}.
 * <p>
 *     The values are read with the field's own {@link BlockLoader}, as they would be for any other column, but into a
 *     {@link SumCountLongBlockLoaderFactory}, which adds them up as they are decoded instead of building a block. For the
 *     segments the query matches entirely the loader is handed a {@link BlockLoader.Docs} that is just a range, so there is no
 *     array of doc ids either. For the others the matching doc ids are collected {@link #SCORED_DOCS_INTERVAL} at a time.
 * </p>
 * <p>
 *     Emits a single page with the intermediate state of {@code SUM} of a {@code double} followed by the one of {@code COUNT},
 *     {@code (value, delta, seen, count, seen)}, so what the coordinator merges is what the plain aggregations would have sent.
 * </p>
 */
public final class LuceneSumCountLongOperator extends LuceneOperator {
    /**
     * How many docs of a segment that is matched entirely are read in one go, which is as long as the operator keeps the
     * driver without checking for cancellation, and how far the status is behind.
     */
    static final int FAST_PATH_DOCS = 1 << 16;

    /**
     * The same as {@link LuceneCountOperator} scores in one go when it has to collect.
     */
    static final int SCORED_DOCS_INTERVAL = 1 << 12;

    public static class Factory extends LuceneOperator.Factory {
        private final IndexedByShardId<? extends RefCounted> shardRefCounters;
        private final ValuesSourceReaderOperator.FieldInfo field;

        public Factory(
            IndexedByShardId<? extends ShardContext> contexts,
            Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFunction,
            DataPartitioning dataPartitioning,
            int docThresholdForAutoStrategy,
            int taskConcurrency,
            ValuesSourceReaderOperator.FieldInfo field,
            LongSupplier directoryBytesRead,
            int minDocsPerSlice,
            QueryWarnings singleValueQueryWarnings
        ) {
            super(
                contexts,
                queryFunction,
                dataPartitioning,
                // It visits every matching doc, like the STATS source does.
                LuceneSourceOperator.Factory.autoStrategy((long) minDocsPerSlice).pickStrategy(NO_LIMIT),
                docThresholdForAutoStrategy,
                taskConcurrency,
                NO_LIMIT,
                false,
                shardContext -> ScoreMode.COMPLETE_NO_SCORES,
                directoryBytesRead,
                minDocsPerSlice,
                singleValueQueryWarnings
            );
            this.shardRefCounters = contexts;
            this.field = field;
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new LuceneSumCountLongOperator(
                shardRefCounters,
                driverContext,
                sliceQueue,
                field,
                directoryBytesRead,
                singleValueQueryWarnings
            );
        }

        @Override
        public String describe() {
            return "LuceneSumCountLongOperator[dataPartitioning = " + dataPartitioning + ", field = " + field.name() + "]";
        }
    }

    private final DriverContext driverContext;
    private final ValuesSourceReaderOperator.FieldInfo field;
    private final SumCountLongBlockLoaderFactory loaderFactory;
    private final Map<Integer, BlockLoader> loaders = new HashMap<>();

    private final RangeDocs rangeDocs = new RangeDocs();
    private final ArrayDocs arrayDocs = new ArrayDocs();
    private final LeafCollector collector = new LeafCollector() {
        @Override
        public void setScorer(Scorable scorer) {}

        @Override
        public void collect(int doc) {
            arrayDocs.docs[arrayDocs.count++] = doc;
        }
    };

    private LeafReaderContext readerLeaf;
    private BlockLoader.ColumnAtATimeReader reader;

    private long docsWithoutIds;
    private long docsWithIds;

    LuceneSumCountLongOperator(
        IndexedByShardId<? extends RefCounted> shardRefCounters,
        DriverContext driverContext,
        LuceneSliceQueue sliceQueue,
        ValuesSourceReaderOperator.FieldInfo field,
        LongSupplier directoryBytesRead,
        QueryWarnings singleValueQueryWarnings
    ) {
        super(shardRefCounters, driverContext, 1, sliceQueue, directoryBytesRead, singleValueQueryWarnings);
        this.driverContext = driverContext;
        this.field = field;
        this.loaderFactory = new SumCountLongBlockLoaderFactory(driverContext.blockFactory());
    }

    @Override
    public boolean isFinished() {
        return doneCollecting;
    }

    @Override
    public void finish() {
        doneCollecting = true;
    }

    @Override
    protected Page getCheckedOutput() throws IOException {
        final long start = System.nanoTime();
        try {
            final LuceneScorer scorer = getCurrentOrLoadNextScorer();
            if (scorer != null) {
                if (scorer.tags().isEmpty() == false) {
                    throw new UnsupportedOperationException("tags not supported by " + getClass());
                }
                sumNext(scorer);
                return null;
            }
            // no scorer means no more docs
            if (isFinished() && pagesEmitted == 0) {
                return buildPage();
            }
            return null;
        } finally {
            processingNanos += System.nanoTime() - start;
        }
    }

    private void sumNext(LuceneScorer scorer) throws IOException {
        final LeafReaderContext leaf = scorer.leafReaderContext();
        final LeafReader leafReader = leaf.reader();
        final int from = scorer.position();
        // Whole leaf partitions are open ended.
        final int to = Math.min(scorer.maxPosition(), leafReader.maxDoc());
        if (from >= to) {
            // Only an open ended range gets here: the scorer is done when it is at the end of a range that has one.
            scorer.markAsDone();
            return;
        }
        if (matchesEverything(scorer, leafReader)) {
            final int end = (int) Math.min(to, (long) from + FAST_PATH_DOCS);
            read(leaf, scorer, rangeDocs.reset(from, end - from));
            docsWithoutIds += end - from;
            if (end < to || scorer.maxPosition() == to) {
                scorer.advanceTo(end);
            } else {
                // An open ended range, there is nothing after it. Don't use markAsDone() for the others: with DOC partitioning
                // the next slice of the leaf would start from the position it leaves and be skipped.
                scorer.markAsDone();
            }
            return;
        }
        arrayDocs.reset();
        scorer.scoreNextRange(collector, leafReader.getLiveDocs(), SCORED_DOCS_INTERVAL);
        if (arrayDocs.count > 0) {
            read(leaf, scorer, arrayDocs);
            docsWithIds += arrayDocs.count;
        }
    }

    /**
     * Whether every doc of the leaf is visited, which is when the query matches all of them and none of them is deleted. Then the
     * docs are just a range.
     */
    private static boolean matchesEverything(LuceneScorer scorer, LeafReader leafReader) {
        Query query = scorer.weight().getQuery();
        return (query == null || query instanceof MatchAllDocsQuery) && leafReader.getLiveDocs() == null;
    }

    private void read(LeafReaderContext leaf, LuceneScorer scorer, BlockLoader.Docs docs) throws IOException {
        if (reader == null || readerLeaf != leaf || reader.canReuse(docs.get(0)) == false) {
            openReader(leaf, scorer.shardContext().index());
        }
        // The block is a placeholder, the values went to the loader factory.
        reader.read(loaderFactory, docs, 0, false);
    }

    private void openReader(LeafReaderContext leaf, int shard) throws IOException {
        Releasables.close(reader);
        reader = null;
        readerLeaf = null;
        BlockLoader loader = loaders.computeIfAbsent(shard, s -> field.buildLoader().build(driverContext, s).loader());
        var readerFunction = loader.columnAtATimeReader(leaf);
        if (readerFunction == null) {
            throw new IllegalStateException("[" + field.name() + "] can't be read a column at a time");
        }
        CircuitBreaker breaker = driverContext.breaker();
        reader = readerFunction.apply(breaker);
        readerLeaf = leaf;
    }

    private Page buildPage() {
        BigInteger sum = loaderFactory.sum();
        long count = loaderFactory.count();
        // The sum is rounded once, and what that rounding lost is the correction term the double sum would have, so that
        // value + delta is the exact sum.
        double value = sum.doubleValue();
        double delta = sum.subtract(new BigDecimal(value).toBigInteger()).doubleValue();

        Block[] blocks = new Block[5];
        Page page = null;
        try {
            blocks[0] = blockFactory.newConstantDoubleBlockWith(value, 1);
            blocks[1] = blockFactory.newConstantDoubleBlockWith(delta, 1);
            blocks[2] = blockFactory.newConstantBooleanBlockWith(count > 0, 1);
            blocks[3] = blockFactory.newConstantLongBlockWith(count, 1);
            blocks[4] = blockFactory.newConstantBooleanBlockWith(true, 1);
            page = new Page(1, blocks);
        } finally {
            if (page == null) {
                Releasables.closeExpectNoException(blocks);
            }
        }
        return page;
    }

    @Override
    protected void additionalClose() {
        Releasables.closeExpectNoException(reader);
        reader = null;
        loaderFactory.close();
    }

    @Override
    protected void describe(StringBuilder sb) {
        sb.append(", field = ").append(field.name());
        sb.append(", docsWithoutIds = ").append(docsWithoutIds);
        sb.append(", docsWithIds = ").append(docsWithIds);
    }

    /** The docs {@code [start, start + count)}, without an array. */
    private static final class RangeDocs implements BlockLoader.Docs {
        private int start;
        private int count;

        RangeDocs reset(int start, int count) {
            this.start = start;
            this.count = count;
            return this;
        }

        @Override
        public int count() {
            return count;
        }

        @Override
        public int get(int i) {
            return start + i;
        }

        @Override
        public boolean mayContainDuplicates() {
            return false;
        }
    }

    /** The doc ids the collector found, in increasing order. */
    private static final class ArrayDocs implements BlockLoader.Docs {
        private final int[] docs = new int[SCORED_DOCS_INTERVAL];
        private int count;

        void reset() {
            count = 0;
        }

        @Override
        public int count() {
            return count;
        }

        @Override
        public int get(int i) {
            return docs[i];
        }

        @Override
        public boolean mayContainDuplicates() {
            return false;
        }
    }
}
