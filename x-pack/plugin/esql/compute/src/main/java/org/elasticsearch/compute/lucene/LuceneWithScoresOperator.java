/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.Weight;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * This is pretty much a copy of LuceneTopNSourceOperator but without the sorts
 * option and that always includes the scores.
 */
public final class LuceneWithScoresOperator extends LuceneOperator {
    private ScoreDoc scoreDoc;

    public static final class Factory implements LuceneOperator.Factory {
        private final int taskConcurrency;
        private final int maxPageSize;
        private final int limit;
        private final DataPartitioning dataPartitioning;
        private final LuceneSliceQueue sliceQueue;

        public Factory(
            List<? extends ShardContext> contexts,
            Function<ShardContext, Query> queryFunction,
            DataPartitioning dataPartitioning,
            int taskConcurrency,
            int maxPageSize,
            int limit
        ) {
            this.maxPageSize = maxPageSize;
            this.limit = limit;
            this.dataPartitioning = dataPartitioning;
            var weightFunction = weightFunction(queryFunction, ScoreMode.TOP_DOCS_WITH_SCORES);
            this.sliceQueue = LuceneSliceQueue.create(contexts, weightFunction, dataPartitioning, taskConcurrency);
            this.taskConcurrency = Math.min(sliceQueue.totalSlices(), taskConcurrency);
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new LuceneWithScoresOperator(driverContext.blockFactory(), maxPageSize, limit, sliceQueue);
        }

        @Override
        public int taskConcurrency() {
            return taskConcurrency;
        }

        public int maxPageSize() {
            return maxPageSize;
        }

        public int limit() {
            return limit;
        }

        @Override
        public String describe() {
            return "LuceneWithScoresOperator[dataPartitioning = "
                + dataPartitioning
                + ", maxPageSize = "
                + maxPageSize
                + ", limit = "
                + limit
                + "]";
        }
    }

    /**
     * Collected docs. {@code null} until we're {@link #emit(boolean)}.
     */
    private ScoreDoc[] scoreDocs;
    /**
     * The offset in {@link #scoreDocs} of the next page.
     */
    private int offset = 0;

    private PerShardCollector perShardCollector;
    private final int limit;

    public LuceneWithScoresOperator(
        BlockFactory blockFactory,
        int maxPageSize,
        int limit,
        LuceneSliceQueue sliceQueue
    ) {
        super(blockFactory, maxPageSize, sliceQueue);
        this.limit = limit;
    }

    @Override
    public boolean isFinished() {
        return doneCollecting && isEmitting() == false;
    }

    @Override
    public void finish() {
        doneCollecting = true;
        scoreDocs = null;
        assert isFinished();
    }

    @Override
    public Page getOutput() {
        if (isFinished()) {
            return null;
        }
        long start = System.nanoTime();
        try {
            if (isEmitting()) {
                return emit(false);
            } else {
                return collect();
            }
        } finally {
            processingNanos += System.nanoTime() - start;
        }
    }

    private Page collect() {
        assert doneCollecting == false;
        var scorer = getCurrentOrLoadNextScorer();
        if (scorer == null) {
            doneCollecting = true;
            return emit(true);
        }
        try {
            if (perShardCollector == null || perShardCollector.shardContext.index() != scorer.shardContext().index()) {
                // TODO: share the bottom between shardCollectors
                perShardCollector = new PerShardCollector(scorer.shardContext(), limit);
            }
            var leafCollector = perShardCollector.getLeafCollector(scorer.leafReaderContext());
            scorer.scoreNextRange(leafCollector, scorer.leafReaderContext().reader().getLiveDocs(), maxPageSize);
        } catch (CollectionTerminatedException cte) {
            // Lucene terminated early the collection (doing topN for an index that's sorted and the topN uses the same sorting)
            scorer.markAsDone();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (scorer.isDone()) {
            var nextScorer = getCurrentOrLoadNextScorer();
            if (nextScorer == null || nextScorer.shardContext().index() != scorer.shardContext().index()) {
                return emit(true);
            }
        }
        return null;
    }

    private boolean isEmitting() {
        return scoreDocs != null && offset < scoreDocs.length;
    }

    private Page emit(boolean startEmitting) {
        if (startEmitting) {
            assert isEmitting() == false : "offset=" + offset + " score_docs=" + Arrays.toString(scoreDocs);
            offset = 0;
            if (perShardCollector != null) {
                scoreDocs = perShardCollector.topFieldCollector.topDocs().scoreDocs;
            } else {
                scoreDocs = new ScoreDoc[0];
            }
        }
        if (offset >= scoreDocs.length) {
            return null;
        }
        int size = Math.min(maxPageSize, scoreDocs.length - offset);
        IntBlock shard = null;
        IntVector segments = null;
        IntVector docs = null;
        DoubleVector scores = null;
        Page page = null;
        try (
            IntVector.Builder currentSegmentBuilder = blockFactory.newIntVectorFixedBuilder(size);
            IntVector.Builder currentDocsBuilder = blockFactory.newIntVectorFixedBuilder(size);
            DoubleVector.Builder currentScoresBuilder = blockFactory.newDoubleVectorBuilder(size);
        ) {
            int start = offset;
            offset += size;
            List<LeafReaderContext> leafContexts = perShardCollector.shardContext.searcher().getLeafContexts();
            for (int i = start; i < offset; i++) {
                int doc = scoreDocs[i].doc;
                int segment = ReaderUtil.subIndex(doc, leafContexts);
                float score = (float) ((FieldDoc) scoreDocs[i]).fields[0];
                currentSegmentBuilder.appendInt(segment);
                currentDocsBuilder.appendInt(doc - leafContexts.get(segment).docBase); // the offset inside the segment
                currentScoresBuilder.appendDouble(score);
            }

            shard = blockFactory.newConstantIntBlockWith(perShardCollector.shardContext.index(), size);
            segments = currentSegmentBuilder.build();
            docs = currentDocsBuilder.build();
            scores = currentScoresBuilder.build();
            page = new Page(size, new DocVector(shard.asVector(), segments, docs, scores, null, null).asBlock());
        } finally {
            if (page == null) {
                Releasables.closeExpectNoException(shard, segments, docs);
            }
        }
        pagesEmitted++;
        return page;
    }

    @Override
    protected void describe(StringBuilder sb) {
        sb.append(", limit=").append(limit);
    }

    static final class PerShardCollector {
        private final ShardContext shardContext;
        private final TopFieldCollector topFieldCollector;
        private int leafIndex;
        private LeafCollector leafCollector;
        private Thread currentThread;

        PerShardCollector(ShardContext shardContext, int limit) throws IOException {
            this.shardContext = shardContext;
            // We don't use CollectorManager here as we don't retrieve the total hits and sort by score.
            this.topFieldCollector = TopFieldCollector.create(Sort.RELEVANCE, limit, 0);
        }

        LeafCollector getLeafCollector(LeafReaderContext leafReaderContext) throws IOException {
            if (currentThread != Thread.currentThread() || leafIndex != leafReaderContext.ord) {
                leafCollector = topFieldCollector.getLeafCollector(leafReaderContext);
                leafIndex = leafReaderContext.ord;
                currentThread = Thread.currentThread();
            }
            return leafCollector;
        }
    }

    static Function<ShardContext, Weight> weightFunction(Function<ShardContext, Query> queryFunction, ScoreMode scoreMode) {
        return ctx -> {
            final var query = queryFunction.apply(ctx);
            final var searcher = ctx.searcher();
            try {
                return searcher.createWeight(searcher.rewrite(query), scoreMode, 1);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }
}
