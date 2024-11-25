/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static org.apache.lucene.search.ScoreMode.COMPLETE;
import static org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES;

/**
 * Source operator that incrementally runs Lucene searches
 */
public class LuceneSourceOperator extends LuceneOperator {

    private int currentPagePos = 0;
    private int remainingDocs;

    private IntVector.Builder docsBuilder;
    private DoubleVector.Builder scoreBuilder;
    private final LeafCollector leafCollector;
    private final int minPageSize;

    public static class Factory extends LuceneOperator.Factory {

        private final int maxPageSize;

        public Factory(
            List<? extends ShardContext> contexts,
            Function<ShardContext, Query> queryFunction,
            DataPartitioning dataPartitioning,
            int taskConcurrency,
            int maxPageSize,
            int limit,
            boolean scoring
        ) {
            super(contexts, queryFunction, dataPartitioning, taskConcurrency, limit, scoring ? COMPLETE : COMPLETE_NO_SCORES);
            this.maxPageSize = maxPageSize;
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new LuceneSourceOperator(driverContext.blockFactory(), maxPageSize, sliceQueue, limit, scoreMode);
        }

        public int maxPageSize() {
            return maxPageSize;
        }

        @Override
        public String describe() {
            return "LuceneSourceOperator[dataPartitioning = "
                + dataPartitioning
                + ", maxPageSize = "
                + maxPageSize
                + ", limit = "
                + limit
                + ", scoreMode = "
                + scoreMode
                + "]";
        }
    }

    @SuppressWarnings("this-escape")
    public LuceneSourceOperator(BlockFactory blockFactory, int maxPageSize, LuceneSliceQueue sliceQueue, int limit, ScoreMode scoreMode) {
        super(blockFactory, maxPageSize, sliceQueue);
        this.minPageSize = Math.max(1, maxPageSize / 2);
        this.remainingDocs = limit;
        int estimatedSize = Math.min(limit, maxPageSize);
        boolean success = false;
        try {
            this.docsBuilder = blockFactory.newIntVectorBuilder(estimatedSize);
            if (scoreMode.needsScores()) {
                scoreBuilder = blockFactory.newDoubleVectorBuilder(estimatedSize);
                this.leafCollector = new ScoringCollector();
            } else {
                scoreBuilder = null;
                this.leafCollector = new LimitingCollector();
            }
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    class LimitingCollector implements LeafCollector {
        @Override
        public void setScorer(Scorable scorer) {}

        @Override
        public void collect(int doc) throws IOException {
            if (remainingDocs > 0) {
                --remainingDocs;
                docsBuilder.appendInt(doc);
                currentPagePos++;
            } else {
                throw new CollectionTerminatedException();
            }
        }
    }

    final class ScoringCollector extends LuceneSourceOperator.LimitingCollector {
        private Scorable scorable;

        @Override
        public void setScorer(Scorable scorer) {
            this.scorable = scorer;
        }

        @Override
        public void collect(int doc) throws IOException {
            super.collect(doc);
            scoreBuilder.appendDouble(scorable.score());
        }
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
    public Page getCheckedOutput() throws IOException {
        if (isFinished()) {
            assert currentPagePos == 0 : currentPagePos;
            return null;
        }
        long start = System.nanoTime();
        try {
            final LuceneScorer scorer = getCurrentOrLoadNextScorer();
            if (scorer == null) {
                return null;
            }
            try {
                scorer.scoreNextRange(
                    leafCollector,
                    scorer.leafReaderContext().reader().getLiveDocs(),
                    // Note: if (maxPageSize - currentPagePos) is a small "remaining" interval, this could lead to slow collection with a
                    // highly selective filter. Having a large "enough" difference between max- and minPageSize (and thus currentPagePos)
                    // alleviates this issue.
                    maxPageSize - currentPagePos
                );
            } catch (CollectionTerminatedException ex) {
                // The leaf collector terminated the execution
                scorer.markAsDone();
            }
            Page page = null;
            if (currentPagePos >= minPageSize || remainingDocs <= 0 || scorer.isDone()) {
                pagesEmitted++;
                IntBlock shard = null;
                IntBlock leaf = null;
                IntVector docs = null;
                DoubleVector scores = null;
                DocBlock docBlock = null;
                try {
                    shard = blockFactory.newConstantIntBlockWith(scorer.shardContext().index(), currentPagePos);
                    leaf = blockFactory.newConstantIntBlockWith(scorer.leafReaderContext().ord, currentPagePos);
                    docs = docsBuilder.build();
                    docsBuilder = blockFactory.newIntVectorBuilder(Math.min(remainingDocs, maxPageSize));
                    docBlock = new DocVector(shard.asVector(), leaf.asVector(), docs, true).asBlock();
                    shard = null;
                    leaf = null;
                    docs = null;
                    if (scoreBuilder == null) {
                        page = new Page(currentPagePos, docBlock);
                    } else {
                        scores = scoreBuilder.build();
                        scoreBuilder = blockFactory.newDoubleVectorBuilder(Math.min(remainingDocs, maxPageSize));
                        page = new Page(currentPagePos, docBlock, scores.asBlock());
                    }
                } finally {
                    if (page == null) {
                        Releasables.closeExpectNoException(shard, leaf, docs, docBlock, scores);
                    }
                }
                currentPagePos = 0;
            }
            return page;
        } finally {
            processingNanos += System.nanoTime() - start;
        }
    }

    @Override
    public void close() {
        Releasables.close(docsBuilder, scoreBuilder);
    }

    @Override
    protected void describe(StringBuilder sb) {
        sb.append(", remainingDocs = ").append(remainingDocs);
    }
}
