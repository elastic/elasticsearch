/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Function;

/**
 * Source operator that incrementally counts the results in Lucene searches
 */
public class LuceneCountOperator extends LuceneOperator {

    private int totalHits = 0;
    private int remainingDocs;

    private IntVector.Builder docsBuilder;
    private final LeafCollector leafCollector;

    public static class Factory implements LuceneOperator.Factory {
        private final DataPartitioning dataPartitioning;
        private final int taskConcurrency;
        private final int limit;
        private final LuceneSliceQueue sliceQueue;

        public Factory(
            List<SearchContext> searchContexts,
            Function<SearchContext, Query> queryFunction,
            DataPartitioning dataPartitioning,
            int taskConcurrency,
            int limit
        ) {
            this.limit = limit;
            this.dataPartitioning = dataPartitioning;
            var weightFunction = weightFunction(queryFunction, ScoreMode.COMPLETE_NO_SCORES);
            this.sliceQueue = LuceneSliceQueue.create(searchContexts, weightFunction, dataPartitioning, taskConcurrency);
            this.taskConcurrency = Math.min(sliceQueue.totalSlices(), taskConcurrency);
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new LuceneCountOperator(sliceQueue, limit);
        }

        @Override
        public int taskConcurrency() {
            return taskConcurrency;
        }

        public int limit() {
            return limit;
        }

        @Override
        public String describe() {
            return "LuceneCountOperator[dataPartitioning = "
                + dataPartitioning
                + ", limit = "
                + limit
                + "]";
        }
    }

    public LuceneCountOperator(LuceneSliceQueue sliceQueue, int limit) {
        super(Integer.MAX_VALUE, sliceQueue);
        this.remainingDocs = limit;
        this.leafCollector = new LeafCollector() {
            @Override
            public void setScorer(Scorable scorer) {}

            @Override
            public void collect(int doc) {
                if (remainingDocs > 0) {
                    --remainingDocs;
                    totalHits++;
                }
            }
        };
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
    public Page getOutput() {
        if (isFinished()) {
            assert remainingDocs == 0 : remainingDocs;
            return null;
        }
        try {
            final LuceneScorer scorer = getCurrentOrLoadNextScorer();
            if (scorer == null) {
                return null;
            }
            Weight weight = scorer.weight;
            var leafReaderContext = scorer.leafReaderContext();
            // see org.apache.lucene.search.TotalHitCountCollector
            int leafCount = weight == null ? -1 : weight.count(leafReaderContext);
            if (leafCount != -1) {
                // check to not go over limit
                var count = Math.min(leafCount, remainingDocs);
                totalHits += count;
                remainingDocs -= count;
            } else {
                scorer.scoreNextRange(
                    leafCollector,
                    leafReaderContext.reader().getLiveDocs(),
                    remainingDocs
                );
            }

            Page page = null;
            if (remainingDocs <= 0 || scorer.isDone()) {
                pagesEmitted++;
                page = new Page(1, IntBlock.newConstantBlockWith(totalHits, 1));
            }
            return page;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
