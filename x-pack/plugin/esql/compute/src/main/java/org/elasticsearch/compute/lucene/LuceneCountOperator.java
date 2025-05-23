/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/**
 * Source operator that incrementally counts the results in Lucene searches
 * Returns always one entry that mimics the Count aggregation internal state:
 * 1. the count as a long (0 if no doc is seen)
 * 2. a bool flag (seen) that's always true meaning that the group (all items) always exists
 */
public class LuceneCountOperator extends LuceneOperator {

    private static final int PAGE_SIZE = 1;

    private int totalHits = 0;
    private int remainingDocs;

    private final LeafCollector leafCollector;

    public static class Factory extends LuceneOperator.Factory {

        public Factory(
            List<? extends ShardContext> contexts,
            Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFunction,
            DataPartitioning dataPartitioning,
            int taskConcurrency,
            int limit
        ) {
            super(
                contexts,
                queryFunction,
                dataPartitioning,
                query -> LuceneSliceQueue.PartitioningStrategy.SHARD,
                taskConcurrency,
                limit,
                false,
                ScoreMode.COMPLETE_NO_SCORES
            );
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new LuceneCountOperator(driverContext.blockFactory(), sliceQueue, limit);
        }

        @Override
        public String describe() {
            return "LuceneCountOperator[dataPartitioning = " + dataPartitioning + ", limit = " + limit + "]";
        }
    }

    public LuceneCountOperator(BlockFactory blockFactory, LuceneSliceQueue sliceQueue, int limit) {
        super(blockFactory, PAGE_SIZE, sliceQueue);
        this.remainingDocs = limit;
        this.leafCollector = new LeafCollector() {
            @Override
            public void setScorer(Scorable scorer) {}

            @Override
            public void collect(DocIdStream stream) throws IOException {
                if (remainingDocs > 0) {
                    int count = Math.min(stream.count(), remainingDocs);
                    totalHits += count;
                    remainingDocs -= count;
                }
            }

            @Override
            public void collect(int doc) {
                if (remainingDocs > 0) {
                    remainingDocs--;
                    totalHits++;
                }
            }
        };
    }

    @Override
    public boolean isFinished() {
        return doneCollecting || remainingDocs == 0;
    }

    @Override
    public void finish() {
        doneCollecting = true;
    }

    @Override
    protected Page getCheckedOutput() throws IOException {
        if (isFinished()) {
            assert remainingDocs <= 0 : remainingDocs;
            return null;
        }
        long start = System.nanoTime();
        try {
            final LuceneScorer scorer = getCurrentOrLoadNextScorer();
            // no scorer means no more docs
            if (scorer == null) {
                remainingDocs = 0;
            } else {
                if (scorer.tags().isEmpty() == false) {
                    throw new UnsupportedOperationException("extra not supported by " + getClass());
                }
                Weight weight = scorer.weight();
                var leafReaderContext = scorer.leafReaderContext();
                // see org.apache.lucene.search.TotalHitCountCollector
                int leafCount = weight.count(leafReaderContext);
                if (leafCount != -1) {
                    // make sure to NOT multi count as the count _shortcut_ (which is segment wide)
                    // handle doc partitioning where the same leaf can be seen multiple times
                    // since the count is global, consider it only for the first partition and skip the rest
                    // SHARD, SEGMENT and the first DOC_ reader in data partitioning contain the first doc (position 0)
                    if (scorer.position() == 0) {
                        // check to not count over the desired number of docs/limit
                        var count = Math.min(leafCount, remainingDocs);
                        totalHits += count;
                        remainingDocs -= count;
                    }
                    scorer.markAsDone();
                } else {
                    // could not apply shortcut, trigger the search
                    // TODO: avoid iterating all documents in multiple calls to make cancellation more responsive.
                    scorer.scoreNextRange(leafCollector, leafReaderContext.reader().getLiveDocs(), remainingDocs);
                }
            }

            Page page = null;
            // emit only one page
            if (remainingDocs <= 0 && pagesEmitted == 0) {
                LongBlock count = null;
                BooleanBlock seen = null;
                try {
                    count = blockFactory.newConstantLongBlockWith(totalHits, PAGE_SIZE);
                    seen = blockFactory.newConstantBooleanBlockWith(true, PAGE_SIZE);
                    page = new Page(PAGE_SIZE, count, seen);
                } finally {
                    if (page == null) {
                        Releasables.closeExpectNoException(count, seen);
                    }
                }
            }
            return page;
        } finally {
            processingNanos += System.nanoTime() - start;
        }
    }

    @Override
    protected void describe(StringBuilder sb) {
        sb.append(", remainingDocs=").append(remainingDocs);
    }
}
