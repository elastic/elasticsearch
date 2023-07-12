/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Weight;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Source operator that incrementally runs Lucene searches
 */
public class LuceneSourceOperator extends LuceneOperator {

    private int numCollectedDocs = 0;

    private final int maxCollectedDocs;

    private IntVector.Builder currentDocsBuilder;

    public static class LuceneSourceOperatorFactory extends LuceneOperatorFactory {

        public LuceneSourceOperatorFactory(
            List<SearchContext> searchContexts,
            Function<SearchContext, Query> queryFunction,
            DataPartitioning dataPartitioning,
            int taskConcurrency,
            int maxPageSize,
            int limit
        ) {
            super(searchContexts, queryFunction, dataPartitioning, taskConcurrency, maxPageSize, limit);
        }

        @Override
        LuceneOperator luceneOperatorForShard(int shardIndex) {
            final SearchContext ctx = searchContexts.get(shardIndex);
            final Query query = queryFunction.apply(ctx);
            return new LuceneSourceOperator(ctx.getSearchExecutionContext().getIndexReader(), shardIndex, query, maxPageSize, limit);
        }

        @Override
        public String describe() {
            return "LuceneSourceOperator[dataPartitioning = " + dataPartitioning + ", limit = " + limit + "]";
        }
    }

    public LuceneSourceOperator(IndexReader reader, int shardId, Query query, int maxPageSize, int limit) {
        super(reader, shardId, query, maxPageSize);
        this.currentDocsBuilder = IntVector.newVectorBuilder(maxPageSize);
        this.maxCollectedDocs = limit;
    }

    LuceneSourceOperator(Weight weight, int shardId, List<PartialLeafReaderContext> leaves, int maxPageSize, int maxCollectedDocs) {
        super(weight, shardId, leaves, maxPageSize);
        this.currentDocsBuilder = IntVector.newVectorBuilder(maxPageSize);
        this.maxCollectedDocs = maxCollectedDocs;
    }

    @Override
    LuceneOperator docSliceLuceneOperator(List<PartialLeafReaderContext> slice) {
        return new LuceneSourceOperator(weight, shardId, slice, maxPageSize, maxCollectedDocs);
    }

    @Override
    LuceneOperator segmentSliceLuceneOperator(IndexSearcher.LeafSlice leafSlice) {
        return new LuceneSourceOperator(
            weight,
            shardId,
            Arrays.asList(leafSlice.leaves).stream().map(PartialLeafReaderContext::new).collect(Collectors.toList()),
            maxPageSize,
            maxCollectedDocs
        );
    }

    @Override
    public boolean isFinished() {
        return currentLeaf >= leaves.size() || numCollectedDocs >= maxCollectedDocs;
    }

    @Override
    public Page getOutput() {
        if (isFinished()) {
            return null;
        }

        // initialize weight if not done yet
        initializeWeightIfNecessary();

        // if there are documents matching, initialize currentLeafReaderContext, currentScorer, and currentScorerPos when we switch
        // to a new leaf reader, otherwise return
        if (maybeReturnEarlyOrInitializeScorer()) {
            return null;
        }

        Page page = null;

        try {
            currentScorerPos = currentScorer.score(new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {
                    // ignore
                }

                @Override
                public void collect(int doc) {
                    if (numCollectedDocs < maxCollectedDocs) {
                        currentDocsBuilder.appendInt(doc);
                        numCollectedDocs++;
                        currentPagePos++;
                    }
                }
            },
                currentLeafReaderContext.leafReaderContext.reader().getLiveDocs(),
                currentScorerPos,
                // Note: if (maxPageSize - currentPagePos) is a small "remaining" interval, this could lead to slow collection with a
                // highly selective filter. Having a large "enough" difference between max- and minPageSize (and thus currentPagePos)
                // alleviates this issue.
                Math.min(currentLeafReaderContext.maxDoc, currentScorerPos + maxPageSize - currentPagePos)
            );

            if (currentPagePos >= minPageSize
                || currentScorerPos >= currentLeafReaderContext.maxDoc
                || numCollectedDocs >= maxCollectedDocs) {
                page = new Page(
                    currentPagePos,
                    new DocVector(
                        IntBlock.newConstantBlockWith(shardId, currentPagePos).asVector(),
                        IntBlock.newConstantBlockWith(currentLeafReaderContext.leafReaderContext.ord, currentPagePos).asVector(),
                        currentDocsBuilder.build(),
                        true
                    ).asBlock()
                );
                currentDocsBuilder = IntVector.newVectorBuilder(maxPageSize);
                currentPagePos = 0;
            }

            if (currentScorerPos >= currentLeafReaderContext.maxDoc) {
                currentLeaf++;
                currentLeafReaderContext = null;
                currentScorer = null;
                currentScorerPos = 0;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        pagesEmitted++;
        return page;
    }
}
