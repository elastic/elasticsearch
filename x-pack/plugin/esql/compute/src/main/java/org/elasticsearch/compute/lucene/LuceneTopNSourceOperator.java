/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Source operator that builds Pages out of the output of a TopFieldCollector (aka TopN)
 */
@Experimental
public class LuceneTopNSourceOperator extends LuceneOperator {

    private TopFieldCollector currentTopFieldCollector;

    private LeafCollector currentTopFieldLeafCollector;

    private IntVector.Builder currentSegmentBuilder;

    private final List<LeafReaderContext> leafReaderContexts;

    private final Sort sort;

    private final CollectorManager<TopFieldCollector, TopFieldDocs> collectorManager;

    public LuceneTopNSourceOperator(IndexReader reader, int shardId, Query query, int maxPageSize, int limit, Sort sort) {
        super(reader, shardId, query, maxPageSize, limit);
        this.currentSegmentBuilder = IntVector.newVectorBuilder(maxPageSize);
        this.leafReaderContexts = reader.leaves();
        this.sort = sort;
        this.collectorManager = TopFieldCollector.createSharedManager(sort, maxCollectedDocs, null, 0);
    }

    private LuceneTopNSourceOperator(
        Weight weight,
        int shardId,
        List<PartialLeafReaderContext> leaves,
        List<LeafReaderContext> leafReaderContexts,
        CollectorManager<TopFieldCollector, TopFieldDocs> collectorManager,
        int maxPageSize,
        int maxCollectedDocs,
        Sort sort
    ) {
        super(weight, shardId, leaves, maxPageSize, maxCollectedDocs);
        this.currentSegmentBuilder = IntVector.newVectorBuilder(maxPageSize);
        this.leafReaderContexts = leafReaderContexts;
        this.collectorManager = collectorManager;
        this.sort = sort;
    }

    public static class LuceneTopNSourceOperatorFactory extends LuceneOperatorFactory {

        private final List<SortBuilder<?>> sorts;

        public LuceneTopNSourceOperatorFactory(
            List<SearchContext> searchContexts,
            Function<SearchContext, Query> queryFunction,
            DataPartitioning dataPartitioning,
            int taskConcurrency,
            int limit,
            List<SortBuilder<?>> sorts
        ) {
            super(searchContexts, queryFunction, dataPartitioning, taskConcurrency, limit);
            assert sorts != null;
            this.sorts = sorts;
        }

        @Override
        LuceneOperator luceneOperatorForShard(int shardIndex) {
            final SearchContext ctx = searchContexts.get(shardIndex);
            final Query query = queryFunction.apply(ctx);
            Sort sort = null;
            try {
                Optional<SortAndFormats> optionalSort = SortBuilder.buildSort(sorts, ctx.getSearchExecutionContext());
                if (optionalSort.isPresent()) {
                    sort = optionalSort.get().sort;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return new LuceneTopNSourceOperator(
                ctx.getSearchExecutionContext().getIndexReader(),
                shardIndex,
                query,
                maxPageSize,
                limit,
                sort
            );
        }

        @Override
        public String describe() {
            String notPrettySorts = sorts.stream().map(s -> Strings.toString(s)).collect(Collectors.joining(","));
            return "LuceneTopNSourceOperator(dataPartitioning = "
                + dataPartitioning
                + ", limit = "
                + limit
                + ", sorts = ["
                + notPrettySorts
                + "])";
        }

    }

    @Override
    LuceneOperator docSliceLuceneOperator(List<PartialLeafReaderContext> slice) {
        return new LuceneTopNSourceOperator(
            weight,
            shardId,
            slice,
            leafReaderContexts,
            collectorManager,
            maxPageSize,
            maxCollectedDocs,
            sort
        );
    }

    @Override
    LuceneOperator segmentSliceLuceneOperator(IndexSearcher.LeafSlice leafSlice) {
        return new LuceneTopNSourceOperator(
            weight,
            shardId,
            Arrays.asList(leafSlice.leaves).stream().map(PartialLeafReaderContext::new).collect(Collectors.toList()),
            leafReaderContexts,
            collectorManager,
            maxPageSize,
            maxCollectedDocs,
            sort
        );
    }

    @Override
    public boolean isFinished() {
        return currentLeaf >= leaves.size();
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
        boolean terminatedEarly = false;

        try {
            if (currentTopFieldCollector == null) {
                currentTopFieldCollector = collectorManager.newCollector();
                currentTopFieldLeafCollector = currentTopFieldCollector.getLeafCollector(currentLeafReaderContext.leafReaderContext);
            }
            try {
                currentScorerPos = currentScorer.score(
                    currentTopFieldLeafCollector,
                    currentLeafReaderContext.leafReaderContext.reader().getLiveDocs(),
                    currentScorerPos,
                    Math.min(currentLeafReaderContext.maxDoc, currentScorerPos + maxPageSize - currentPagePos)
                );
            } catch (CollectionTerminatedException cte) {
                // Lucene terminated early the collection (doing topN for an index that's sorted and the topN uses the same sorting)
                // make sure to move to the next leaf (topDocs can be called only once) or create a new collector for the rest of the docs
                terminatedEarly = true;
            }
            TopFieldDocs topFieldDocs = currentTopFieldCollector.topDocs();
            for (ScoreDoc doc : topFieldDocs.scoreDocs) {
                int segment = ReaderUtil.subIndex(doc.doc, leafReaderContexts);
                currentSegmentBuilder.appendInt(segment);
                currentBlockBuilder.appendInt(doc.doc - leafReaderContexts.get(segment).docBase); // the offset inside the segment
                currentPagePos++;
            }

            if (terminatedEarly || currentPagePos >= minPageSize || currentScorerPos >= currentLeafReaderContext.maxDoc) {
                page = new Page(
                    currentPagePos,
                    new DocVector(
                        IntBlock.newConstantBlockWith(shardId, currentPagePos).asVector(),
                        currentSegmentBuilder.build(),
                        currentBlockBuilder.build(),
                        null
                    ).asBlock()
                );
                currentBlockBuilder = IntVector.newVectorBuilder(maxPageSize);
                currentSegmentBuilder = IntVector.newVectorBuilder(maxPageSize);
                currentPagePos = 0;
            }

            if (terminatedEarly || currentScorerPos >= currentLeafReaderContext.maxDoc) {
                currentLeaf++;
                currentLeafReaderContext = null;
                currentScorer = null;
                currentScorerPos = 0;
                currentTopFieldCollector = null;
                currentTopFieldLeafCollector = null;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        pagesEmitted++;
        return page;
    }
}
