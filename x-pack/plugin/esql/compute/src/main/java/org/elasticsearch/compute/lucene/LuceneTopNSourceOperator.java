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
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.Strings;
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
public class LuceneTopNSourceOperator extends LuceneOperator {

    private Thread currentThread;

    private final TopFieldCollector topFieldCollector;// this should only be created via the collector manager

    private LeafCollector currentLeafCollector;

    private final List<LeafReaderContext> leafReaderContexts;

    private final CollectorManager<TopFieldCollector, TopFieldDocs> collectorManager;// one for each shard

    private LeafReaderContext previousLeafReaderContext;

    /**
     * Collected docs. {@code null} until we're {@link #doneCollecting}.
     */
    private ScoreDoc[] scoreDocs;
    /**
     * The offset in {@link #scoreDocs} of the next page.
     */
    private int offset = 0;

    public LuceneTopNSourceOperator(IndexReader reader, int shardId, Sort sort, Query query, int maxPageSize, int limit) {
        super(reader, shardId, query, maxPageSize);
        this.leafReaderContexts = reader.leaves();
        this.collectorManager = TopFieldCollector.createSharedManager(sort, limit, null, 0);
        try {
            this.topFieldCollector = collectorManager.newCollector();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.currentThread = Thread.currentThread();
    }

    private LuceneTopNSourceOperator(
        Weight weight,
        int shardId,
        List<PartialLeafReaderContext> leaves,
        List<LeafReaderContext> leafReaderContexts,
        CollectorManager<TopFieldCollector, TopFieldDocs> collectorManager,
        Thread currentThread,
        int maxPageSize
    ) {
        super(weight, shardId, leaves, maxPageSize);
        this.leafReaderContexts = leafReaderContexts;
        this.collectorManager = collectorManager;
        try {
            this.topFieldCollector = collectorManager.newCollector();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.currentThread = currentThread;
    }

    public static class LuceneTopNSourceOperatorFactory extends LuceneOperatorFactory {

        private final List<SortBuilder<?>> sorts;

        public LuceneTopNSourceOperatorFactory(
            List<SearchContext> searchContexts,
            Function<SearchContext, Query> queryFunction,
            DataPartitioning dataPartitioning,
            int taskConcurrency,
            int maxPageSize,
            int limit,
            List<SortBuilder<?>> sorts
        ) {
            super(searchContexts, queryFunction, dataPartitioning, taskConcurrency, maxPageSize, limit);
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
                sort,
                query,
                maxPageSize,
                limit
            );
        }

        @Override
        public String describe() {
            String notPrettySorts = sorts.stream().map(s -> Strings.toString(s)).collect(Collectors.joining(","));
            return "LuceneTopNSourceOperator[dataPartitioning = "
                + dataPartitioning
                + ", maxPageSize = "
                + maxPageSize
                + ", limit = "
                + limit
                + ", sorts = ["
                + notPrettySorts
                + "]]";
        }

    }

    @Override
    LuceneOperator docSliceLuceneOperator(List<PartialLeafReaderContext> slice) {
        return new LuceneTopNSourceOperator(weight, shardId, slice, leafReaderContexts, collectorManager, currentThread, maxPageSize);
    }

    @Override
    LuceneOperator segmentSliceLuceneOperator(IndexSearcher.LeafSlice leafSlice) {
        return new LuceneTopNSourceOperator(
            weight,
            shardId,
            Arrays.asList(leafSlice.leaves).stream().map(PartialLeafReaderContext::new).collect(Collectors.toList()),
            leafReaderContexts,
            collectorManager,
            currentThread,
            maxPageSize
        );
    }

    @Override
    void initializeWeightIfNecessary() {
        if (weight == null) {
            try {
                IndexSearcher indexSearcher = new IndexSearcher(indexReader);
                weight = indexSearcher.createWeight(indexSearcher.rewrite(new ConstantScoreQuery(query)), ScoreMode.TOP_DOCS, 1);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    protected boolean doneCollecting() {
        return currentLeaf >= leaves.size();
    }

    private boolean doneEmitting() {
        /*
         * If there aren't any leaves then we never initialize scoreDocs.
         */
        return leaves.isEmpty() || offset >= scoreDocs.length;
    }

    @Override
    public boolean isFinished() {
        return doneCollecting() && doneEmitting();
    }

    @Override
    public Page getOutput() {
        if (doneCollecting()) {
            return emit();
        }
        return collect();
    }

    private Page collect() {
        assert false == doneCollecting();
        // initialize weight if not done yet
        initializeWeightIfNecessary();

        // if there are documents matching, initialize currentLeafReaderContext and currentScorer when we switch to a new group in the slice
        if (maybeReturnEarlyOrInitializeScorer()) {
            // if there are no more documents matching and we reached the final slice, build the Page
            scoreDocs = topFieldCollector.topDocs().scoreDocs;
            return emit();
        }

        try {
            // one leaf collector per thread and per segment/leaf
            if (currentLeafCollector == null
                || currentThread.equals(Thread.currentThread()) == false
                || previousLeafReaderContext != currentLeafReaderContext.leafReaderContext) {
                currentLeafCollector = topFieldCollector.getLeafCollector(currentLeafReaderContext.leafReaderContext);
                currentThread = Thread.currentThread();
                previousLeafReaderContext = currentLeafReaderContext.leafReaderContext;
            }

            try {
                currentScorerPos = currentScorer.score(
                    currentLeafCollector,
                    currentLeafReaderContext.leafReaderContext.reader().getLiveDocs(),
                    currentScorerPos,
                    Math.min(currentLeafReaderContext.maxDoc, currentScorerPos + maxPageSize)
                );
            } catch (CollectionTerminatedException cte) {
                // Lucene terminated early the collection (doing topN for an index that's sorted and the topN uses the same sorting)
                currentScorerPos = currentLeafReaderContext.maxDoc;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (currentScorerPos >= currentLeafReaderContext.maxDoc) {
            // move to the next leaf if we are done reading from the current leaf (current scorer position reached the final doc)
            currentLeaf++;
            currentLeafReaderContext = null;
            currentScorer = null;
            currentScorerPos = 0;
        }
        if (doneCollecting()) {
            // we reached the final leaf in this slice/operator, build the single Page this operator should create
            scoreDocs = topFieldCollector.topDocs().scoreDocs;
            return emit();
        }
        return null;
    }

    private Page emit() {
        assert doneCollecting();
        if (doneEmitting()) {
            return null;
        }
        int size = Math.min(maxPageSize, scoreDocs.length - offset);
        IntVector.Builder currentSegmentBuilder = IntVector.newVectorBuilder(size);
        IntVector.Builder currentDocsBuilder = IntVector.newVectorBuilder(size);

        int start = offset;
        offset += size;
        for (int i = start; i < offset; i++) {
            int doc = scoreDocs[i].doc;
            int segment = ReaderUtil.subIndex(doc, leafReaderContexts);
            currentSegmentBuilder.appendInt(segment);
            currentDocsBuilder.appendInt(doc - leafReaderContexts.get(segment).docBase); // the offset inside the segment
        }

        pagesEmitted++;
        return new Page(
            size,
            new DocVector(
                IntBlock.newConstantBlockWith(shardId, size).asVector(),
                currentSegmentBuilder.build(),
                currentDocsBuilder.build(),
                null
            ).asBlock()
        );
    }
}
