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
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Source operator that builds Pages out of the output of a TopFieldCollector (aka TopN)
 */
public final class LuceneTopNSourceOperator extends LuceneOperator {
    public static final class Factory extends LuceneOperator.Factory {
        private final int maxPageSize;
        private final List<SortBuilder<?>> sorts;

        public Factory(
            List<? extends ShardContext> contexts,
            Function<ShardContext, Query> queryFunction,
            DataPartitioning dataPartitioning,
            int taskConcurrency,
            int maxPageSize,
            int limit,
            List<SortBuilder<?>> sorts
        ) {
            super(contexts, queryFunction, dataPartitioning, taskConcurrency, limit, ScoreMode.TOP_DOCS);
            this.maxPageSize = maxPageSize;
            this.sorts = sorts;
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new LuceneTopNSourceOperator(driverContext.blockFactory(), maxPageSize, sorts, limit, sliceQueue);
        }

        public int maxPageSize() {
            return maxPageSize;
        }

        @Override
        public String describe() {
            String notPrettySorts = sorts.stream().map(Strings::toString).collect(Collectors.joining(","));
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

    /**
     * Collected docs. {@code null} until we're {@link #emit(boolean)}.
     */
    private ScoreDoc[] scoreDocs;
    /**
     * The offset in {@link #scoreDocs} of the next page.
     */
    private int offset = 0;

    private PerShardCollector perShardCollector;
    private final List<SortBuilder<?>> sorts;
    private final int limit;

    public LuceneTopNSourceOperator(
        BlockFactory blockFactory,
        int maxPageSize,
        List<SortBuilder<?>> sorts,
        int limit,
        LuceneSliceQueue sliceQueue
    ) {
        super(blockFactory, maxPageSize, sliceQueue);
        this.sorts = sorts;
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
    public Page getCheckedOutput() throws IOException {
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

    private Page collect() throws IOException {
        assert doneCollecting == false;
        var scorer = getCurrentOrLoadNextScorer();
        if (scorer == null) {
            doneCollecting = true;
            return emit(true);
        }
        try {
            if (perShardCollector == null || perShardCollector.shardContext.index() != scorer.shardContext().index()) {
                // TODO: share the bottom between shardCollectors
                perShardCollector = new PerShardCollector(scorer.shardContext(), sorts, limit);
            }
            var leafCollector = perShardCollector.getLeafCollector(scorer.leafReaderContext());
            scorer.scoreNextRange(leafCollector, scorer.leafReaderContext().reader().getLiveDocs(), maxPageSize);
        } catch (CollectionTerminatedException cte) {
            // Lucene terminated early the collection (doing topN for an index that's sorted and the topN uses the same sorting)
            scorer.markAsDone();
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
        Page page = null;
        try (
            IntVector.Builder currentSegmentBuilder = blockFactory.newIntVectorFixedBuilder(size);
            IntVector.Builder currentDocsBuilder = blockFactory.newIntVectorFixedBuilder(size)
        ) {
            int start = offset;
            offset += size;
            List<LeafReaderContext> leafContexts = perShardCollector.shardContext.searcher().getLeafContexts();
            for (int i = start; i < offset; i++) {
                int doc = scoreDocs[i].doc;
                int segment = ReaderUtil.subIndex(doc, leafContexts);
                currentSegmentBuilder.appendInt(segment);
                currentDocsBuilder.appendInt(doc - leafContexts.get(segment).docBase); // the offset inside the segment
            }

            shard = blockFactory.newConstantIntBlockWith(perShardCollector.shardContext.index(), size);
            segments = currentSegmentBuilder.build();
            docs = currentDocsBuilder.build();
            page = new Page(size, new DocVector(shard.asVector(), segments, docs, null).asBlock());
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
        sb.append(", limit = ").append(limit);
        String notPrettySorts = sorts.stream().map(Strings::toString).collect(Collectors.joining(","));
        sb.append(", sorts = [").append(notPrettySorts).append("]");
    }

    static final class PerShardCollector {
        private final ShardContext shardContext;
        private final TopFieldCollector topFieldCollector;
        private int leafIndex;
        private LeafCollector leafCollector;
        private Thread currentThread;

        PerShardCollector(ShardContext shardContext, List<SortBuilder<?>> sorts, int limit) throws IOException {
            this.shardContext = shardContext;
            Optional<SortAndFormats> sortAndFormats = shardContext.buildSort(sorts);
            if (sortAndFormats.isEmpty()) {
                throw new IllegalStateException("sorts must not be disabled in TopN");
            }

            // We don't use CollectorManager here as we don't retrieve the total hits and sort by score.
            this.topFieldCollector = new TopFieldCollectorManager(sortAndFormats.get().sort, limit, null, 0, false).newCollector();
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
}
