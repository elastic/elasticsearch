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
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.lucene.search.ScoreMode.TOP_DOCS;
import static org.apache.lucene.search.ScoreMode.TOP_DOCS_WITH_SCORES;

/**
 * Source operator that builds Pages out of the output of a TopFieldCollector (aka TopN)
 */
public final class LuceneTopNSourceOperator extends LuceneOperator {
    public static class Factory extends LuceneOperator.Factory {
        private final int maxPageSize;
        private final List<SortBuilder<?>> sorts;

        public Factory(
            List<? extends ShardContext> contexts,
            Function<ShardContext, Query> queryFunction,
            DataPartitioning dataPartitioning,
            int taskConcurrency,
            int maxPageSize,
            int limit,
            List<SortBuilder<?>> sorts,
            boolean needsScore
        ) {
            super(
                contexts,
                queryFunction,
                dataPartitioning,
                query -> LuceneSliceQueue.PartitioningStrategy.SHARD,
                taskConcurrency,
                limit,
                needsScore,
                needsScore ? TOP_DOCS_WITH_SCORES : TOP_DOCS
            );
            this.maxPageSize = maxPageSize;
            this.sorts = sorts;
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new LuceneTopNSourceOperator(driverContext.blockFactory(), maxPageSize, sorts, limit, sliceQueue, needsScore);
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
                + ", needsScore = "
                + needsScore
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
    private final boolean needsScore;

    public LuceneTopNSourceOperator(
        BlockFactory blockFactory,
        int maxPageSize,
        List<SortBuilder<?>> sorts,
        int limit,
        LuceneSliceQueue sliceQueue,
        boolean needsScore
    ) {
        super(blockFactory, maxPageSize, sliceQueue);
        this.sorts = sorts;
        this.limit = limit;
        this.needsScore = needsScore;
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
                perShardCollector = newPerShardCollector(scorer.shardContext(), sorts, needsScore, limit);
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
                scoreDocs = perShardCollector.collector.topDocs().scoreDocs;
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
        DocBlock docBlock = null;
        DoubleBlock scores = null;
        Page page = null;
        try (
            IntVector.Builder currentSegmentBuilder = blockFactory.newIntVectorFixedBuilder(size);
            IntVector.Builder currentDocsBuilder = blockFactory.newIntVectorFixedBuilder(size);
            DoubleVector.Builder currentScoresBuilder = scoreVectorOrNull(size);
        ) {
            int start = offset;
            offset += size;
            List<LeafReaderContext> leafContexts = perShardCollector.shardContext.searcher().getLeafContexts();
            for (int i = start; i < offset; i++) {
                int doc = scoreDocs[i].doc;
                int segment = ReaderUtil.subIndex(doc, leafContexts);
                currentSegmentBuilder.appendInt(segment);
                currentDocsBuilder.appendInt(doc - leafContexts.get(segment).docBase); // the offset inside the segment
                if (currentScoresBuilder != null) {
                    float score = getScore(scoreDocs[i]);
                    currentScoresBuilder.appendDouble(score);
                }
            }

            shard = blockFactory.newConstantIntBlockWith(perShardCollector.shardContext.index(), size);
            segments = currentSegmentBuilder.build();
            docs = currentDocsBuilder.build();
            docBlock = new DocVector(shard.asVector(), segments, docs, null).asBlock();
            shard = null;
            segments = null;
            docs = null;
            if (currentScoresBuilder == null) {
                page = new Page(size, docBlock);
            } else {
                scores = currentScoresBuilder.build().asBlock();
                page = new Page(size, docBlock, scores);
            }
        } finally {
            if (page == null) {
                Releasables.closeExpectNoException(shard, segments, docs, docBlock, scores);
            }
        }
        return page;
    }

    private float getScore(ScoreDoc scoreDoc) {
        if (scoreDoc instanceof FieldDoc fieldDoc) {
            if (Float.isNaN(fieldDoc.score)) {
                if (sorts != null) {
                    return (Float) fieldDoc.fields[sorts.size() + 1];
                } else {
                    return (Float) fieldDoc.fields[0];
                }
            } else {
                return fieldDoc.score;
            }
        } else {
            return scoreDoc.score;
        }
    }

    private DoubleVector.Builder scoreVectorOrNull(int size) {
        if (needsScore) {
            return blockFactory.newDoubleVectorFixedBuilder(size);
        } else {
            return null;
        }
    }

    @Override
    protected void describe(StringBuilder sb) {
        sb.append(", limit = ").append(limit);
        sb.append(", needsScore = ").append(needsScore);
        String notPrettySorts = sorts.stream().map(Strings::toString).collect(Collectors.joining(","));
        sb.append(", sorts = [").append(notPrettySorts).append("]");
    }

    abstract static class PerShardCollector {
        private final ShardContext shardContext;
        private final TopDocsCollector<?> collector;
        private int leafIndex;
        private LeafCollector leafCollector;
        private Thread currentThread;

        PerShardCollector(ShardContext shardContext, TopDocsCollector<?> collector) {
            this.shardContext = shardContext;
            this.collector = collector;
        }

        LeafCollector getLeafCollector(LeafReaderContext leafReaderContext) throws IOException {
            if (currentThread != Thread.currentThread() || leafIndex != leafReaderContext.ord) {
                leafCollector = collector.getLeafCollector(leafReaderContext);
                leafIndex = leafReaderContext.ord;
                currentThread = Thread.currentThread();
            }
            return leafCollector;
        }
    }

    static final class NonScoringPerShardCollector extends PerShardCollector {
        NonScoringPerShardCollector(ShardContext shardContext, Sort sort, int limit) {
            // We don't use CollectorManager here as we don't retrieve the total hits and sort by score.
            super(shardContext, new TopFieldCollectorManager(sort, limit, null, 0, false).newCollector());
        }
    }

    static final class ScoringPerShardCollector extends PerShardCollector {
        ScoringPerShardCollector(ShardContext shardContext, TopDocsCollector<?> topDocsCollector) {
            super(shardContext, topDocsCollector);
        }
    }

    private static Function<ShardContext, Weight> weightFunction(
        Function<ShardContext, Query> queryFunction,
        List<SortBuilder<?>> sorts,
        boolean needsScore
    ) {
        return ctx -> {
            final var query = queryFunction.apply(ctx);
            final var searcher = ctx.searcher();
            try {
                // we create a collector with a limit of 1 to determine the appropriate score mode to use.
                var scoreMode = newPerShardCollector(ctx, sorts, needsScore, 1).collector.scoreMode();
                return searcher.createWeight(searcher.rewrite(query), scoreMode, 1);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    private static PerShardCollector newPerShardCollector(ShardContext context, List<SortBuilder<?>> sorts, boolean needsScore, int limit)
        throws IOException {
        Optional<SortAndFormats> sortAndFormats = context.buildSort(sorts);
        if (sortAndFormats.isEmpty()) {
            throw new IllegalStateException("sorts must not be disabled in TopN");
        }
        if (needsScore == false) {
            return new NonScoringPerShardCollector(context, sortAndFormats.get().sort, limit);
        }
        Sort sort = sortAndFormats.get().sort;
        if (Sort.RELEVANCE.equals(sort)) {
            // SORT _score DESC
            return new ScoringPerShardCollector(context, new TopScoreDocCollectorManager(limit, null, 0).newCollector());
        }

        // SORT ..., _score, ...
        var l = new ArrayList<>(Arrays.asList(sort.getSort()));
        l.add(SortField.FIELD_DOC);
        l.add(SortField.FIELD_SCORE);
        sort = new Sort(l.toArray(SortField[]::new));
        return new ScoringPerShardCollector(context, new TopFieldCollectorManager(sort, limit, null, 0).newCollector());
    }
}
