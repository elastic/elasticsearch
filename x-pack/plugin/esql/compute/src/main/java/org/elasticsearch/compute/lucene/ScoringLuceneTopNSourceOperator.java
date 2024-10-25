/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A {@link LuceneTopNSourceOperator} that provides scores.
 */
public final class ScoringLuceneTopNSourceOperator extends LuceneTopNSourceOperator {

    public static class Factory extends LuceneTopNSourceOperator.Factory {

        public Factory(
            List<? extends ShardContext> contexts,
            Function<ShardContext, Query> queryFunction,
            DataPartitioning dataPartitioning,
            int taskConcurrency,
            int maxPageSize,
            int limit,
            List<SortBuilder<?>> sorts
        ) {
            super(contexts, queryFunction, dataPartitioning, taskConcurrency, maxPageSize, limit, sorts, ScoreMode.COMPLETE);
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new ScoringLuceneTopNSourceOperator(driverContext.blockFactory(), maxPageSize, sorts, limit, sliceQueue);
        }

        @Override
        public String describe() {
            String notPrettySorts = sorts.stream().map(Strings::toString).collect(Collectors.joining(","));
            return "ScoringLuceneTopNSourceOperator[dataPartitioning = "
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

    public ScoringLuceneTopNSourceOperator(
        BlockFactory blockFactory,
        int maxPageSize,
        List<SortBuilder<?>> sorts,
        int limit,
        LuceneSliceQueue sliceQueue
    ) {
        super(blockFactory, maxPageSize, sorts, limit, sliceQueue);
    }

    @Override
    protected DoubleVector.Builder scoreVectorOrNull(int size) {
        return blockFactory.newDoubleVectorFixedBuilder(size);
    }

    @Override
    protected void consumeScore(ScoreDoc scoreDoc, DoubleVector.Builder currentScoresBuilder) {
        if (currentScoresBuilder != null) {
            float score = getScore(scoreDoc);
            currentScoresBuilder.appendDouble(score);
        }
    }

    @Override
    protected Page maybeAppendScore(Page page, DoubleVector.Builder currentScoresBuilder) {
        return page.appendBlocks(new Block[] { currentScoresBuilder.build().asBlock() });
    }

    float getScore(ScoreDoc scoreDoc) {
        if (scoreDoc instanceof FieldDoc fieldDoc) {
            if (Float.isNaN(fieldDoc.score)) {
                if (sorts != null) {
                    return (Float) fieldDoc.fields[sorts.size()];
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

    @Override
    PerShardCollector newPerShardCollector(ShardContext shardContext, List<SortBuilder<?>> sorts, int limit) throws IOException {
        Optional<SortAndFormats> sortAndFormats = shardContext.buildSort(sorts);
        Sort sort;
        if (sortAndFormats.isPresent()) {
            var l = new ArrayList<>(Arrays.asList(sortAndFormats.get().sort.getSort()));
            l.add(SortField.FIELD_SCORE);
            sort = new Sort(l.toArray(SortField[]::new));
        } else {
            sort = null;
        }
        return new ScoringPerShardCollector(shardContext, sort, limit);
    }

    static class ScoringPerShardCollector extends PerShardCollector {

        // TODO : make this configurable / inferrable?
        private static final int MAX_HITS = 100_000;
        private static final int TOTAL_HITS_THRESHOLD = 100;

        ScoringPerShardCollector(ShardContext shardContext, Sort sort, int limit) {
            this.shardContext = shardContext;
            if (sort == null) {
                this.collector = new UnsortedScoreCollector(new PriorityQueue<>(Math.min(limit, MAX_HITS)) {
                    @Override
                    protected boolean lessThan(ScoreDoc a, ScoreDoc b) {
                        return a.doc > b.doc;
                    }
                });
            } else if (sort.needsScores()) {
                this.collector = new TopScoreDocCollectorManager(Math.min(limit, MAX_HITS), TOTAL_HITS_THRESHOLD).newCollector();
            } else {
                this.collector = new TopFieldCollectorManager(sort, Math.min(limit, MAX_HITS), TOTAL_HITS_THRESHOLD).newCollector();
            }
        }
    }

    private static class UnsortedScoreCollector extends TopDocsCollector<ScoreDoc> {

        protected UnsortedScoreCollector(PriorityQueue<ScoreDoc> pq) {
            super(pq);
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            return new LeafCollector() {
                private Scorable scorable;

                @Override
                public void setScorer(Scorable scorable) {
                    this.scorable = scorable;
                }

                @Override
                public void collect(int docID) throws IOException {
                    float score = scorable.score();
                    pq.add(new ScoreDoc(docID, score));
                    totalHits++;
                }
            };
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE;
        }
    }
}
