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
import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DisjunctionDISIApproximation;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Rescorer;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class LuceneRetrieveOperator extends LuceneOperator {
    private final int limit;
    private final List<Tuple<Rescorer, Integer>> rescorers;
    private final RetrieveFeatureExtractor featureExtractor;

    protected LuceneRetrieveOperator(
        BlockFactory blockFactory,
        int maxPageSize,
        LuceneSliceQueue sliceQueue,
        int limit,
        List<Tuple<Rescorer, Integer>> rescorers,
        List<Tuple<String, Query>> features
    ) {
        super(blockFactory, maxPageSize, sliceQueue);
        this.limit = limit;
        this.rescorers = rescorers; // TODO should this be moved to LuceneSliceQueue?
        this.featureExtractor = new RetrieveFeatureExtractor(features);
    }

    public static class Factory implements LuceneOperator.Factory {
        private final DataPartitioning dataPartitioning;
        private final int taskConcurrency;
        private final int maxPageSize;
        private final int limit;
        private final LuceneSliceQueue sliceQueue;
        private final List<Tuple<Rescorer, Integer>> rescorers;
        private final List<Tuple<String, Query>> features;

        public Factory(
            List<? extends ShardContext> contexts,
            Function<ShardContext, Query> queryFunction,
            List<Tuple<Rescorer, Integer>> rescorers,
            List<Tuple<String, Query>> features,
            DataPartitioning dataPartitioning,
            int taskConcurrency,
            int maxPageSize,
            int limit
        ) {
            this.maxPageSize = maxPageSize;
            this.limit = limit;
            this.dataPartitioning = dataPartitioning;
            var weightFunction = weightFunction(queryFunction);
            this.sliceQueue = LuceneSliceQueue.create(contexts, weightFunction, dataPartitioning, taskConcurrency);
            this.taskConcurrency = Math.min(sliceQueue.totalSlices(), taskConcurrency);
            this.rescorers = rescorers;
            this.features = features;
        }


        @Override
        public String describe() {
            return null;
        }

        @Override
        public int taskConcurrency() {
            return 0;
        }

        public int maxPageSize() {
            return maxPageSize;
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new LuceneRetrieveOperator(driverContext.blockFactory(), maxPageSize, sliceQueue, limit, rescorers, features);
        }

        static Function<ShardContext, Weight> weightFunction(Function<ShardContext, Query> queryFunction) {
            return ctx -> {
                final var query = queryFunction.apply(ctx);
                final var searcher = ctx.searcher();
                try {
                    return searcher.createWeight(query, ScoreMode.COMPLETE, 1);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            };
        }
    }

    private ScoreDoc[] scoreDocs;
    private int offset = 0;

    private PerShardCollector perShardCollector;

    @Override
    protected void describe(StringBuilder sb) {

    }

    @Override
    public void finish() {
        doneCollecting = true;
        scoreDocs = null;
        assert isFinished();
    }

    private boolean isEmitting() {
        return scoreDocs != null && offset < scoreDocs.length;
    }

    @Override
    public boolean isFinished() {
        return doneCollecting && isEmitting() == false;
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

    private void runRescorersAndSetScoreDocs() {
        TopDocs topDocs = perShardCollector.topFieldCollector.topDocs();
        for(Tuple<Rescorer, Integer> rescorerConfig: rescorers) {
            Rescorer rescorer = rescorerConfig.v1();
            int windowSize = rescorerConfig.v2();
            try {
                topDocs = rescorer.rescore(perShardCollector.shardContext.searcher(), topDocs, windowSize);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        scoreDocs = topDocs.scoreDocs;
    }

    private Page emit(boolean startEmitting) {
        if (startEmitting) {
            assert isEmitting() == false : "offset=" + offset + " score_docs=" + Arrays.toString(scoreDocs);
            offset = 0;
            if (perShardCollector != null) {
                runRescorersAndSetScoreDocs();
            } else {
                scoreDocs = new ScoreDoc[0];
            }
            featureExtractor.setNextIterator(getCurrentLeafContext());
        }

        if (offset >= scoreDocs.length) {
            return null;
        }
        int size = Math.min(maxPageSize, scoreDocs.length - offset);
        IntBlock shard = null;
        IntVector segments = null;
        IntVector docs = null;
        Page page = null;
        DoubleVector scores = null;
        List<Map<String, Float>> extractedFeaturesList = new ArrayList<>(size);
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
                float score = getScore(scoreDocs[i]);
                currentSegmentBuilder.appendInt(segment);
                currentDocsBuilder.appendInt(doc - leafContexts.get(segment).docBase); // the offset inside the segment
                currentScoresBuilder.appendDouble(score);
                extractedFeaturesList.add(featureExtractor.getFeatures(doc));
            }

            shard = blockFactory.newConstantIntBlockWith(perShardCollector.shardContext.index(), size);
            segments = currentSegmentBuilder.build();
            docs = currentDocsBuilder.build();
            scores = currentScoresBuilder.build();
            page = new Page(size, new DocVector(shard.asVector(), segments, docs, scores, extractedFeaturesList, null).asBlock());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            if (page == null) {
                Releasables.closeExpectNoException(shard, segments, docs, scores);
            }
        }

        pagesEmitted++;
        return page;
    }

    private float getScore(ScoreDoc scoreDoc) {
        FieldDoc fieldDoc = (FieldDoc) scoreDoc;
        if (Float.isNaN(fieldDoc.score)) {
            return (float) fieldDoc.fields[0];
        } else {
            return fieldDoc.score;
        }
    }

    class RetrieveFeatureExtractor {
        private final List<Tuple<String, Query>> features;
        private final List<Scorer> scorers;
        private DisjunctionDISIApproximation rankerIterator;

        public RetrieveFeatureExtractor(List<Tuple<String, Query>> features) {
            this.features = features;
            this.scorers = new ArrayList<>(features.size());
        }

        public int featureSize() { return features.size(); }

        private void setNextIterator(LeafReaderContext currentShardContext) {
            scorers.clear();
            if (features.size() == 0 ) { return; }

            DisiPriorityQueue disiPriorityQueue = new DisiPriorityQueue(features.size());
            try {
                for (Tuple<String, Query> feature : features) {
                    var weight = feature.v2().createWeight(perShardCollector.shardContext.searcher(), ScoreMode.COMPLETE, 1);
                    Scorer featureScorer = weight.scorer(currentShardContext);
                    if (featureScorer != null) {
                        disiPriorityQueue.add(new DisiWrapper(featureScorer));
                    }
                    scorers.add(featureScorer);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            rankerIterator = new DisjunctionDISIApproximation(disiPriorityQueue);
        }

        private Map<String, Float> getFeatures(int docId) throws IOException {
            if (features.size() == 0) {
                return null;
            }
            Map<String, Float> featureMap = Maps.newMapWithExpectedSize(features.size());
            rankerIterator.advance(docId);
            for(int i = 0; i < features.size(); i++) {
                Scorer scorer = scorers.get(i);
                if (scorer != null && scorer.docID() == docId) {
                    featureMap.put(features.get(i).v1(), scorer.score());
                }
            }
            return featureMap;
        }
    }

    static final class PerShardCollector {
        private final ShardContext shardContext;
        private final TopFieldCollector topFieldCollector;
        private int leafIndex;
        private LeafCollector leafCollector;
        private Thread currentThread;

        PerShardCollector(ShardContext shardContext, int limit) throws IOException {
            this.shardContext = shardContext;
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
}
