/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.sampler;

import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.MultiBucketCollector;
import org.elasticsearch.search.aggregations.bucket.DeferringBucketCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * A specialization of {@link DeferringBucketCollector} that collects all
 * matches and then replays only the top scoring documents to child
 * aggregations. The method
 * {@link BestDocsDeferringCollector#createTopDocsCollector(int)} is designed to
 * be overridden and allows subclasses to choose a custom collector
 * implementation for determining the top N matches.
 */
public class BestDocsDeferringCollector extends DeferringBucketCollector implements Releasable {
    private final List<PerSegmentCollects> entries = new ArrayList<>();
    private BucketCollector deferred;
    private ObjectArray<PerParentBucketSamples> perBucketSamples;
    private final int shardSize;
    private PerSegmentCollects perSegCollector;
    private final BigArrays bigArrays;
    private final Consumer<Long> circuitBreakerConsumer;

    private static final long SENTINEL_SIZE = RamUsageEstimator.shallowSizeOfInstance(Object.class);

    /**
     * Sole constructor.
     *
     * @param shardSize The number of top-scoring docs to collect for each bucket
     * @param circuitBreakerConsumer consumer for tracking runtime bytes in request circuit breaker
     */
    BestDocsDeferringCollector(int shardSize, BigArrays bigArrays, Consumer<Long> circuitBreakerConsumer) {
        this.shardSize = shardSize;
        this.bigArrays = bigArrays;
        this.circuitBreakerConsumer = circuitBreakerConsumer;
        perBucketSamples = bigArrays.newObjectArray(1);
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE;
    }

    /** Set the deferred collectors. */
    @Override
    public void setDeferredCollector(Iterable<BucketCollector> deferredCollectors) {
        this.deferred = MultiBucketCollector.wrap(true, deferredCollectors);
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx) throws IOException {
        perSegCollector = new PerSegmentCollects(aggCtx);
        entries.add(perSegCollector);

        // Deferring collector
        return new LeafBucketCollector() {
            @Override
            public void setScorer(Scorable scorer) throws IOException {
                perSegCollector.setScorer(scorer);
            }

            @Override
            public void collect(int doc, long bucket) throws IOException {
                perSegCollector.collect(doc, bucket);
            }
        };
    }

    // Designed to be overridden by subclasses that may score docs by criteria
    // other than Lucene score
    protected TopDocsCollector<? extends ScoreDoc> createTopDocsCollector(int size) throws IOException {
        return new TopScoreDocCollectorManager(size, null, Integer.MAX_VALUE, false).newCollector();
    }

    // Can be overridden by subclasses that have a different priority queue implementation
    // and need different memory sizes
    protected long getPriorityQueueSlotSize() {
        // Generic sentinel object
        return SENTINEL_SIZE;
    }

    @Override
    public void preCollection() throws IOException {
        deferred.preCollection();
    }

    @Override
    public void postCollection() throws IOException {
        runDeferredAggs();
    }

    @Override
    public void prepareSelectedBuckets(long... selectedBuckets) throws IOException {
        // no-op - deferred aggs processed in postCollection call
    }

    private void runDeferredAggs() throws IOException {
        // ScoreDoc is 12b ([float + int + int])
        circuitBreakerConsumer.accept(12L * shardSize);
        try {
            List<ScoreDoc> allDocs = new ArrayList<>(shardSize);
            for (int i = 0; i < perBucketSamples.size(); i++) {
                PerParentBucketSamples perBucketSample = perBucketSamples.get(i);
                if (perBucketSample == null) {
                    continue;
                }
                perBucketSample.getMatches(allDocs);
            }

            // Sort the top matches by docID for the benefit of deferred collector
            allDocs.sort((o1, o2) -> {
                if (o1.doc == o2.doc) {
                    return o1.shardIndex - o2.shardIndex;
                }
                return o1.doc - o2.doc;
            });
            try {
                for (PerSegmentCollects perSegDocs : entries) {
                    perSegDocs.replayRelatedMatches(allDocs);
                }
            } catch (IOException e) {
                throw new ElasticsearchException("IOException collecting best scoring results", e);
            }
        } finally {
            // done with allDocs now, reclaim some memory
            circuitBreakerConsumer.accept(-12L * shardSize);
        }
        deferred.postCollection();
    }

    class PerParentBucketSamples {
        private LeafCollector currentLeafCollector;
        private TopDocsCollector<? extends ScoreDoc> tdc;
        private long parentBucket;
        private int matchedDocs;

        PerParentBucketSamples(long parentBucket, Scorable scorer, AggregationExecutionContext aggCtx) {
            try {
                this.parentBucket = parentBucket;

                // Add to CB based on the size and the implementations per-doc overhead
                circuitBreakerConsumer.accept((long) shardSize * getPriorityQueueSlotSize());

                tdc = createTopDocsCollector(shardSize);
                currentLeafCollector = tdc.getLeafCollector(aggCtx.getLeafReaderContext());
                setScorer(scorer);
            } catch (IOException e) {
                throw new ElasticsearchException("IO error creating collector", e);
            }
        }

        public void getMatches(List<ScoreDoc> allDocs) {
            TopDocs topDocs = tdc.topDocs();
            ScoreDoc[] sd = topDocs.scoreDocs;
            matchedDocs = sd.length;
            for (ScoreDoc scoreDoc : sd) {
                // A bit of a hack to (ab)use shardIndex property here to
                // hold a bucket ID but avoids allocating extra data structures
                // and users should have bigger concerns if bucket IDs
                // exceed int capacity..
                scoreDoc.shardIndex = (int) parentBucket;
            }
            allDocs.addAll(Arrays.asList(sd));
        }

        public void collect(int doc) throws IOException {
            currentLeafCollector.collect(doc);
        }

        public void setScorer(Scorable scorer) throws IOException {
            currentLeafCollector.setScorer(scorer);
        }

        public void changeSegment(AggregationExecutionContext aggCtx) throws IOException {
            currentLeafCollector = tdc.getLeafCollector(aggCtx.getLeafReaderContext());
        }

        public int getDocCount() {
            return matchedDocs;
        }
    }

    class PerSegmentCollects extends Scorable {
        private final AggregationExecutionContext aggCtx;
        int maxDocId = Integer.MIN_VALUE;
        private float currentScore;
        private Scorable currentScorer;

        PerSegmentCollects(AggregationExecutionContext aggCtx) throws IOException {
            // The publisher behaviour for Reader/Scorer listeners triggers a
            // call to this constructor with a null scorer so we can't call
            // scorer.getWeight() and pass the Weight to our base class.
            // However, passing null seems to have no adverse effects here...
            this.aggCtx = aggCtx;
            for (int i = 0; i < perBucketSamples.size(); i++) {
                PerParentBucketSamples perBucketSample = perBucketSamples.get(i);
                if (perBucketSample == null) {
                    continue;
                }
                perBucketSample.changeSegment(aggCtx);
            }
        }

        public void setScorer(Scorable scorer) throws IOException {
            this.currentScorer = scorer;
            for (int i = 0; i < perBucketSamples.size(); i++) {
                PerParentBucketSamples perBucketSample = perBucketSamples.get(i);
                if (perBucketSample == null) {
                    continue;
                }
                perBucketSample.setScorer(scorer);
            }
        }

        public void replayRelatedMatches(List<ScoreDoc> sd) throws IOException {
            try {
                final LeafBucketCollector leafCollector = deferred.getLeafCollector(aggCtx);
                leafCollector.setScorer(this);

                currentScore = 0;
                if (maxDocId < 0) {
                    return;
                }
                for (ScoreDoc scoreDoc : sd) {
                    // Doc ids from TopDocCollector are root-level Reader so
                    // need rebasing
                    int rebased = scoreDoc.doc - aggCtx.getLeafReaderContext().docBase;
                    if ((rebased >= 0) && (rebased <= maxDocId)) {
                        currentScore = scoreDoc.score;
                        // We stored the bucket ID in Lucene's shardIndex property
                        // for convenience.
                        leafCollector.collect(rebased, scoreDoc.shardIndex);
                    }
                }
            } catch (CollectionTerminatedException e) {
                // collection was terminated prematurely
                // continue with the following leaf
            }
        }

        @Override
        public float score() throws IOException {
            return currentScore;
        }

        public void collect(int docId, long parentBucket) throws IOException {
            perBucketSamples = bigArrays.grow(perBucketSamples, parentBucket + 1);
            PerParentBucketSamples sampler = perBucketSamples.get((int) parentBucket);
            if (sampler == null) {
                sampler = new PerParentBucketSamples(parentBucket, currentScorer, aggCtx);
                perBucketSamples.set((int) parentBucket, sampler);
            }
            sampler.collect(docId);
            maxDocId = Math.max(maxDocId, docId);
        }

    }

    public int getDocCount(long parentBucket) {
        if (perBucketSamples.size() <= parentBucket) {
            return 0;
        }
        PerParentBucketSamples sampler = perBucketSamples.get((int) parentBucket);
        if (sampler == null) {
            // There are conditions where no docs are collected and the aggs
            // framework still asks for doc count.
            return 0;
        }
        return sampler.getDocCount();
    }

    @Override
    public void close() throws ElasticsearchException {
        Releasables.close(perBucketSamples);
    }

}
