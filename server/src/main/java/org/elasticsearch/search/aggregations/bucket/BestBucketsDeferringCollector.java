/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.MultiBucketCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.LongUnaryOperator;

/**
 * A specialization of {@link DeferringBucketCollector} that collects all
 * matches and then is able to replay a given subset of buckets which represent
 * the survivors from a pruning process performed by the aggregator that owns
 * this collector.
 */
public class BestBucketsDeferringCollector extends DeferringBucketCollector {
    private static class Entry {
        AggregationExecutionContext aggCtx;
        PackedLongValues docDeltas;
        PackedLongValues buckets;

        Entry(AggregationExecutionContext aggCtx, PackedLongValues docDeltas, PackedLongValues buckets) {
            this.aggCtx = Objects.requireNonNull(aggCtx);
            this.docDeltas = Objects.requireNonNull(docDeltas);
            this.buckets = Objects.requireNonNull(buckets);
        }
    }

    private final Query topLevelQuery;
    private final IndexSearcher searcher;
    private final boolean isGlobal;

    private List<Entry> entries = new ArrayList<>();
    private BucketCollector collector;
    private AggregationExecutionContext aggCtx;
    private PackedLongValues.Builder docDeltasBuilder;
    private PackedLongValues.Builder bucketsBuilder;
    private LongHash selectedBuckets;
    private boolean finished = false;

    /**
     * Sole constructor.
     * @param isGlobal Whether this collector visits all documents (global context)
     */
    public BestBucketsDeferringCollector(Query topLevelQuery, IndexSearcher searcher, boolean isGlobal) {
        this.topLevelQuery = topLevelQuery;
        this.searcher = searcher;
        this.isGlobal = isGlobal;
    }

    @Override
    public ScoreMode scoreMode() {
        if (collector == null) {
            throw new IllegalStateException();
        }
        return collector.scoreMode();
    }

    /** Set the deferred collectors. */
    @Override
    public void setDeferredCollector(Iterable<BucketCollector> deferredCollectors) {
        this.collector = MultiBucketCollector.wrap(true, deferredCollectors);
    }

    /**
     * Button up the builders for the current leaf.
     */
    private void finishLeaf() {
        if (aggCtx != null) {
            assert docDeltasBuilder != null && bucketsBuilder != null;
            assert docDeltasBuilder.size() > 0;
            entries.add(new Entry(aggCtx, docDeltasBuilder.build(), bucketsBuilder.build()));
            clearLeaf();
        }
    }

    /**
     * Clear the status for the current leaf.
     */
    private void clearLeaf() {
        aggCtx = null;
        docDeltasBuilder = null;
        bucketsBuilder = null;
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext context) throws IOException {
        finishLeaf();

        return new LeafBucketCollector() {
            int lastDoc = 0;

            @Override
            public void collect(int doc, long bucket) {
                if (aggCtx == null) {
                    aggCtx = context;
                    docDeltasBuilder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
                    bucketsBuilder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
                }
                docDeltasBuilder.add(doc - lastDoc);
                bucketsBuilder.add(bucket);
                lastDoc = doc;
            }
        };
    }

    @Override
    public void preCollection() throws IOException {
        collector.preCollection();
    }

    @Override
    public void postCollection() throws IOException {
        finishLeaf();
        finished = true;
    }

    /**
     * Replay the wrapped collector, but only on a selection of buckets.
     */
    @Override
    public void prepareSelectedBuckets(LongArray selectedBuckets) throws IOException {
        if (finished == false) {
            throw new IllegalStateException("Cannot replay yet, collection is not finished: postCollect() has not been called");
        }
        if (this.selectedBuckets != null) {
            throw new IllegalStateException("Already been replayed");
        }

        this.selectedBuckets = new LongHash(selectedBuckets.size(), BigArrays.NON_RECYCLING_INSTANCE);
        for (long i = 0; i < selectedBuckets.size(); i++) {
            this.selectedBuckets.add(selectedBuckets.get(i));
        }

        boolean needsScores = scoreMode().needsScores();
        Weight weight = null;
        if (needsScores) {
            Query query = isGlobal ? new MatchAllDocsQuery() : topLevelQuery;
            weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE, 1f);
        }

        for (Entry entry : entries) {
            assert entry.docDeltas.size() > 0 : "segment should have at least one document to replay, got 0";
            try {
                final LeafBucketCollector leafCollector = collector.getLeafCollector(entry.aggCtx);
                DocIdSetIterator scoreIt = null;
                if (needsScores) {
                    Scorer scorer = weight.scorer(entry.aggCtx.getLeafReaderContext());
                    if (scorer == null) {
                        failInCaseOfBadScorer("no scores are available");
                    }
                    // We don't need to check if the scorer is null
                    // since we are sure that there are documents to replay (entry.docDeltas it not empty).
                    scoreIt = scorer.iterator();
                    leafCollector.setScorer(scorer);
                }
                final PackedLongValues.Iterator docDeltaIterator = entry.docDeltas.iterator();
                final PackedLongValues.Iterator buckets = entry.buckets.iterator();
                int doc = 0;
                for (long i = 0, end = entry.docDeltas.size(); i < end; ++i) {
                    doc += (int) docDeltaIterator.next();
                    final long bucket = buckets.next();
                    final long rebasedBucket = this.selectedBuckets.find(bucket);
                    if (rebasedBucket != -1) {
                        if (needsScores) {
                            if (scoreIt.docID() < doc) {
                                scoreIt.advance(doc);
                            }
                            // aggregations should only be replayed on matching documents
                            if (scoreIt.docID() != doc) {
                                failInCaseOfBadScorer("score for different docid");
                            }
                        }
                        leafCollector.collect(doc, rebasedBucket);
                    }
                }
            } catch (CollectionTerminatedException e) {
                // collection was terminated prematurely
                // continue with the following leaf
            }
            // release resources
            entry.buckets = null;
            entry.docDeltas = null;
        }
        collector.postCollection();
    }

    /*
     * Fail with when no scores are available or a scorer for incorrect doc ids are used when replaying
     *
     * The likely cause is that a children aggregation has a terms aggregator with collection mode breadth_first and this
     * terms aggregator has a sub aggregation that requires score. See #37650
     * Sub aggregators of children aggregation can't access scores, because that information isn't kept track of by the children aggregator
     * when collect mode is breath first. Keeping track of this scores in breath first mode would require a non-trivial amount of heap
     * memory.
     */
    private static void failInCaseOfBadScorer(String message) {
        String likelyExplanation =
            "nesting an aggregation under a children aggregation and terms aggregation with collect mode breadth_first isn't possible";
        throw new RuntimeException(message + ", " + likelyExplanation);
    }

    /**
     * Wrap the provided aggregator so that it behaves (almost) as if it had
     * been collected directly.
     */
    @Override
    public Aggregator wrap(final Aggregator in, BigArrays bigArrays) {
        return new WrappedAggregator(in) {
            @Override
            public InternalAggregation[] buildAggregations(LongArray owningBucketOrds) throws IOException {
                if (selectedBuckets == null) {
                    throw new IllegalStateException("Collection has not been replayed yet.");
                }
                try (LongArray rebasedOrds = bigArrays.newLongArray(owningBucketOrds.size())) {
                    for (long ordIdx = 0; ordIdx < owningBucketOrds.size(); ordIdx++) {
                        rebasedOrds.set(ordIdx, selectedBuckets.find(owningBucketOrds.get(ordIdx)));
                        if (rebasedOrds.get(ordIdx) == -1) {
                            throw new IllegalStateException("Cannot build for a bucket which has not been collected");
                        }
                    }
                    return in.buildAggregations(rebasedOrds);
                }
            }
        };
    }

    /**
     * Merge or prune the selected buckets.
     * <p>
     * This process rebuilds some packed structures and is O(number_of_collected_docs) so
     * do your best to skip calling it unless you need it.
     *
     * @param howToRewrite a unary operator which maps a bucket's ordinal to the ordinal it has
     *   after this process. If a bucket's ordinal is mapped to -1 then the bucket is removed entirely.
     */
    public void rewriteBuckets(LongUnaryOperator howToRewrite) {
        List<Entry> newEntries = new ArrayList<>(entries.size());
        for (Entry sourceEntry : entries) {
            PackedLongValues.Builder newDocDeltas = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
            PackedLongValues.Iterator docDeltasItr = sourceEntry.docDeltas.iterator();

            PackedLongValues.Builder newBuckets = merge(howToRewrite, newDocDeltas, docDeltasItr, sourceEntry.buckets);
            // Only create an entry if this segment has buckets after merging
            if (newBuckets.size() > 0) {
                assert newDocDeltas.size() > 0 : "docDeltas was empty but we had buckets";
                newEntries.add(new Entry(sourceEntry.aggCtx, newDocDeltas.build(), newBuckets.build()));
            }
        }
        entries = newEntries;

        // if there are buckets that have been collected in the current segment
        // we need to update the bucket ordinals there too
        if (bucketsBuilder != null && bucketsBuilder.size() > 0) {
            PackedLongValues currentBuckets = bucketsBuilder.build();
            PackedLongValues.Builder newDocDeltas = PackedLongValues.packedBuilder(PackedInts.DEFAULT);

            // The current segment's deltas aren't built yet, so build to a temp object
            PackedLongValues currentDeltas = docDeltasBuilder.build();
            PackedLongValues.Iterator docDeltasItr = currentDeltas.iterator();

            PackedLongValues.Builder newBuckets = merge(howToRewrite, newDocDeltas, docDeltasItr, currentBuckets);
            if (newDocDeltas.size() == 0) {
                // We've decided not to keep *anything* in the current leaf so we should just pitch our state.
                clearLeaf();
            } else {
                docDeltasBuilder = newDocDeltas;
                bucketsBuilder = newBuckets;
            }
        }
    }

    private static PackedLongValues.Builder merge(
        LongUnaryOperator howToRewrite,
        PackedLongValues.Builder newDocDeltas,
        PackedLongValues.Iterator docDeltasItr,
        PackedLongValues currentBuckets
    ) {
        PackedLongValues.Builder newBuckets = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
        long lastGoodDelta = 0;
        var itr = currentBuckets.iterator();
        while (itr.hasNext()) {
            long bucket = itr.next();
            assert docDeltasItr.hasNext();
            long delta = docDeltasItr.next();

            // Only merge in the ordinal if it hasn't been "removed", signified with -1
            long ordinal = howToRewrite.applyAsLong(bucket);

            if (ordinal != -1) {
                newBuckets.add(ordinal);
                newDocDeltas.add(delta + lastGoodDelta);
                lastGoodDelta = 0;
            } else {
                // we are skipping this ordinal, which means we need to accumulate the
                // doc delta's since the last "good" delta
                lastGoodDelta += delta;
            }
        }
        return newBuckets;
    }
}
