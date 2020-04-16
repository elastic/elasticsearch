/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.MultiBucketCollector;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A specialization of {@link DeferringBucketCollector} that collects all
 * matches and then is able to replay a given subset of buckets which represent
 * the survivors from a pruning process performed by the aggregator that owns
 * this collector.
 */
public class BestBucketsDeferringCollector extends DeferringBucketCollector {
    static class Entry {
        final LeafReaderContext context;
        final PackedLongValues docDeltas;
        final PackedLongValues buckets;

        Entry(LeafReaderContext context, PackedLongValues docDeltas, PackedLongValues buckets) {
            this.context = Objects.requireNonNull(context);
            this.docDeltas = Objects.requireNonNull(docDeltas);
            this.buckets = Objects.requireNonNull(buckets);
        }
    }

    protected List<Entry> entries = new ArrayList<>();
    protected BucketCollector collector;
    protected final SearchContext searchContext;
    protected final boolean isGlobal;
    protected LeafReaderContext context;
    protected PackedLongValues.Builder docDeltasBuilder;
    protected PackedLongValues.Builder bucketsBuilder;
    protected long maxBucket = -1;
    protected boolean finished = false;
    protected LongHash selectedBuckets;

    /**
     * Sole constructor.
     * @param context The search context
     * @param isGlobal Whether this collector visits all documents (global context)
     */
    public BestBucketsDeferringCollector(SearchContext context, boolean isGlobal) {
        this.searchContext = context;
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
        this.collector = MultiBucketCollector.wrap(deferredCollectors);
    }

    private void finishLeaf() {
        if (context != null) {
            assert docDeltasBuilder != null && bucketsBuilder != null;
            entries.add(new Entry(context, docDeltasBuilder.build(), bucketsBuilder.build()));
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
        finishLeaf();

        context = null;
        // allocates the builder lazily in case this segment doesn't contain any match
        docDeltasBuilder = null;
        bucketsBuilder = null;

        return new LeafBucketCollector() {
            int lastDoc = 0;

            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (context == null) {
                    context = ctx;
                    docDeltasBuilder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
                    bucketsBuilder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
                }
                docDeltasBuilder.add(doc - lastDoc);
                bucketsBuilder.add(bucket);
                lastDoc = doc;
                maxBucket = Math.max(maxBucket, bucket);
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
    public void prepareSelectedBuckets(long... selectedBuckets) throws IOException {
        if (finished == false) {
            throw new IllegalStateException("Cannot replay yet, collection is not finished: postCollect() has not been called");
        }
        if (this.selectedBuckets != null) {
            throw new IllegalStateException("Already been replayed");
        }

        final LongHash hash = new LongHash(selectedBuckets.length, BigArrays.NON_RECYCLING_INSTANCE);
        for (long bucket : selectedBuckets) {
            hash.add(bucket);
        }
        this.selectedBuckets = hash;

        boolean needsScores = scoreMode().needsScores();
        Weight weight = null;
        if (needsScores) {
            Query query = isGlobal ? new MatchAllDocsQuery() : searchContext.query();
            weight = searchContext.searcher().createWeight(searchContext.searcher().rewrite(query), ScoreMode.COMPLETE, 1f);
        }

        for (Entry entry : entries) {
            assert entry.docDeltas.size() > 0 : "segment should have at least one document to replay, got 0";
            try {
                final LeafBucketCollector leafCollector = collector.getLeafCollector(entry.context);
                DocIdSetIterator scoreIt = null;
                if (needsScores) {
                    Scorer scorer = weight.scorer(entry.context);
                    // We don't need to check if the scorer is null
                    // since we are sure that there are documents to replay (entry.docDeltas it not empty).
                    scoreIt = scorer.iterator();
                    leafCollector.setScorer(scorer);
                }
                final PackedLongValues.Iterator docDeltaIterator = entry.docDeltas.iterator();
                final PackedLongValues.Iterator buckets = entry.buckets.iterator();
                int doc = 0;
                for (long i = 0, end = entry.docDeltas.size(); i < end; ++i) {
                    doc += docDeltaIterator.next();
                    final long bucket = buckets.next();
                    final long rebasedBucket = hash.find(bucket);
                    if (rebasedBucket != -1) {
                        if (needsScores) {
                            if (scoreIt.docID() < doc) {
                                scoreIt.advance(doc);
                            }
                            // aggregations should only be replayed on matching documents
                            assert scoreIt.docID() == doc;
                        }
                        leafCollector.collect(doc, rebasedBucket);
                    }
                }
            } catch (CollectionTerminatedException e) {
                // collection was terminated prematurely
                // continue with the following leaf
            }
        }
        collector.postCollection();
    }

    /**
     * Wrap the provided aggregator so that it behaves (almost) as if it had
     * been collected directly.
     */
    @Override
    public Aggregator wrap(final Aggregator in) {

        return new WrappedAggregator(in) {

            @Override
            public InternalAggregation buildAggregation(long bucket) throws IOException {
                if (selectedBuckets == null) {
                    throw new IllegalStateException("Collection has not been replayed yet.");
                }
                final long rebasedBucket = selectedBuckets.find(bucket);
                if (rebasedBucket == -1) {
                    throw new IllegalStateException("Cannot build for a bucket which has not been collected");
                }
                return in.buildAggregation(rebasedBucket);
            }

        };
    }

}
