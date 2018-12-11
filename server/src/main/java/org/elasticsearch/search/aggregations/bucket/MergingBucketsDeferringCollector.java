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
import org.apache.lucene.search.DocIdSetIterator;
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

/**
 * A specialization of {@link DeferringBucketCollector} that collects all
 * matches and then is able to replay a given subset of buckets. Exposes
 * mergeBuckets, which can be invoked by the aggregator when increasing the
 * rounding interval.
 */
public class MergingBucketsDeferringCollector extends DeferringBucketCollector {

    List<Entry> entries = new ArrayList<>();
    BucketCollector collector;
    final SearchContext searchContext;
    LeafReaderContext context;
    PackedLongValues.Builder docDeltas;
    PackedLongValues.Builder buckets;
    long maxBucket = -1;
    boolean finished = false;
    LongHash selectedBuckets;

    public MergingBucketsDeferringCollector(SearchContext context) {
        this.searchContext = context;
    }

    @Override
    public void setDeferredCollector(Iterable<BucketCollector> deferredCollectors) {
        this.collector = MultiBucketCollector.wrap(deferredCollectors);
    }

    @Override
    public ScoreMode scoreMode() {
        if (collector == null) {
            throw new IllegalStateException();
        }
        return collector.scoreMode();
    }

    @Override
    public void preCollection() throws IOException {
        collector.preCollection();
    }

    private void finishLeaf() {
        if (context != null) {
            entries.add(new Entry(context, docDeltas.build(), buckets.build()));
        }
        context = null;
        docDeltas = null;
        buckets = null;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
        finishLeaf();

        context = ctx;
        docDeltas = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
        buckets = PackedLongValues.packedBuilder(PackedInts.DEFAULT);

        return new LeafBucketCollector() {
            int lastDoc = 0;

            @Override
            public void collect(int doc, long bucket) {
                docDeltas.add(doc - lastDoc);
                buckets.add(bucket);
                lastDoc = doc;
                maxBucket = Math.max(maxBucket, bucket);
            }
        };
    }

    public void mergeBuckets(long[] mergeMap) {

        List<Entry> newEntries = new ArrayList<>(entries.size());
        for (Entry sourceEntry : entries) {
            PackedLongValues.Builder newBuckets = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
            for (PackedLongValues.Iterator itr = sourceEntry.buckets.iterator(); itr.hasNext();) {
                long bucket = itr.next();
                newBuckets.add(mergeMap[Math.toIntExact(bucket)]);
            }
            newEntries.add(new Entry(sourceEntry.context, sourceEntry.docDeltas, newBuckets.build()));
        }
        entries = newEntries;

        // if there are buckets that have been collected in the current segment
        // we need to update the bucket ordinals there too
        if (buckets.size() > 0) {
            PackedLongValues currentBuckets = buckets.build();
            PackedLongValues.Builder newBuckets = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
            for (PackedLongValues.Iterator itr = currentBuckets.iterator(); itr.hasNext();) {
                long bucket = itr.next();
                newBuckets.add(mergeMap[Math.toIntExact(bucket)]);
            }
            buckets = newBuckets;
        }
    }

    @Override
    public void postCollection() {
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

        boolean needsScores = collector.scoreMode().needsScores();
        Weight weight = null;
        if (needsScores) {
            weight = searchContext.searcher().createWeight(
                    searchContext.searcher().rewrite(searchContext.query()),
                    ScoreMode.COMPLETE, 1f);
        }
        for (Entry entry : entries) {
            final LeafBucketCollector leafCollector = collector.getLeafCollector(entry.context);
            DocIdSetIterator docIt = null;
            if (needsScores && entry.docDeltas.size() > 0) {
                Scorer scorer = weight.scorer(entry.context);
                // We don't need to check if the scorer is null
                // since we are sure that there are documents to replay
                // (entry.docDeltas it not empty).
                docIt = scorer.iterator();
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
                        if (docIt.docID() < doc) {
                            docIt.advance(doc);
                        }
                        // aggregations should only be replayed on matching
                        // documents
                        assert docIt.docID() == doc;
                    }
                    leafCollector.collect(doc, rebasedBucket);
                }
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
                    throw new IllegalStateException("Cannot build for a bucket which has not been collected [" + bucket + "]");
                }
                return in.buildAggregation(rebasedBucket);
            }

        };
    }

    private static class Entry {
        final LeafReaderContext context;
        final PackedLongValues docDeltas;
        final PackedLongValues buckets;

        Entry(LeafReaderContext context, PackedLongValues docDeltas, PackedLongValues buckets) {
            this.context = context;
            this.docDeltas = docDeltas;
            this.buckets = buckets;
        }
    }

}
