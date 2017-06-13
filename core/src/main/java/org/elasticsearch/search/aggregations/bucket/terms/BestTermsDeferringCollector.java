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

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.RoaringDocIdSet;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.DeferringBucketCollector;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * A specialization of {@link DeferringBucketCollector} for {@link TermsAggregator} that
 * first records all matches in a {@link RoaringDocIdSet} and then replay the best buckets
 * selected by the aggregator that owns this collector.
 * The second pass uses a {@link TermsAggregator.BucketSelectorValuesSource} to collect
 * documents that belongs to the selected buckets.
 */
public final class BestTermsDeferringCollector extends DeferringBucketCollector {
    private static class LeafEntry {
        final LeafReaderContext context;
        final RoaringDocIdSet.Builder builder;

        private LeafEntry(LeafReaderContext context) {
            this.context = context;
            this.builder = new RoaringDocIdSet.Builder(context.reader().maxDoc());
        }
    }

    private final SearchContext searchContext;
    private final List<LeafEntry> leaves = new ArrayList<>();
    private BucketValuesSourceProvider provider;
    private LongHash bestBucketHash;

    interface BucketValuesSourceProvider {
        TermsAggregator.BucketSelectorValuesSource get(long... buckets);
    }

    public BestTermsDeferringCollector(BucketCollector deferredCollector,
                                       SearchContext searchContext,
                                       BucketValuesSourceProvider provider) {
        super(deferredCollector);
        this.searchContext = searchContext;
        this.provider = provider;
    }

    @Override
    public void replaySelectedBuckets(long... selectedBuckets) throws IOException {
        final LongHash hash = new LongHash(selectedBuckets.length, BigArrays.NON_RECYCLING_INSTANCE);
        for (long bucket : selectedBuckets) {
            hash.add(bucket);
        }
        this.bestBucketHash = hash;
        TermsAggregator.BucketSelectorValuesSource valuesSource = provider.get(selectedBuckets);

        boolean needsScores = deferredCollector.needsScores();
        Weight weight = null;
        if (needsScores) {
            weight = searchContext.searcher().createNormalizedWeight(searchContext.query(), true);
        }
        for (LeafEntry leafEntry : leaves) {
            RoaringDocIdSet docIdSet = leafEntry.builder.build();
            DocIdSetIterator it = docIdSet.iterator();
            if (it == null) {
                continue;
            }
            final LeafBucketCollector leafCollector = deferredCollector.getLeafCollector(leafEntry.context);
            final SortedNumericDocValues dvs = valuesSource.get(leafEntry.context);
            DocIdSetIterator docIt = null;
            if (needsScores) {
                Scorer scorer = weight.scorer(leafEntry.context);
                // We don't need to check if the scorer is null
                // since we are sure that there are documents to replay.
                docIt = scorer.iterator();
                leafCollector.setScorer(scorer);
            }
            int docId;
            while ((docId = it.nextDoc()) != NO_MORE_DOCS) {
                if (dvs.advanceExact(docId)) {
                    if (needsScores) {
                        if (docIt.docID() < docId) {
                            docIt.advance(docId);
                        }
                        // aggregations should only be replayed on matching documents
                        assert docIt.docID() == docId;
                    }
                    for (int i = 0; i < dvs.docValueCount(); i++) {
                        long bucket = dvs.nextValue();
                        leafCollector.collect(docId, bucket);
                    }
                }
            }
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
        final LeafEntry entry = new LeafEntry(ctx);
        leaves.add(entry);
        return new LeafBucketCollector() {
            int lastDoc = -1;
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (doc != lastDoc) {
                    entry.builder.add(doc);
                    lastDoc = doc;
                }
            }
        };
    }

    @Override
    public boolean needsScores() {
        return false;
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
                if (bestBucketHash == null) {
                    throw new IllegalStateException("Collection has not been replayed yet.");
                }
                final long rebasedBucket = bestBucketHash.find(bucket);
                if (rebasedBucket == -1) {
                    throw new IllegalStateException("Cannot build for a bucket which has not been collected");
                }
                return in.buildAggregation(rebasedBucket);
            }
        };
    }
}
