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

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.RoaringDocIdSet;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.DeferringBucketCollector;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A specialization of {@link DeferringBucketCollector} that collects all
 * matches and then, in a second pass, replays the documents that contain a top bucket
 * selected during the first pass.
 */
final class BestCompositeBucketsDeferringCollector extends DeferringBucketCollector {
    private static class Entry {
        final LeafReaderContext context;
        final DocIdSet docIdSet;

        Entry(LeafReaderContext context, DocIdSet docIdSet) {
            this.context = context;
            this.docIdSet = docIdSet;
        }
    }

    private final SearchContext searchContext;
    private final CompositeValuesCollectorQueue queue;
    private final boolean isCollectionSorted;
    private final List<Entry> entries = new ArrayList<>();

    private BucketCollector collector;
    private LeafReaderContext context;
    private RoaringDocIdSet.Builder sortedBuilder;
    private DocIdSetBuilder builder;
    private boolean finished = false;

    /**
     * Sole constructor.
     * @param context The search context.
     * @param queue The queue that is used to record the top composite buckets.
     * @param isCollectionSorted true if the parent aggregator will pass documents sorted by doc_id.
     */
    BestCompositeBucketsDeferringCollector(SearchContext context, CompositeValuesCollectorQueue queue, boolean isCollectionSorted) {
        this.searchContext = context;
        this.queue = queue;
        this.isCollectionSorted = isCollectionSorted;
    }

    @Override
    public boolean needsScores() {
        assert collector != null;
        return collector.needsScores();
    }

    /** Set the deferred collectors. */
    @Override
    public void setDeferredCollector(Iterable<BucketCollector> deferredCollectors) {
        this.collector = BucketCollector.wrap(deferredCollectors);
    }

    private void finishLeaf() {
        if (context != null) {
            DocIdSet docIdSet = isCollectionSorted ? sortedBuilder.build() : builder.build();
            entries.add(new Entry(context, docIdSet));
        }
        context = null;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
        finishLeaf();

        context = ctx;
        if (isCollectionSorted) {
            sortedBuilder = new RoaringDocIdSet.Builder(ctx.reader().maxDoc());
        } else {
            builder = new DocIdSetBuilder(ctx.reader().maxDoc());
        }

        return new LeafBucketCollector() {
            int lastDoc = -1;
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (lastDoc != doc) {
                    if (isCollectionSorted) {
                        sortedBuilder.add(doc);
                    } else {
                        builder.grow(1).add(doc);
                    }
                }
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

    @Override
    public void prepareSelectedBuckets(long... selectedBuckets) throws IOException {
        // the selected buckets are extracted directly from the queue
        assert selectedBuckets.length == 0;
        if (!finished) {
            throw new IllegalStateException("Cannot replay yet, collection is not finished: postCollect() has not been called");
        }

        final boolean needsScores = needsScores();
        Weight weight = null;
        if (needsScores) {
            Query query = searchContext.query();
            weight = searchContext.searcher().createNormalizedWeight(query, true);
        }
        for (Entry entry : entries) {
            DocIdSetIterator docIdSetIterator = entry.docIdSet.iterator();
            if (docIdSetIterator == null) {
                continue;
            }
            final LeafBucketCollector subCollector = collector.getLeafCollector(entry.context);
            final LeafBucketCollector collector =
                queue.getLeafCollector(entry.context, getSecondPassCollector(subCollector));
            DocIdSetIterator scorerIt = null;
            if (needsScores) {
                Scorer scorer = weight.scorer(entry.context);
                // We don't need to check if the scorer is null
                // since we are sure that there are documents to replay (docIdSetIterator it not empty).
                scorerIt = scorer.iterator();
                subCollector.setScorer(scorer);
            }
            int docID;
            while ((docID = docIdSetIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (needsScores) {
                    assert scorerIt.docID() < docID;
                    scorerIt.advance(docID);
                    // aggregations should only be replayed on matching documents
                    assert scorerIt.docID() == docID;
                }
                collector.collect(docID);
            }
        }
        collector.postCollection();
    }

    /**
     * Replay the top buckets from the matching documents.
     */
    private LeafBucketCollector getSecondPassCollector(LeafBucketCollector subCollector) {
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                Integer slot = queue.compareCurrent();
                if (slot != null) {
                    // The candidate key is a top bucket.
                    // We can defer the collection of this document/bucket to the sub collector
                    subCollector.collect(doc, slot);
                }
            }
        };
    }
}
