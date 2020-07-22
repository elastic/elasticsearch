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
package org.elasticsearch.join.aggregations;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * An aggregator that joins documents based on global ordinals.
 * Global ordinals that match the main query and the <code>inFilter</code> query are replayed
 * with documents matching the <code>outFilter</code> query.
 */
public abstract class ParentJoinAggregator extends BucketsAggregator implements SingleBucketAggregator {
    private final Weight inFilter;
    private final Weight outFilter;
    private final ValuesSource.Bytes.WithOrdinals valuesSource;

    /**
     * Strategy for collecting results.
     */
    private final CollectionStrategy collectionStrategy;

    public ParentJoinAggregator(String name,
                                    AggregatorFactories factories,
                                    SearchContext context,
                                    Aggregator parent,
                                    Query inFilter,
                                    Query outFilter,
                                    ValuesSource.Bytes.WithOrdinals valuesSource,
                                    long maxOrd,
                                    CardinalityUpperBound cardinality,
                                    Map<String, Object> metadata) throws IOException {
        /*
         * We have to use MANY to work around
         * https://github.com/elastic/elasticsearch/issues/59097
         */
        super(name, factories, context, parent, CardinalityUpperBound.MANY, metadata);

        if (maxOrd > Integer.MAX_VALUE) {
            throw new IllegalStateException("the number of parent [" + maxOrd + "] + is greater than the allowed limit " +
                "for this aggregation: " + Integer.MAX_VALUE);
        }

        // these two filters are cached in the parser
        this.inFilter = context.searcher().createWeight(context.searcher().rewrite(inFilter), ScoreMode.COMPLETE_NO_SCORES, 1f);
        this.outFilter = context.searcher().createWeight(context.searcher().rewrite(outFilter), ScoreMode.COMPLETE_NO_SCORES, 1f);
        this.valuesSource = valuesSource;
        boolean singleAggregator = parent == null;
        collectionStrategy = singleAggregator && cardinality == CardinalityUpperBound.ONE
            ? new DenseCollectionStrategy(maxOrd, context.bigArrays())
            : new SparseCollectionStrategy(context.bigArrays(), cardinality);
    }

    @Override
    public final LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final SortedSetDocValues globalOrdinals = valuesSource.globalOrdinalsValues(ctx);
        final Bits parentDocs = Lucene.asSequentialAccessBits(ctx.reader().maxDoc(), inFilter.scorerSupplier(ctx));
        return new LeafBucketCollector() {
            @Override
            public void collect(int docId, long owningBucketOrd) throws IOException {
                if (parentDocs.get(docId) && globalOrdinals.advanceExact(docId)) {
                    int globalOrdinal = (int) globalOrdinals.nextOrd();
                    assert globalOrdinal != -1 && globalOrdinals.nextOrd() == SortedSetDocValues.NO_MORE_ORDS;
                    collectionStrategy.add(owningBucketOrd, globalOrdinal);
                }
            }
        };
    }

    @Override
    protected void beforeBuildingBuckets(long[] ordsToCollect) throws IOException {
        IndexReader indexReader = context().searcher().getIndexReader();
        for (LeafReaderContext ctx : indexReader.leaves()) {
            Scorer childDocsScorer = outFilter.scorer(ctx);
            if (childDocsScorer == null) {
                continue;
            }
            DocIdSetIterator childDocsIter = childDocsScorer.iterator();

            final LeafBucketCollector sub = collectableSubAggregators.getLeafCollector(ctx);

            final SortedSetDocValues globalOrdinals = valuesSource.globalOrdinalsValues(ctx);
            // Set the scorer, since we now replay only the child docIds
            sub.setScorer(new Scorable() {
                @Override
                public float score() {
                    return 1f;
                }

                @Override
                public int docID() {
                    return childDocsIter.docID();
                }
            });

            final Bits liveDocs = ctx.reader().getLiveDocs();
            for (int docId = childDocsIter.nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = childDocsIter.nextDoc()) {
                if (liveDocs != null && liveDocs.get(docId) == false) {
                    continue;
                }
                if (false == globalOrdinals.advanceExact(docId)) {
                    continue;
                }
                int globalOrdinal = (int) globalOrdinals.nextOrd();
                assert globalOrdinal != -1 && globalOrdinals.nextOrd() == SortedSetDocValues.NO_MORE_ORDS;
                /*
                 * Check if we contain every ordinal. It's almost certainly be
                 * faster to replay all the matching ordinals and filter them down
                 * to just those listed in ordsToCollect, but we don't have a data
                 * structure that maps a primitive long to a list of primitive
                 * longs. 
                 */
                for (long owningBucketOrd: ordsToCollect) {
                    if (collectionStrategy.exists(owningBucketOrd, globalOrdinal)) {
                        collectBucket(sub, docId, owningBucketOrd);
                    }
                }
            }
        }
    }

    @Override
    protected void doClose() {
        Releasables.close(collectionStrategy);
    }

    /**
     * Strategy for collecting the global ordinals of the join field in for all
     * docs that match the {@code ParentJoinAggregator#inFilter} and then
     * checking if which of the docs in the
     * {@code ParentJoinAggregator#outFilter} also have the ordinal.
     */
    protected interface CollectionStrategy extends Releasable {
        void add(long owningBucketOrd, int globalOrdinal);
        boolean exists(long owningBucketOrd, int globalOrdinal);
    }

    /**
     * Uses a dense, bit per ordinal representation of the join field in the
     * docs that match {@code ParentJoinAggregator#inFilter}. Its memory usage
     * is proportional to the maximum ordinal so it is only a good choice if
     * most docs match.
     */
    protected class DenseCollectionStrategy implements CollectionStrategy {
        private final BitArray ordsBits;

        public DenseCollectionStrategy(long maxOrd, BigArrays bigArrays) {
            ordsBits = new BitArray((int) maxOrd, context.bigArrays());
        }

        @Override
        public void add(long owningBucketOrd, int globalOrdinal) {
            assert owningBucketOrd == 0;
            ordsBits.set(globalOrdinal);
        }

        @Override
        public boolean exists(long owningBucketOrd, int globalOrdinal) {
            assert owningBucketOrd == 0;
            return ordsBits.get(globalOrdinal);
        }

        @Override
        public void close() {
            ordsBits.close();
        }
    }

    /**
     * Uses a hashed representation of whether of the join field in the docs
     * that match {@code ParentJoinAggregator#inFilter}. Its memory usage is
     * proportional to the number of matching global ordinals so it is used
     * when only some docs might match.
     */
    protected class SparseCollectionStrategy implements CollectionStrategy {
        private final LongKeyedBucketOrds ordsHash;

        public SparseCollectionStrategy(BigArrays bigArrays, CardinalityUpperBound cardinality) {
            ordsHash = LongKeyedBucketOrds.build(bigArrays, cardinality);
        }

        @Override
        public void add(long owningBucketOrd, int globalOrdinal) {
            ordsHash.add(owningBucketOrd, globalOrdinal);
        }

        @Override
        public boolean exists(long owningBucketOrd, int globalOrdinal) {
            return ordsHash.find(owningBucketOrd, globalOrdinal) >= 0;
        }

        @Override
        public void close() {
            ordsHash.close();
        }
    }
}
