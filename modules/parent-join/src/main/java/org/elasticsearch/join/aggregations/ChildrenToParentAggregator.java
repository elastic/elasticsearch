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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link BucketsAggregator} which resolves to the matching parent documents.
 *
 * It ensures that each parent only matches once per bucket.
 */
public class ChildrenToParentAggregator extends BucketsAggregator implements SingleBucketAggregator {

    static final ParseField TYPE_FIELD = new ParseField("type");

    private final Weight childFilter;
    private final Weight parentFilter;
    private final ValuesSource.Bytes.WithOrdinals valuesSource;

    // Maybe use PagedGrowableWriter? This will be less wasteful than LongArray,
    // but then we don't have the reuse feature of BigArrays.
    // Also if we know the highest possible value that a parent agg will create
    // then we store multiple values into one slot
    private final LongArray childrenOrdToBuckets;

    // Only pay the extra storage price if the a parentOrd has multiple buckets
    // Most of the times a parent doesn't have multiple buckets, since there is
    // only one document per parent ord,
    // only in the case of terms agg if a parent doc has multiple terms per
    // field this is needed:
    private final LongObjectPagedHashMap<long[]> childrenOrdToOtherBuckets;
    private boolean multipleBucketsPerChildrenOrd = false;

    public ChildrenToParentAggregator(String name, AggregatorFactories factories,
            SearchContext context, Aggregator parent, Query childFilter,
            Query parentFilter, ValuesSource.Bytes.WithOrdinals valuesSource,
            long maxOrd, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
            throws IOException {
        super(name, factories, context, parent, pipelineAggregators, metaData);
        // these two filters are cached in the parser
        this.childFilter = context.searcher().createWeight(context.searcher().rewrite(childFilter), ScoreMode.COMPLETE_NO_SCORES, 1f);
        this.parentFilter = context.searcher().createWeight(context.searcher().rewrite(parentFilter), ScoreMode.COMPLETE_NO_SCORES, 1f);
        this.childrenOrdToBuckets = context.bigArrays().newLongArray(maxOrd, false);
        this.childrenOrdToBuckets.fill(0, maxOrd, -1);
        this.childrenOrdToOtherBuckets = new LongObjectPagedHashMap<>(context.bigArrays());
        this.valuesSource = valuesSource;
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        return new InternalParent(name, bucketDocCount(owningBucketOrdinal),
                bucketAggregations(owningBucketOrdinal), pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalParent(name, 0, buildEmptySubAggregations(), pipelineAggregators(),
                metaData());
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final SortedSetDocValues globalOrdinals = valuesSource.globalOrdinalsValues(ctx);
        final Bits childDocs = Lucene.asSequentialAccessBits(ctx.reader().maxDoc(), childFilter.scorerSupplier(ctx));
        return new LeafBucketCollector() {

            @Override
            public void collect(int docId, long bucket) throws IOException {
                if (childDocs.get(docId) && globalOrdinals.advanceExact(docId)) {
                    long globalOrdinal = globalOrdinals.nextOrd();
                    assert globalOrdinals.nextOrd() == SortedSetDocValues.NO_MORE_ORDS;
                    if (globalOrdinal != -1) {
                        if (childrenOrdToBuckets.get(globalOrdinal) == -1) {
                            childrenOrdToBuckets.set(globalOrdinal, bucket);
                        } else {
                            long[] bucketOrds = childrenOrdToOtherBuckets.get(globalOrdinal);
                            if (bucketOrds != null) {
                                bucketOrds = Arrays.copyOf(bucketOrds, bucketOrds.length + 1);
                                bucketOrds[bucketOrds.length - 1] = bucket;
                                childrenOrdToOtherBuckets.put(globalOrdinal, bucketOrds);
                            } else {
                                childrenOrdToOtherBuckets.put(globalOrdinal, new long[] { bucket });
                            }
                            multipleBucketsPerChildrenOrd = true;
                        }
                    }
                }
            }
        };
    }

    @Override
    protected void doPostCollection() throws IOException {
        IndexReader indexReader = context().searcher().getIndexReader();
        for (LeafReaderContext ctx : indexReader.leaves()) {
            Scorer parentDocsScorer = parentFilter.scorer(ctx);
            if (parentDocsScorer == null) {
                continue;
            }
            DocIdSetIterator parentDocsIter = parentDocsScorer.iterator();

            final LeafBucketCollector sub = collectableSubAggregators.getLeafCollector(ctx);

            final SortedSetDocValues globalOrdinals = valuesSource.globalOrdinalsValues(ctx);
            // Set the scorer, since we now replay only the parent docIds
            sub.setScorer(new Scorable() {
                @Override
                public float score() {
                    return 1f;
                }

                @Override
                public int docID() {
                    return parentDocsIter.docID();
                }
            });

            // TODO: this is unwanted allocation, just for initial verification
            // of the implementation idea, probably needs to be done differently
            // for production use so we do not cause memory churn here
            //Set<Tuple<Integer,Long>> seenParents = new HashSet<>();
            Map<Integer, Set<Long>> seenParents = new HashMap<>();

            final Bits liveDocs = ctx.reader().getLiveDocs();
            for (int docId = parentDocsIter
                    .nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = parentDocsIter
                            .nextDoc()) {
                if (liveDocs != null && liveDocs.get(docId) == false) {
                    continue;
                }
                if (globalOrdinals.advanceExact(docId)) {
                    long globalOrdinal = globalOrdinals.nextOrd();
                    assert globalOrdinals.nextOrd() == SortedSetDocValues.NO_MORE_ORDS;
                    long bucketOrd = childrenOrdToBuckets.get(globalOrdinal);
                    if (bucketOrd != -1) {
                        collectBucketIfUnique(sub, seenParents, docId, bucketOrd);
                        if (multipleBucketsPerChildrenOrd) {
                            long[] otherBucketOrds = childrenOrdToOtherBuckets.get(globalOrdinal);
                            if (otherBucketOrds != null) {
                                for (long otherBucketOrd : otherBucketOrds) {
                                    // only collect each parentId once per bucket
                                    collectBucketIfUnique(sub, seenParents, docId, otherBucketOrd);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private void collectBucketIfUnique(LeafBucketCollector sub, Map<Integer, Set<Long>> seenParents,
                                       int docId, long bucketOrd) throws IOException {
        // only collect each parentId once per bucket
        Set<Long> seenBucketOrds = seenParents.computeIfAbsent(docId, integer -> new HashSet<>());
        if (seenBucketOrds.add(bucketOrd)) {
            collectBucket(sub, docId, bucketOrd);
        }
    }

    @Override
    protected void doClose() {
        Releasables.close(childrenOrdToBuckets, childrenOrdToOtherBuckets);
    }
}
