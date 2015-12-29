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
package org.elasticsearch.search.aggregations.bucket.children;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

// The RecordingPerReaderBucketCollector assumes per segment recording which isn't the case for this
// aggregation, for this reason that collector can't be used
public class ParentToChildrenAggregator extends SingleBucketAggregator {

    private final String parentType;
    private final Weight childFilter;
    private final Weight parentFilter;
    private final ValuesSource.Bytes.WithOrdinals.ParentChild valuesSource;

    // Maybe use PagedGrowableWriter? This will be less wasteful than LongArray, but then we don't have the reuse feature of BigArrays.
    // Also if we know the highest possible value that a parent agg will create then we store multiple values into one slot
    private final LongArray parentOrdToBuckets;

    // Only pay the extra storage price if the a parentOrd has multiple buckets
    // Most of the times a parent doesn't have multiple buckets, since there is only one document per parent ord,
    // only in the case of terms agg if a parent doc has multiple terms per field this is needed:
    private final LongObjectPagedHashMap<long[]> parentOrdToOtherBuckets;
    private boolean multipleBucketsPerParentOrd = false;

    public ParentToChildrenAggregator(String name, AggregatorFactories factories, AggregationContext aggregationContext,
                                      Aggregator parent, String parentType, Query childFilter, Query parentFilter,
                                      ValuesSource.Bytes.WithOrdinals.ParentChild valuesSource,
            long maxOrd, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, factories, aggregationContext, parent, pipelineAggregators, metaData);
        this.parentType = parentType;
        // these two filters are cached in the parser
        this.childFilter = aggregationContext.searchContext().searcher().createNormalizedWeight(childFilter, false);
        this.parentFilter = aggregationContext.searchContext().searcher().createNormalizedWeight(parentFilter, false);
        this.parentOrdToBuckets = aggregationContext.bigArrays().newLongArray(maxOrd, false);
        this.parentOrdToBuckets.fill(0, maxOrd, -1);
        this.parentOrdToOtherBuckets = new LongObjectPagedHashMap<>(aggregationContext.bigArrays());
        this.valuesSource = valuesSource;
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        return new InternalChildren(name, bucketDocCount(owningBucketOrdinal), bucketAggregations(owningBucketOrdinal), pipelineAggregators(),
                metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalChildren(name, 0, buildEmptySubAggregations(), pipelineAggregators(), metaData());
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        final SortedDocValues globalOrdinals = valuesSource.globalOrdinalsValues(parentType, ctx);
        assert globalOrdinals != null;
        Scorer parentScorer = parentFilter.scorer(ctx);
        final Bits parentDocs = Lucene.asSequentialAccessBits(ctx.reader().maxDoc(), parentScorer);
        return new LeafBucketCollector() {

            @Override
            public void collect(int docId, long bucket) throws IOException {
                if (parentDocs.get(docId)) {
                    long globalOrdinal = globalOrdinals.getOrd(docId);
                    if (globalOrdinal != -1) {
                        if (parentOrdToBuckets.get(globalOrdinal) == -1) {
                            parentOrdToBuckets.set(globalOrdinal, bucket);
                        } else {
                            long[] bucketOrds = parentOrdToOtherBuckets.get(globalOrdinal);
                            if (bucketOrds != null) {
                                bucketOrds = Arrays.copyOf(bucketOrds, bucketOrds.length + 1);
                                bucketOrds[bucketOrds.length - 1] = bucket;
                                parentOrdToOtherBuckets.put(globalOrdinal, bucketOrds);
                            } else {
                                parentOrdToOtherBuckets.put(globalOrdinal, new long[]{bucket});
                            }
                            multipleBucketsPerParentOrd = true;
                        }
                    }
                }
            }
        };
    }

    @Override
    protected void doPostCollection() throws IOException {
        IndexReader indexReader = context().searchContext().searcher().getIndexReader();
        for (LeafReaderContext ctx : indexReader.leaves()) {
            Scorer childDocsScorer = childFilter.scorer(ctx);
            if (childDocsScorer == null) {
                continue;
            }
            DocIdSetIterator childDocsIter = childDocsScorer.iterator();

            final LeafBucketCollector sub = collectableSubAggregators.getLeafCollector(ctx);
            final SortedDocValues globalOrdinals = valuesSource.globalOrdinalsValues(parentType, ctx);

            // Set the scorer, since we now replay only the child docIds
            sub.setScorer(new ConstantScoreScorer(null, 1f,childDocsIter));

            final Bits liveDocs = ctx.reader().getLiveDocs();
            for (int docId = childDocsIter.nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = childDocsIter.nextDoc()) {
                if (liveDocs != null && liveDocs.get(docId) == false) {
                    continue;
                }
                long globalOrdinal = globalOrdinals.getOrd(docId);
                if (globalOrdinal != -1) {
                    long bucketOrd = parentOrdToBuckets.get(globalOrdinal);
                    if (bucketOrd != -1) {
                        collectBucket(sub, docId, bucketOrd);
                        if (multipleBucketsPerParentOrd) {
                            long[] otherBucketOrds = parentOrdToOtherBuckets.get(globalOrdinal);
                            if (otherBucketOrds != null) {
                                for (long otherBucketOrd : otherBucketOrds) {
                                    collectBucket(sub, docId, otherBucketOrd);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void doClose() {
        Releasables.close(parentOrdToBuckets, parentOrdToOtherBuckets);
    }

    public static class Factory extends ValuesSourceAggregatorFactory<ValuesSource.Bytes.WithOrdinals.ParentChild> {

        private final String parentType;
        private final Query parentFilter;
        private final Query childFilter;

        public Factory(String name, ValuesSourceConfig<ValuesSource.Bytes.WithOrdinals.ParentChild> config, String parentType, Query parentFilter, Query childFilter) {
            super(name, InternalChildren.TYPE.name(), config);
            this.parentType = parentType;
            this.parentFilter = parentFilter;
            this.childFilter = childFilter;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                Map<String, Object> metaData) throws IOException {
            return new NonCollectingAggregator(name, aggregationContext, parent, pipelineAggregators, metaData) {

                @Override
                public InternalAggregation buildEmptyAggregation() {
                    return new InternalChildren(name, 0, buildEmptySubAggregations(), pipelineAggregators(), metaData());
                }

            };
        }

        @Override
        protected Aggregator doCreateInternal(ValuesSource.Bytes.WithOrdinals.ParentChild valuesSource,
                AggregationContext aggregationContext, Aggregator parent, boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators,
                Map<String, Object> metaData) throws IOException {
            long maxOrd = valuesSource.globalMaxOrd(aggregationContext.searchContext().searcher(), parentType);
            return new ParentToChildrenAggregator(name, factories, aggregationContext, parent, parentType, childFilter, parentFilter,
                    valuesSource, maxOrd, pipelineAggregators, metaData);
        }

    }
}
