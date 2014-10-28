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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.ReaderContextAware;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.ApplyAcceptedDocsFilter;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.index.cache.fixedbitset.FixedBitSetFilter;
import org.elasticsearch.index.search.child.ConstantScorer;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// The RecordingPerReaderBucketCollector assumes per segment recording which isn't the case for this
// aggregation, for this reason that collector can't be used
public class ParentToChildrenAggregator extends SingleBucketAggregator implements ReaderContextAware {

    private final String parentType;
    private final Filter childFilter;
    private final FixedBitSetFilter parentFilter;
    private final ValuesSource.Bytes.WithOrdinals.ParentChild valuesSource;

    // Maybe use PagedGrowableWriter? This will be less wasteful than LongArray, but then we don't have the reuse feature of BigArrays.
    // Also if we know the highest possible value that a parent agg will create then we store multiple values into one slot
    private final LongArray parentOrdToBuckets;

    // Only pay the extra storage price if the a parentOrd has multiple buckets
    // Most of the times a parent doesn't have multiple buckets, since there is only one document per parent ord,
    // only in the case of terms agg if a parent doc has multiple terms per field this is needed:
    private final LongObjectPagedHashMap<long[]> parentOrdToOtherBuckets;
    private boolean multipleBucketsPerParentOrd = false;

    private List<AtomicReaderContext> replay = new ArrayList<>();
    private SortedDocValues globalOrdinals;
    private Bits parentDocs;

    public ParentToChildrenAggregator(String name, AggregatorFactories factories, AggregationContext aggregationContext,
                                      Aggregator parent, String parentType, Filter childFilter, Filter parentFilter,
                                      ValuesSource.Bytes.WithOrdinals.ParentChild valuesSource, long maxOrd) {
        super(name, factories, aggregationContext, parent);
        this.parentType = parentType;
        // The child filter doesn't rely on random access it just used to iterate over all docs with a specific type,
        // so use the filter cache instead. When the filter cache is smarter with what filter impl to pick we can benefit
        // from it here
        this.childFilter = new ApplyAcceptedDocsFilter(aggregationContext.searchContext().filterCache().cache(childFilter));
        this.parentFilter = aggregationContext.searchContext().fixedBitSetFilterCache().getFixedBitSetFilter(parentFilter);
        this.parentOrdToBuckets = aggregationContext.bigArrays().newLongArray(maxOrd, false);
        this.parentOrdToBuckets.fill(0, maxOrd, -1);
        this.parentOrdToOtherBuckets = new LongObjectPagedHashMap<>(aggregationContext.bigArrays());
        this.valuesSource = valuesSource;
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        return new InternalChildren(name, bucketDocCount(owningBucketOrdinal), bucketAggregations(owningBucketOrdinal));
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalChildren(name, 0, buildEmptySubAggregations());
    }

    @Override
    public void collect(int docId, long bucketOrdinal) throws IOException {
        if (parentDocs != null && parentDocs.get(docId)) {
            long globalOrdinal = globalOrdinals.getOrd(docId);
            if (globalOrdinal != -1) {
                if (parentOrdToBuckets.get(globalOrdinal) == -1) {
                    parentOrdToBuckets.set(globalOrdinal, bucketOrdinal);
                } else {
                    long[] bucketOrds = parentOrdToOtherBuckets.get(globalOrdinal);
                    if (bucketOrds != null) {
                        bucketOrds = Arrays.copyOf(bucketOrds, bucketOrds.length + 1);
                        bucketOrds[bucketOrds.length - 1] = bucketOrdinal;
                        parentOrdToOtherBuckets.put(globalOrdinal, bucketOrds);
                    } else {
                        parentOrdToOtherBuckets.put(globalOrdinal, new long[]{bucketOrdinal});
                    }
                    multipleBucketsPerParentOrd = true;
                }
            }
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        if (replay == null) {
            return;
        }

        globalOrdinals = valuesSource.globalOrdinalsValues(parentType);
        assert globalOrdinals != null;
        try {
            DocIdSet parentDocIdSet = parentFilter.getDocIdSet(reader, null);
            if (parentDocIdSet != null) {
                parentDocs = parentDocIdSet.bits();
            } else {
                parentDocs = null;
            }
            DocIdSet childDocIdSet = childFilter.getDocIdSet(reader, null);
            if (globalOrdinals != null && !DocIdSets.isEmpty(childDocIdSet)) {
                replay.add(reader);
            }
        } catch (IOException e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    @Override
    protected void doPostCollection() throws IOException {
        List<AtomicReaderContext> replay = this.replay;
        this.replay = null;

        for (AtomicReaderContext atomicReaderContext : replay) {
            context.setNextReader(atomicReaderContext);

            SortedDocValues globalOrdinals = valuesSource.globalOrdinalsValues(parentType);
            DocIdSet childDocIdSet = childFilter.getDocIdSet(atomicReaderContext, atomicReaderContext.reader().getLiveDocs());
            if (childDocIdSet == null) {
                continue;
            }
            DocIdSetIterator childDocsIter = childDocIdSet.iterator();
            if (childDocsIter == null) {
                continue;
            }

            // Set the scorer, since we now replay only the child docIds
            context.setScorer(ConstantScorer.create(childDocsIter, null, 1f));

            for (int docId = childDocsIter.nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = childDocsIter.nextDoc()) {
                long globalOrdinal = globalOrdinals.getOrd(docId);
                if (globalOrdinal != -1) {
                    long bucketOrd = parentOrdToBuckets.get(globalOrdinal);
                    if (bucketOrd != -1) {
                        collectBucket(docId, bucketOrd);
                        if (multipleBucketsPerParentOrd) {
                            long[] otherBucketOrds = parentOrdToOtherBuckets.get(globalOrdinal);
                            if (otherBucketOrds != null) {
                                for (long otherBucketOrd : otherBucketOrds) {
                                    collectBucket(docId, otherBucketOrd);
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
        private final Filter parentFilter;
        private final Filter childFilter;

        public Factory(String name, ValuesSourceConfig<ValuesSource.Bytes.WithOrdinals.ParentChild> config, String parentType, Filter parentFilter, Filter childFilter) {
            super(name, InternalChildren.TYPE.name(), config);
            this.parentType = parentType;
            this.parentFilter = parentFilter;
            this.childFilter = childFilter;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            throw new ElasticsearchIllegalStateException("[children] aggregation doesn't support unmapped");
        }

        @Override
        protected Aggregator create(ValuesSource.Bytes.WithOrdinals.ParentChild valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            long maxOrd = valuesSource.globalMaxOrd(aggregationContext.searchContext().searcher(), parentType);
            return new ParentToChildrenAggregator(name, factories, aggregationContext, parent, parentType, childFilter, parentFilter, valuesSource, maxOrd);
        }

    }
}
