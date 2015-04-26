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
package org.elasticsearch.search.aggregations.bucket.significant;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.terms.GlobalOrdinalsStringTermsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.ContextIndexSearcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * An global ordinal based implementation of significant terms, based on {@link SignificantStringTermsAggregator}.
 */
public class GlobalOrdinalsSignificantTermsAggregator extends GlobalOrdinalsStringTermsAggregator {

    protected long numCollectedDocs;
    protected final SignificantTermsAggregatorFactory termsAggFactory;

    public GlobalOrdinalsSignificantTermsAggregator(String name, AggregatorFactories factories, ValuesSource.Bytes.WithOrdinals.FieldData valuesSource,
                                                    BucketCountThresholds bucketCountThresholds,
                                                    IncludeExclude.OrdinalsFilter includeExclude, AggregationContext aggregationContext, Aggregator parent,
                                                    SignificantTermsAggregatorFactory termsAggFactory, Map<String, Object> metaData) throws IOException {

        super(name, factories, valuesSource, null, bucketCountThresholds, includeExclude, aggregationContext, parent, SubAggCollectionMode.DEPTH_FIRST, false, metaData);
        this.termsAggFactory = termsAggFactory;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        return new LeafBucketCollectorBase(super.getLeafCollector(ctx, sub), null) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                super.collect(doc, bucket);
                numCollectedDocs++;
            }
        };
    }


    @Override
    public SignificantStringTerms buildAggregation(long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        if (globalOrds == null) { // no context in this reader
            return buildEmptyAggregation();
        }

        final int size;
        if (bucketCountThresholds.getMinDocCount() == 0) {
            // if minDocCount == 0 then we can end up with more buckets then maxBucketOrd() returns
            size = (int) Math.min(globalOrds.getValueCount(), bucketCountThresholds.getShardSize());
        } else {
            size = (int) Math.min(maxBucketOrd(), bucketCountThresholds.getShardSize());
        }
        long supersetSize = termsAggFactory.prepareBackground(context);
        long subsetSize = numCollectedDocs;

        BucketSignificancePriorityQueue ordered = new BucketSignificancePriorityQueue(size);
        SignificantStringTerms.Bucket spare = null;
        for (long globalTermOrd = 0; globalTermOrd < globalOrds.getValueCount(); ++globalTermOrd) {
            if (includeExclude != null && !acceptedGlobalOrdinals.get(globalTermOrd)) {
                continue;
            }
            final long bucketOrd = getBucketOrd(globalTermOrd);
            final int bucketDocCount = bucketOrd < 0 ? 0 : bucketDocCount(bucketOrd);
            if (bucketCountThresholds.getMinDocCount() > 0 && bucketDocCount == 0) {
                continue;
            }
            if (spare == null) {
                spare = new SignificantStringTerms.Bucket(new BytesRef(), 0, 0, 0, 0, null);
            }
            spare.bucketOrd = bucketOrd;
            copy(globalOrds.lookupOrd(globalTermOrd), spare.termBytes);
            spare.subsetDf = bucketDocCount;
            spare.subsetSize = subsetSize;
            spare.supersetDf = termsAggFactory.getBackgroundFrequency(spare.termBytes);
            spare.supersetSize = supersetSize;
            // During shard-local down-selection we use subset/superset stats
            // that are for this shard only
            // Back at the central reducer these properties will be updated with
            // global stats
            spare.updateScore(termsAggFactory.getSignificanceHeuristic());
            if (spare.subsetDf >= bucketCountThresholds.getShardMinDocCount()) {
                spare = (SignificantStringTerms.Bucket) ordered.insertWithOverflow(spare);
            }
        }

        final InternalSignificantTerms.Bucket[] list = new InternalSignificantTerms.Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; i--) {
            final SignificantStringTerms.Bucket bucket = (SignificantStringTerms.Bucket) ordered.pop();
            // the terms are owned by the BytesRefHash, we need to pull a copy since the BytesRef hash data may be recycled at some point
            bucket.termBytes = BytesRef.deepCopyOf(bucket.termBytes);
            bucket.aggregations = bucketAggregations(bucket.bucketOrd);
            list[i] = bucket;
        }

        return new SignificantStringTerms(subsetSize, supersetSize, name, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(), termsAggFactory.getSignificanceHeuristic(), Arrays.asList(list), metaData());
    }

    @Override
    public SignificantStringTerms buildEmptyAggregation() {
        // We need to account for the significance of a miss in our global stats - provide corpus size as context
        ContextIndexSearcher searcher = context.searchContext().searcher();
        IndexReader topReader = searcher.getIndexReader();
        int supersetSize = topReader.numDocs();
        return new SignificantStringTerms(0, supersetSize, name, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(), termsAggFactory.getSignificanceHeuristic(), Collections.<InternalSignificantTerms.Bucket>emptyList(), metaData());
    }

    @Override
    protected void doClose() {
        Releasables.close(termsAggFactory);
    }

    public static class WithHash extends GlobalOrdinalsSignificantTermsAggregator {

        private final LongHash bucketOrds;

        public WithHash(String name, AggregatorFactories factories, ValuesSource.Bytes.WithOrdinals.FieldData valuesSource, BucketCountThresholds bucketCountThresholds, IncludeExclude.OrdinalsFilter includeExclude, AggregationContext aggregationContext, Aggregator parent, SignificantTermsAggregatorFactory termsAggFactory, Map<String, Object> metaData) throws IOException {
            super(name, factories, valuesSource, bucketCountThresholds, includeExclude, aggregationContext, parent, termsAggFactory, metaData);
            bucketOrds = new LongHash(1, aggregationContext.bigArrays());
        }

        @Override
        public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                final LeafBucketCollector sub) throws IOException {
            return new LeafBucketCollectorBase(super.getLeafCollector(ctx, sub), null) {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    assert bucket == 0;
                    numCollectedDocs++;
                    globalOrds.setDocument(doc);
                    final int numOrds = globalOrds.cardinality();
                    for (int i = 0; i < numOrds; i++) {
                        final long globalOrd = globalOrds.ordAt(i);
                        long bucketOrd = bucketOrds.add(globalOrd);
                        if (bucketOrd < 0) {
                            bucketOrd = -1 - bucketOrd;
                            collectExistingBucket(sub, doc, bucketOrd);
                        } else {
                            collectBucket(sub, doc, bucketOrd);
                        }
                    }
                }
            };
        }

        @Override
        protected long getBucketOrd(long termOrd) {
            return bucketOrds.find(termOrd);
        }

        @Override
        protected void doClose() {
            Releasables.close(termsAggFactory, bucketOrds);
        }
    }

}

