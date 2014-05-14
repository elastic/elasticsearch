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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.terms.StringTermsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.ContextIndexSearcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

/**
 * An aggregator of significant string values.
 */
public class SignificantStringTermsAggregator extends StringTermsAggregator {

    protected long numCollectedDocs;
    protected final SignificantTermsAggregatorFactory termsAggFactory;

    public SignificantStringTermsAggregator(String name, AggregatorFactories factories, ValuesSource valuesSource,
            long estimatedBucketCount, BucketCountThresholds bucketCountThresholds,
            IncludeExclude includeExclude, AggregationContext aggregationContext, Aggregator parent,
            SignificantTermsAggregatorFactory termsAggFactory) {

        super(name, factories, valuesSource, estimatedBucketCount, null, bucketCountThresholds, includeExclude, aggregationContext, parent);
        this.termsAggFactory = termsAggFactory;
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        super.collect(doc, owningBucketOrdinal);
        numCollectedDocs++;
    }

    @Override
    public SignificantStringTerms buildAggregation(long owningBucketOrdinal) {
        assert owningBucketOrdinal == 0;

        final int size = (int) Math.min(bucketOrds.size(), bucketCountThresholds.getShardSize());
        long supersetSize = termsAggFactory.prepareBackground(context);
        long subsetSize = numCollectedDocs;

        BucketSignificancePriorityQueue ordered = new BucketSignificancePriorityQueue(size);
        SignificantStringTerms.Bucket spare = null;
        for (int i = 0; i < bucketOrds.size(); i++) {
            if (spare == null) {
                spare = new SignificantStringTerms.Bucket(new BytesRef(), 0, 0, 0, 0, null);
            }

            bucketOrds.get(i, spare.termBytes);
            spare.subsetDf = bucketDocCount(i);
            spare.subsetSize = subsetSize;
            spare.supersetDf = termsAggFactory.getBackgroundFrequency(spare.termBytes);
            spare.supersetSize = supersetSize;
            // During shard-local down-selection we use subset/superset stats 
            // that are for this shard only
            // Back at the central reducer these properties will be updated with
            // global stats
            spare.updateScore();

            spare.bucketOrd = i;
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

        return new SignificantStringTerms(subsetSize, supersetSize, name, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(), Arrays.asList(list));
    }

    @Override
    public SignificantStringTerms buildEmptyAggregation() {
        // We need to account for the significance of a miss in our global stats - provide corpus size as context
        ContextIndexSearcher searcher = context.searchContext().searcher();
        IndexReader topReader = searcher.getIndexReader();
        int supersetSize = topReader.numDocs();
        return new SignificantStringTerms(0, supersetSize, name, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(), Collections.<InternalSignificantTerms.Bucket>emptyList());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds, termsAggFactory);
    }

    /**
     * Extension of SignificantStringTermsAggregator that caches bucket ords using terms ordinals.
     */
    public static class WithOrdinals extends SignificantStringTermsAggregator {

        private final ValuesSource.Bytes.WithOrdinals valuesSource;
        private BytesValues.WithOrdinals bytesValues;
        private Ordinals.Docs ordinals;
        private LongArray ordinalToBucket;

        public WithOrdinals(String name, AggregatorFactories factories, ValuesSource.Bytes.WithOrdinals valuesSource,
                long esitmatedBucketCount, BucketCountThresholds bucketCountThresholds, AggregationContext aggregationContext,
                Aggregator parent, SignificantTermsAggregatorFactory termsAggFactory) {
            super(name, factories, valuesSource, esitmatedBucketCount, bucketCountThresholds, null, aggregationContext, parent, termsAggFactory);
            this.valuesSource = valuesSource;
        }

        @Override
        public void setNextReader(AtomicReaderContext reader) {
            bytesValues = valuesSource.bytesValues();
            ordinals = bytesValues.ordinals();
            final long maxOrd = ordinals.getMaxOrd();
            if (ordinalToBucket == null || ordinalToBucket.size() < maxOrd) {
                if (ordinalToBucket != null) {
                    ordinalToBucket.close();
                }
                ordinalToBucket = context().bigArrays().newLongArray(BigArrays.overSize(maxOrd), false);
            }
            ordinalToBucket.fill(0, maxOrd, -1L);
        }

        @Override
        public void collect(int doc, long owningBucketOrdinal) throws IOException {
            assert owningBucketOrdinal == 0 : "this is a per_bucket aggregator";
            numCollectedDocs++;
            final int valuesCount = ordinals.setDocument(doc);

            for (int i = 0; i < valuesCount; ++i) {
                final long ord = ordinals.nextOrd();
                long bucketOrd = ordinalToBucket.get(ord);
                if (bucketOrd < 0) { // unlikely condition on a low-cardinality field
                    final BytesRef bytes = bytesValues.getValueByOrd(ord);
                    final int hash = bytesValues.currentValueHash();
                    assert hash == bytes.hashCode();
                    bucketOrd = bucketOrds.add(bytes, hash);
                    if (bucketOrd < 0) { // already seen in another segment
                        bucketOrd = -1 - bucketOrd;
                        collectExistingBucket(doc, bucketOrd);
                    } else {
                        collectBucket(doc, bucketOrd);
                    }
                    ordinalToBucket.set(ord, bucketOrd);
                } else {
                    collectExistingBucket(doc, bucketOrd);
                }

            }
        }

        @Override
        public void doClose() {
            Releasables.close(bucketOrds, termsAggFactory, ordinalToBucket);
        }

    }

}

