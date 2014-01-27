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

import com.google.common.collect.UnmodifiableIterator;
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
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.BytesRefHash;
//import org.elasticsearch.search.aggregations.bucket.significant.StringTerms.Bucket;
//import org.elasticsearch.search.aggregations.bucket.terms.Terms;
//import org.elasticsearch.search.aggregations.bucket.terms.support.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.bytes.BytesValuesSource;
import org.elasticsearch.search.internal.ContextIndexSearcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

/**
 * An aggregator of significant string values.
 */
public class SignificantStringTermsAggregator extends BucketsAggregator {

    private final ValuesSource valuesSource;
    private final int requiredSize;
    private final int shardSize;
    private final long minDocCount;
    protected final BytesRefHash bucketOrds;
    private final IncludeExclude includeExclude;
    private BytesValues values;
    protected int numCollectedDocs;
    private SignificantTermsAggregatorFactory termsAggFactory;

    public SignificantStringTermsAggregator(String name, AggregatorFactories factories, ValuesSource valuesSource, SignificantTermsAggregatorFactory termsAggFactory, long estimatedBucketCount,
                                 int requiredSize, int shardSize, long minDocCount,
                                 IncludeExclude includeExclude, AggregationContext aggregationContext, Aggregator parent) {

        super(name, BucketAggregationMode.PER_BUCKET, factories, estimatedBucketCount, aggregationContext, parent);
        this.valuesSource = valuesSource;
        this.termsAggFactory = termsAggFactory;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        this.minDocCount = minDocCount;
        this.includeExclude = includeExclude;
        bucketOrds = new BytesRefHash(estimatedBucketCount, aggregationContext.pageCacheRecycler());
    }

    @Override
    public boolean shouldCollect() {
        return true;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        values = valuesSource.bytesValues();
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        numCollectedDocs++;
        assert owningBucketOrdinal == 0;
        final int valuesCount = values.setDocument(doc);

        for (int i = 0; i < valuesCount; ++i) {
            final BytesRef bytes = values.nextValue();
            if (includeExclude != null && !includeExclude.accept(bytes)) {
                continue;
            }
            final int hash = values.currentValueHash();
            assert hash == bytes.hashCode();
            long bucketOrdinal = bucketOrds.add(bytes, hash);
            if (bucketOrdinal < 0) { // already seen
                bucketOrdinal = - 1 - bucketOrdinal;
            }
            //TODO this system of counting is only based on doc volumes.
            // There are scenarios where count distinct of *entities* is 
            // required e.g. see https://docs.google.com/a/elasticsearch.com/presentation/d/17jkxrsmSq6Gpd2mKAIO4949jSCgkSIM3j2KIOQ8oEEI/edit?usp=sharing
            // and the section on credit card fraud which involves counting
            // unique payee references not volumes of docs
            collectBucket(doc, bucketOrdinal);
        }
    }

    /** Returns an iterator over the field data terms. */
    private static Iterator<BytesRef> terms(final BytesValues.WithOrdinals bytesValues, boolean reverse) {
        final Ordinals.Docs ordinals = bytesValues.ordinals();
        if (reverse) {
            return new UnmodifiableIterator<BytesRef>() {

                long i = ordinals.getMaxOrd() - 1;

                @Override
                public boolean hasNext() {
                    return i >= Ordinals.MIN_ORDINAL;
                }

                @Override
                public BytesRef next() {
                    bytesValues.getValueByOrd(i--);
                    return bytesValues.copyShared();
                }

            };
        } else {
            return new UnmodifiableIterator<BytesRef>() {

                long i = Ordinals.MIN_ORDINAL;

                @Override
                public boolean hasNext() {
                    return i < ordinals.getMaxOrd();
                }

                @Override
                public BytesRef next() {
                    bytesValues.getValueByOrd(i++);
                    return bytesValues.copyShared();
                }

            };
        }
    }
    
    

    @Override
    public SignificantStringTerms buildAggregation(long owningBucketOrdinal) {
        assert owningBucketOrdinal == 0;

        final int size = (int) Math.min(bucketOrds.size(), shardSize);

        ContextIndexSearcher searcher = context.searchContext().searcher();
        IndexReader topReader = searcher.getIndexReader();
        int supersetSize = topReader.numDocs();
        int subsetSize = numCollectedDocs;

        BucketSignificancePriorityQueue ordered = new BucketSignificancePriorityQueue(size);
        SignificantStringTerms.Bucket spare = null;
        for (int i = 0; i < bucketOrds.size(); i++) {
            if (spare == null) {
                spare = new SignificantStringTerms.Bucket(new BytesRef(), 0, 0, 0, 0, null);
            }

            bucketOrds.get(i, spare.termBytes);
            spare.subsetDf = bucketDocCount(i);
            spare.subsetSize = subsetSize;
            spare.supersetDf = termsAggFactory.getBackgroundFrequency(topReader, spare.termBytes);
            spare.supersetSize = supersetSize;
            assert spare.subsetDf <= spare.supersetDf;
            // During shard-local down-selection we use subset/superset stats 
            // that are for this shard only
            // Back at the central reducer these properties will be updated with
            // global stats
            spare.updateScore();

            spare.bucketOrd = i;
            spare = (SignificantStringTerms.Bucket) ordered.insertWithOverflow(spare);
        }

        final InternalSignificantTerms.Bucket[] list = new InternalSignificantTerms.Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; --i) {
            final SignificantStringTerms.Bucket bucket = (SignificantStringTerms.Bucket) ordered.pop();
            bucket.aggregations = bucketAggregations(bucket.bucketOrd);
            list[i] = bucket;
        }

        return new SignificantStringTerms(subsetSize, supersetSize, name, requiredSize, minDocCount, Arrays.asList(list));
    }

    @Override
    public SignificantStringTerms buildEmptyAggregation() {
        // We need to account for the significance of a miss in our global stats
        // - provide corpus size as context
        ContextIndexSearcher searcher = context.searchContext().searcher();
        IndexReader topReader = searcher.getIndexReader();
        int supersetSize = topReader.numDocs();
        return new SignificantStringTerms(0, supersetSize, name, requiredSize, minDocCount, Collections.<InternalSignificantTerms.Bucket> emptyList());
    }

    @Override
    public void doRelease() {
        Releasables.release(bucketOrds);
    }

    /**
     * Extension of SignificantStringTermsAggregator that caches bucket ords using terms ordinals.
     */
    public static class WithOrdinals extends SignificantStringTermsAggregator {

        private final BytesValuesSource.WithOrdinals valuesSource;
        private BytesValues.WithOrdinals bytesValues;
        private Ordinals.Docs ordinals;
        private LongArray ordinalToBucket;

        public WithOrdinals(String name, AggregatorFactories factories, BytesValuesSource.WithOrdinals valuesSource, SignificantTermsAggregatorFactory indexedFieldName, 
                long esitmatedBucketCount, int requiredSize, int shardSize, long minDocCount, AggregationContext aggregationContext, Aggregator parent) {
            super(name, factories, valuesSource, indexedFieldName, esitmatedBucketCount, requiredSize, shardSize, minDocCount, null, aggregationContext, parent);
            this.valuesSource = valuesSource;
        }

        @Override
        public void setNextReader(AtomicReaderContext reader) {
            bytesValues = valuesSource.bytesValues();
            ordinals = bytesValues.ordinals();
            final long maxOrd = ordinals.getMaxOrd();
            if (ordinalToBucket == null || ordinalToBucket.size() < maxOrd) {
                if (ordinalToBucket != null) {
                    ordinalToBucket.release();
                }
                ordinalToBucket = BigArrays.newLongArray(BigArrays.overSize(maxOrd), context().pageCacheRecycler(), false);
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
                if (bucketOrd < 0) { // unlikely condition on a low-cardinality
                                     // field
                    final BytesRef bytes = bytesValues.getValueByOrd(ord);
                    final int hash = bytesValues.currentValueHash();
                    assert hash == bytes.hashCode();
                    bucketOrd = bucketOrds.add(bytes, hash);
                    if (bucketOrd < 0) { // already seen in another segment
                        bucketOrd = -1 - bucketOrd;
                    }
                    ordinalToBucket.set(ord, bucketOrd);
                }

                collectBucket(doc, bucketOrd);
            }
        }

        @Override
        public void doRelease() {
            Releasables.release(bucketOrds, ordinalToBucket);
        }
    }

}

