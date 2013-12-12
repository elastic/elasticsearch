/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.elasticsearch.common.lucene.ReaderContextAware;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.support.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.bytes.BytesValuesSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

/**
 * An aggregator of string values.
 */
public class StringTermsAggregator extends BucketsAggregator {

    private final ValuesSource valuesSource;
    private final InternalOrder order;
    private final int requiredSize;
    private final int shardSize;
    protected final BytesRefHash bucketOrds;
    private final IncludeExclude includeExclude;

    public StringTermsAggregator(String name, AggregatorFactories factories, ValuesSource valuesSource, long estimatedBucketCount,
                                 InternalOrder order, int requiredSize, int shardSize,
                                 IncludeExclude includeExclude, AggregationContext aggregationContext, Aggregator parent) {

        super(name, BucketAggregationMode.PER_BUCKET, factories, estimatedBucketCount, aggregationContext, parent);
        this.valuesSource = valuesSource;
        this.order = order;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        this.includeExclude = includeExclude;
        bucketOrds = new BytesRefHash();
    }

    @Override
    public boolean shouldCollect() {
        return true;
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        final BytesValues values = valuesSource.bytesValues();
        final int valuesCount = values.setDocument(doc);

        for (int i = 0; i < valuesCount; ++i) {
            final BytesRef bytes = values.nextValue();
            if (includeExclude != null && !includeExclude.accept(bytes)) {
                continue;
            }
            final int hash = values.currentValueHash();
            assert hash == bytes.hashCode();
            int bucketOrdinal = bucketOrds.add(bytes, hash);
            if (bucketOrdinal < 0) { // already seen
                bucketOrdinal = - 1 - bucketOrdinal;
            }
            collectBucket(doc, bucketOrdinal);
        }
    }

    // private impl that stores a bucket ord. This allows for computing the aggregations lazily.
    static class OrdinalBucket extends StringTerms.Bucket {

        int bucketOrd;

        public OrdinalBucket() {
            super(new BytesRef(), 0, null);
        }

    }

    @Override
    public StringTerms buildAggregation(long owningBucketOrdinal) {
        assert owningBucketOrdinal == 0;
        final int size = Math.min(bucketOrds.size(), shardSize);

        BucketPriorityQueue ordered = new BucketPriorityQueue(size, order.comparator());
        OrdinalBucket spare = null;
        for (int i = 0; i < bucketOrds.size(); ++i) {
            if (spare == null) {
                spare = new OrdinalBucket();
            }
            bucketOrds.get(i, spare.termBytes);
            spare.docCount = bucketDocCount(i);
            spare.bucketOrd = i;
            spare = (OrdinalBucket) ordered.insertWithOverflow(spare);
        }

        final InternalTerms.Bucket[] list = new InternalTerms.Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; --i) {
            final OrdinalBucket bucket = (OrdinalBucket) ordered.pop();
            bucket.aggregations = bucketAggregations(bucket.bucketOrd);
            list[i] = bucket;
        }
        return new StringTerms(name, order, requiredSize, Arrays.asList(list));
    }

    @Override
    public StringTerms buildEmptyAggregation() {
        return new StringTerms(name, order, requiredSize, Collections.<InternalTerms.Bucket>emptyList());
    }

    /**
     * Extension of StringTermsAggregator that caches bucket ords using terms ordinals.
     */
    public static class WithOrdinals extends StringTermsAggregator implements ReaderContextAware {

        private final BytesValuesSource.WithOrdinals valuesSource;
        private BytesValues.WithOrdinals bytesValues;
        private Ordinals.Docs ordinals;
        private LongArray ordinalToBucket;

        public WithOrdinals(String name, AggregatorFactories factories, BytesValuesSource.WithOrdinals valuesSource, long esitmatedBucketCount,
                InternalOrder order, int requiredSize, int shardSize, AggregationContext aggregationContext, Aggregator parent) {
            super(name, factories, valuesSource, esitmatedBucketCount, order, requiredSize, shardSize, null, aggregationContext, parent);
            this.valuesSource = valuesSource;
        }

        @Override
        public void setNextReader(AtomicReaderContext reader) {
            bytesValues = valuesSource.bytesValues();
            ordinals = bytesValues.ordinals();
            final long maxOrd = ordinals.getMaxOrd();
            if (ordinalToBucket == null || ordinalToBucket.size() < maxOrd) {
                ordinalToBucket = BigArrays.newLongArray(BigArrays.overSize(maxOrd));
            }
            ordinalToBucket.fill(0, maxOrd, -1L);
        }

        @Override
        public void collect(int doc, long owningBucketOrdinal) throws IOException {
            assert owningBucketOrdinal == 0 : "this is a per_bucket aggregator";
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
                        bucketOrd = - 1 - bucketOrd;
                    }
                    ordinalToBucket.set(ord, bucketOrd);
                }

                collectBucket(doc, bucketOrd);
            }
        }
    }

}

