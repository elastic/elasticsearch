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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.support.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Arrays;

/**
 * An aggregator of string values that relies on global ordinals in order to build buckets.
 */
public class GlobalOrdinalsStringTermsAggregator extends AbstractStringTermsAggregator {

    private final ValuesSource.Bytes.WithOrdinals.FieldData valuesSource;
    private BytesValues.WithOrdinals globalValues;
    private Ordinals.Docs globalOrdinals;

    public GlobalOrdinalsStringTermsAggregator(String name, AggregatorFactories factories, ValuesSource.Bytes.WithOrdinals.FieldData valuesSource, long estimatedBucketCount,
            InternalOrder order, int requiredSize, int shardSize, long minDocCount, AggregationContext aggregationContext, Aggregator parent) {
        super(name, factories, estimatedBucketCount, aggregationContext, parent, order, requiredSize, shardSize, minDocCount);
        this.valuesSource = valuesSource;
    }

    protected long createBucketOrd(long termOrd) {
        return termOrd;
    }

    protected long getBucketOrd(long termOrd) {
        return termOrd;
    }

    @Override
    public boolean shouldCollect() {
        return true;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        globalValues = valuesSource.globalBytesValues();
        globalOrdinals = globalValues.ordinals();
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        final int numOrds = globalOrdinals.setDocument(doc);
        for (int i = 0; i < numOrds; i++) {
            final long globalOrd = globalOrdinals.nextOrd();
            collectBucket(doc, createBucketOrd(globalOrd));
        }
    }

    private static void copy(BytesRef from, BytesRef to) {
        if (to.bytes.length < from.length) {
            to.bytes = new byte[ArrayUtil.oversize(from.length, RamUsageEstimator.NUM_BYTES_BYTE)];
        }
        to.offset = 0;
        to.length = from.length;
        System.arraycopy(from.bytes, from.offset, to.bytes, 0, from.length);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (globalOrdinals == null) { // no context in this reader
            return buildEmptyAggregation();
        }

        final int size;
        if (minDocCount == 0) {
            // if minDocCount == 0 then we can end up with more buckets then maxBucketOrd() returns
            size = (int) Math.min(globalOrdinals.getMaxOrd(), shardSize);
        } else {
            size = (int) Math.min(maxBucketOrd(), shardSize);
        }
        BucketPriorityQueue ordered = new BucketPriorityQueue(size, order.comparator(this));
        StringTerms.Bucket spare = null;
        for (long termOrd = Ordinals.MIN_ORDINAL; termOrd < globalOrdinals.getMaxOrd(); ++termOrd) {
            final long bucketOrd = getBucketOrd(termOrd);
            final long bucketDocCount = bucketOrd < 0 ? 0 : bucketDocCount(bucketOrd);
            if (minDocCount > 0 && bucketDocCount == 0) {
                continue;
            }
            if (spare == null) {
                spare = new StringTerms.Bucket(new BytesRef(), 0, null);
            }
            spare.bucketOrd = bucketOrd;
            spare.docCount = bucketDocCount;
            copy(globalValues.getValueByOrd(termOrd), spare.termBytes);
            spare = (StringTerms.Bucket) ordered.insertWithOverflow(spare);
        }

        final InternalTerms.Bucket[] list = new InternalTerms.Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; --i) {
            final StringTerms.Bucket bucket = (StringTerms.Bucket) ordered.pop();
            bucket.aggregations = bucket.docCount == 0 ? bucketEmptyAggregations() : bucketAggregations(bucket.bucketOrd);
            list[i] = bucket;
        }

        return new StringTerms(name, order, requiredSize, minDocCount, Arrays.asList(list));
    }

    /**
     * Variant of {@link GlobalOrdinalsStringTermsAggregator} that rebases hashes in order to make them dense. Might be
     * useful in case few hashes are visited.
     */
    public static class WithHash extends GlobalOrdinalsStringTermsAggregator {

        private final LongHash bucketOrds;

        public WithHash(String name, AggregatorFactories factories, ValuesSource.Bytes.WithOrdinals.FieldData valuesSource, long estimatedBucketCount,
                InternalOrder order, int requiredSize, int shardSize, long minDocCount, AggregationContext aggregationContext,
                Aggregator parent) {
            super(name, factories, valuesSource, estimatedBucketCount, order, requiredSize, shardSize, minDocCount, aggregationContext, parent);
            bucketOrds = new LongHash(estimatedBucketCount, aggregationContext.bigArrays());
        }

        @Override
        protected long createBucketOrd(long termOrd) {
            long bucketOrd = bucketOrds.add(termOrd);
            if (bucketOrd < 0) {
                bucketOrd = -1 - bucketOrd;
            }
            return bucketOrd;
        }

        @Override
        protected long getBucketOrd(long termOrd) {
            return bucketOrds.find(termOrd);
        }

        @Override
        protected void doClose() {
            Releasables.close(bucketOrds);
        }

    }

}
