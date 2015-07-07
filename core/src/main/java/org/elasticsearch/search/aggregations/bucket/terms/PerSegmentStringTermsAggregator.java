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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.bucket.terms.support.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * An aggregator for string terms that computes and ord to count map for each
 * segment, and merges counts in {@link #buildAggregation(long)}.
 */
final class PerSegmentStringTermsAggregator extends AbstractStringTermsAggregator {

    private final ValuesSource.Bytes.WithOrdinals valuesSource;
    private final List<Tuple<RandomAccessOrds, IntArray>> segmentCounts;

    PerSegmentStringTermsAggregator(
            String name, ValuesSource.Bytes.WithOrdinals valuesSource, AggregationContext context,
            Aggregator parent, Order order, BucketCountThresholds bucketCountThresholds,
            boolean showTermDocCountError, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) throws IOException {

        super(name, AggregatorFactories.EMPTY, context, parent, order, bucketCountThresholds, SubAggCollectionMode.DEPTH_FIRST, showTermDocCountError, pipelineAggregators, metaData);

        this.valuesSource = valuesSource;
        this.segmentCounts = new ArrayList<>();
    }

    @Override
    protected void doClose() {
        List<IntArray> arrays = new ArrayList<>();
        for (Tuple<RandomAccessOrds, IntArray> tuple : segmentCounts) {
            arrays.add(tuple.v2());
        }
        Releasables.close(arrays);
        segmentCounts.clear();
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        final RandomAccessOrds values = valuesSource.ordinalsValues(ctx);
        final IntArray counts = context.bigArrays().newIntArray(values.getValueCount() + 1);
        segmentCounts.add(new Tuple<>(values, counts));
        final SortedDocValues singleton = DocValues.unwrapSingleton(values);
        if (singleton != null) {
            return getSingleLeafCollector(counts, singleton);
        } else {
            return getMultiLeafCollector(counts, values);
        }
    }

    private LeafBucketCollector getSingleLeafCollector(final IntArray counts, final SortedDocValues values) {
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0;
                final int ord = values.getOrd(doc);
                counts.increment(ord + 1, 1);
            }
        };
    }

    private LeafBucketCollector getMultiLeafCollector(final IntArray counts, final SortedSetDocValues values) {
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0;
                values.setDocument(doc);
                for (long ord = values.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = values.nextOrd()) {
                    counts.increment(ord + 1, 1);
                }
            }
        };
    }

    private long sumValueCount() {
        long sum = 0;
        for (Tuple<RandomAccessOrds, IntArray> tuple : segmentCounts) {
            sum += tuple.v1().getValueCount();
        }
        return sum;
    }

    private static class TermCount {

        final IntArray counts;
        final TermsEnum termsEnum;
        final boolean includeZeros;

        BytesRef term;
        int count;
        long ord = 0;

        TermCount(TermsEnum termsEnum, IntArray counts, boolean includeZeros) {
            this.termsEnum = termsEnum;
            this.counts = counts;
            this.includeZeros = includeZeros;
        }

        boolean moveToNext() throws IOException {
            for (ord = ord + 1; ord < counts.size(); ++ord) {
                count = counts.get(ord);
                if (includeZeros || count > 0) {
                    termsEnum.seekExact(ord - 1);
                    term = termsEnum.term();
                    return true;
                }
            }
            return false;
        }
    }

    private static class Bucket extends InternalTerms.Bucket {

        private final BytesRefBuilder term;

        Bucket(boolean showDocCountError) {
            super(0, InternalAggregations.EMPTY, showDocCountError, 0, null);
            this.term = new BytesRefBuilder();
        }

        @Override
        public Object getKey() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getKeyAsString() {
            throw new UnsupportedOperationException();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        InternalTerms.Bucket newBucket(long docCount, InternalAggregations aggs,
                long docCountError) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Number getKeyAsNumber() {
            throw new UnsupportedOperationException();
        }

        @Override
        int compareTerm(Terms.Bucket other) {
            return term.get().compareTo(((Bucket) other).term.get());
        }
        
    }

    @Override
    public InternalAggregation buildAggregation(long bucketOrd) throws IOException {
        assert bucketOrd == 0;

        final int size = (int) Math.min(sumValueCount(), bucketCountThresholds.getShardSize());
        final BucketPriorityQueue buckets = new BucketPriorityQueue(size, order.comparator(this));

        final PriorityQueue<TermCount> pq = new PriorityQueue<TermCount>(segmentCounts.size()) {
            @Override
            protected boolean lessThan(TermCount a, TermCount b) {
                return a.term.compareTo(b.term) < 0;
            }
        };
        for (Tuple<RandomAccessOrds, IntArray> tuple : segmentCounts) {
            final TermCount termCount = new TermCount(tuple.v1().termsEnum(), tuple.v2(), bucketCountThresholds.getShardMinDocCount() == 0);
            if (termCount.moveToNext()) {
                pq.add(termCount);
            }
        }

        long sumDocCount = 0;
        Bucket spare = null;
        while (pq.size() > 0) {
            if (spare == null) {
                spare = new Bucket(showTermDocCountError);
            }
            TermCount top = pq.top();
            spare.term.copyBytes(top.term);
            spare.docCount = top.count;
            while (true) {
                if (top.moveToNext()) {
                    top = pq.updateTop();
                } else {
                    pq.pop();
                    if (pq.size() == 0) {
                        break;
                    }
                    top = pq.top();
                }
                if (spare.term.get().bytesEquals(top.term) == false) {
                    break;
                }
                spare.docCount += top.count;
            }

            sumDocCount += spare.docCount;
            if (spare.docCount >= bucketCountThresholds.getShardMinDocCount()) {
                spare = (Bucket) buckets.insertWithOverflow(spare);
            }
        }

        final InternalTerms.Bucket[] list = new InternalTerms.Bucket[buckets.size()];
        for (int i = buckets.size() - 1; i >= 0; --i) {
            Bucket bucket = (Bucket) buckets.pop();
            list[i] = new StringTerms.Bucket(bucket.term.get(), bucket.docCount, InternalAggregations.EMPTY, showTermDocCountError, 0);
        }

        long sumOtherDocCount = sumDocCount;
        for (Terms.Bucket b : list) {
            sumOtherDocCount -= b.getDocCount();
        }

        return new StringTerms(name, order, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getShardSize(),
                bucketCountThresholds.getMinDocCount(), Arrays.asList(list), showTermDocCountError, 0, sumOtherDocCount,
                pipelineAggregators(), metaData());
    }

}
