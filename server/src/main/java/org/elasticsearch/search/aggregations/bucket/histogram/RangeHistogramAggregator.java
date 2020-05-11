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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.mapper.RangeType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram.EmptyBucketInfo;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RangeHistogramAggregator extends BucketsAggregator {
    private final ValuesSource.Range valuesSource;
    private final DocValueFormat formatter;
    private final double interval, offset;
    private final BucketOrder order;
    private final boolean keyed;
    private final long minDocCount;
    private final double minBound, maxBound;

    private final LongHash bucketOrds;

    public RangeHistogramAggregator(String name, AggregatorFactories factories, double interval, double offset,
                             BucketOrder order, boolean keyed, long minDocCount, double minBound, double maxBound,
                             @Nullable ValuesSource.Range valuesSource, DocValueFormat formatter,
                             SearchContext context, Aggregator parent, Map<String, Object> metadata) throws IOException {

        super(name, factories, context, parent, metadata);
        if (interval <= 0) {
            throw new IllegalArgumentException("interval must be positive, got: " + interval);
        }
        this.interval = interval;
        this.offset = offset;
        this.order = order;
        order.validate(this);
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.minBound = minBound;
        this.maxBound = maxBound;
        this.valuesSource = valuesSource;
        this.formatter = formatter;

        bucketOrds = new LongHash(1, context.bigArrays());
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
        final RangeType rangeType = valuesSource.rangeType();
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0;
                if (values.advanceExact(doc)) {
                    // Is it possible for valuesCount to be > 1 here? Multiple ranges are encoded into the same BytesRef in the binary doc
                    // values, so it isn't clear what we'd be iterating over.
                    final int valuesCount = values.docValueCount();
                    assert valuesCount == 1 : "Value count for ranges should always be 1";
                    double previousKey = Double.NEGATIVE_INFINITY;

                    for (int i = 0; i < valuesCount; i++) {
                        BytesRef encodedRanges = values.nextValue();
                        List<RangeFieldMapper.Range> ranges = rangeType.decodeRanges(encodedRanges);
                        double previousFrom = Double.NEGATIVE_INFINITY;
                        for (RangeFieldMapper.Range range : ranges) {
                            final Double from = rangeType.doubleValue(range.getFrom());
                            // The encoding should ensure that this assert is always true.
                            assert from >= previousFrom : "Start of range not >= previous start";
                            final Double to = rangeType.doubleValue(range.getTo());
                            final double startKey = Math.floor((from - offset) / interval);
                            final double endKey = Math.floor((to - offset) / interval);
                            for (double  key = startKey > previousKey ? startKey : previousKey; key <= endKey; key++) {
                                if (key == previousKey) {
                                    continue;
                                }
                                // Bucket collection identical to NumericHistogramAggregator, could be refactored
                                long bucketOrd = bucketOrds.add(Double.doubleToLongBits(key));
                                if (bucketOrd < 0) { // already seen
                                    bucketOrd = -1 - bucketOrd;
                                    collectExistingBucket(sub, doc, bucketOrd);
                                } else {
                                    collectBucket(sub, doc, bucketOrd);
                                }
                            }
                            if (endKey > previousKey) {
                                previousKey = endKey;
                            }
                        }

                    }
                }
            }
        };
    }

    // TODO: buildAggregations and buildEmptyAggregation are literally just copied out of NumericHistogramAggregator.  We could refactor
    // this to an abstract super class, if we wanted to.  Might be overkill.
    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForVariableBuckets(owningBucketOrds, bucketOrds,
            (bucketValue, docCount, subAggregationResults) -> {
                double roundKey = Double.longBitsToDouble(bucketValue);
                double key = roundKey * interval + offset;
                return new InternalHistogram.Bucket(key, docCount, keyed, formatter, subAggregationResults);
            }, buckets -> {
                // the contract of the histogram aggregation is that shards must return buckets ordered by key in ascending order
                CollectionUtil.introSort(buckets, BucketOrder.key(true).comparator());

                EmptyBucketInfo emptyBucketInfo = null;
                if (minDocCount == 0) {
                    emptyBucketInfo = new EmptyBucketInfo(interval, offset, minBound, maxBound, buildEmptySubAggregations());
                }
                return new InternalHistogram(name, buckets, order, minDocCount, emptyBucketInfo, formatter, keyed, metadata());
            });
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalHistogram.EmptyBucketInfo emptyBucketInfo = null;
        if (minDocCount == 0) {
            emptyBucketInfo = new InternalHistogram.EmptyBucketInfo(interval, offset, minBound, maxBound, buildEmptySubAggregations());
        }
        return new InternalHistogram(name, Collections.emptyList(), order, minDocCount, emptyBucketInfo, formatter, keyed, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }
}
