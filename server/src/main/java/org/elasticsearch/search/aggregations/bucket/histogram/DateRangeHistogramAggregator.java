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
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.lease.Releasables;
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
import org.elasticsearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * An aggregator for date values. Every date is rounded down using a configured
 * {@link Rounding}.
 *
 * @see Rounding
 */
class DateRangeHistogramAggregator extends BucketsAggregator {

    private final ValuesSource.Range valuesSource;
    private final DocValueFormat formatter;
    private final Rounding rounding;
    /**
     * The rounding prepared for rewriting the data in the shard.
     */
    private final Rounding.Prepared preparedRounding;
    private final BucketOrder order;
    private final boolean keyed;

    private final long minDocCount;
    private final ExtendedBounds extendedBounds;

    private final LongKeyedBucketOrds bucketOrds;

    DateRangeHistogramAggregator(String name, AggregatorFactories factories, Rounding rounding, Rounding.Prepared preparedRounding,
                                 BucketOrder order, boolean keyed,
                                 long minDocCount, @Nullable ExtendedBounds extendedBounds, @Nullable ValuesSource valuesSource,
                                 DocValueFormat formatter, SearchContext aggregationContext,
                                 Aggregator parent, boolean collectsFromSingleBucket, Map<String, Object> metadata) throws IOException {

        super(name, factories, aggregationContext, parent, metadata);
        this.rounding = rounding;
        this.preparedRounding = preparedRounding;
        this.order = order;
        order.validate(this);
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.extendedBounds = extendedBounds;
        this.valuesSource = (ValuesSource.Range) valuesSource;
        this.formatter = formatter;
        if (this.valuesSource.rangeType() != RangeType.DATE) {
            throw new IllegalArgumentException("Expected date range type but found range type [" + this.valuesSource.rangeType().name
                + "]");
        }

        bucketOrds = LongKeyedBucketOrds.build(context.bigArrays(), collectsFromSingleBucket);
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
        RangeType rangeType = valuesSource.rangeType();
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (values.advanceExact(doc)) {
                    // Is it possible for valuesCount to be > 1 here? Multiple ranges are encoded into the same BytesRef in the binary doc
                    // values, so it isn't clear what we'd be iterating over.
                    int valuesCount = values.docValueCount();
                    assert valuesCount == 1 : "Value count for ranges should always be 1";
                    long previousKey = Long.MIN_VALUE;

                    for (int i = 0; i < valuesCount; i++) {
                        BytesRef encodedRanges = values.nextValue();
                        List<RangeFieldMapper.Range> ranges = rangeType.decodeRanges(encodedRanges);
                        long previousFrom = Long.MIN_VALUE;
                        for (RangeFieldMapper.Range range : ranges) {
                            Long from = (Long) range.getFrom();
                            // The encoding should ensure that this assert is always true.
                            assert from >= previousFrom : "Start of range not >= previous start";
                            final Long to = (Long) range.getTo();
                            final long startKey = preparedRounding.round(from);
                            final long endKey = preparedRounding.round(to);
                            for (long  key = startKey > previousKey ? startKey : previousKey; key <= endKey;
                                 key = preparedRounding.nextRoundingValue(key)) {
                                if (key == previousKey) {
                                    continue;
                                }
                                // Bucket collection identical to NumericHistogramAggregator, could be refactored
                                long bucketOrd = bucketOrds.add(owningBucketOrd, key);
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

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForVariableBuckets(owningBucketOrds, bucketOrds,
            (bucketValue, docCount, subAggregationResults) ->
                new InternalDateHistogram.Bucket(bucketValue, docCount, keyed, formatter, subAggregationResults),
            buckets -> {
                // the contract of the histogram aggregation is that shards must return buckets ordered by key in ascending order
                CollectionUtil.introSort(buckets, BucketOrder.key(true).comparator());

                // value source will be null for unmapped fields
                // Important: use `rounding` here, not `shardRounding`
                InternalDateHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0
                        ? new InternalDateHistogram.EmptyBucketInfo(rounding.withoutOffset(), buildEmptySubAggregations(), extendedBounds)
                        : null;
                return new InternalDateHistogram(name, buckets, order, minDocCount, rounding.offset(), emptyBucketInfo, formatter,
                        keyed, metadata());
            });
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalDateHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0
                ? new InternalDateHistogram.EmptyBucketInfo(rounding, buildEmptySubAggregations(), extendedBounds)
                : null;
        return new InternalDateHistogram(name, Collections.emptyList(), order, minDocCount, rounding.offset(), emptyBucketInfo, formatter,
                keyed, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        add.accept("total_buckets", bucketOrds.size());
    }
}
