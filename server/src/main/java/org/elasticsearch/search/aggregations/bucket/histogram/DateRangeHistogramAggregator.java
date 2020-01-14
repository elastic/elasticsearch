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
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.mapper.RangeType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
    private final Rounding shardRounding;
    private final BucketOrder order;
    private final boolean keyed;

    private final long minDocCount;
    private final ExtendedBounds extendedBounds;

    private final LongHash bucketOrds;

    DateRangeHistogramAggregator(String name, AggregatorFactories factories, Rounding rounding, Rounding shardRounding,
                                 BucketOrder order, boolean keyed,
                                 long minDocCount, @Nullable ExtendedBounds extendedBounds, @Nullable ValuesSource.Range valuesSource,
                                 DocValueFormat formatter, SearchContext aggregationContext,
                                 Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                                 Map<String, Object> metaData) throws IOException {

        super(name, factories, aggregationContext, parent, pipelineAggregators, metaData);
        this.rounding = rounding;
        this.shardRounding = shardRounding;
        this.order = InternalOrder.validate(order, this);
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.extendedBounds = extendedBounds;
        this.valuesSource = valuesSource;
        this.formatter = formatter;

        bucketOrds = new LongHash(1, aggregationContext.bigArrays());
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
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
                    long previousKey = Long.MIN_VALUE;

                    for (int i = 0; i < valuesCount; i++) {
                        BytesRef encodedRanges = values.nextValue();
                        List<RangeFieldMapper.Range> ranges = rangeType.decodeRanges(encodedRanges);
                        long previousFrom = Long.MIN_VALUE;
                        for (RangeFieldMapper.Range range : ranges) {
                            final Long from = (Long) range.getFrom();
                            // The encoding should ensure that this assert is always true.
                            assert from >= previousFrom : "Start of range not >= previous start";
                            final Long to = (Long) range.getTo();
                            final long startKey = shardRounding.round(from);
                            final long endKey = shardRounding.round(to);
                            for (long  key = startKey > previousKey ? startKey : previousKey; key <= endKey;
                                 key = shardRounding.nextRoundingValue(key)) {
                                if (key == previousKey) {
                                    continue;
                                }
                                // Bucket collection identical to NumericHistogramAggregator, could be refactored
                                long bucketOrd = bucketOrds.add(key);
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
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        consumeBucketsAndMaybeBreak((int) bucketOrds.size());

        List<InternalDateHistogram.Bucket> buckets = new ArrayList<>((int) bucketOrds.size());
        for (long i = 0; i < bucketOrds.size(); i++) {
            buckets.add(new InternalDateHistogram.Bucket(bucketOrds.get(i), bucketDocCount(i), keyed, formatter, bucketAggregations(i)));
        }

        // the contract of the histogram aggregation is that shards must return buckets ordered by key in ascending order
        CollectionUtil.introSort(buckets, BucketOrder.key(true).comparator(this));

        // value source will be null for unmapped fields
        // Important: use `rounding` here, not `shardRounding`
        InternalDateHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0
                ? new InternalDateHistogram.EmptyBucketInfo(rounding, buildEmptySubAggregations(), extendedBounds)
                : null;
        return new InternalDateHistogram(name, buckets, order, minDocCount, rounding.offset(), emptyBucketInfo, formatter, keyed,
                pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalDateHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0
                ? new InternalDateHistogram.EmptyBucketInfo(rounding, buildEmptySubAggregations(), extendedBounds)
                : null;
        return new InternalDateHistogram(name, Collections.emptyList(), order, minDocCount, rounding.offset(), emptyBucketInfo, formatter,
                keyed, pipelineAggregators(), metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }
}
