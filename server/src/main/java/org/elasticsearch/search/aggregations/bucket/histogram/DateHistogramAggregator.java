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
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * An aggregator for date values. Every date is rounded down using a configured
 * {@link Rounding}.
 *
 * @see Rounding
 */
class DateHistogramAggregator extends BucketsAggregator {

    private final ValuesSource.Numeric valuesSource;
    private final DocValueFormat formatter;
    private final Rounding rounding;
    /**
     * The rounding prepared for rewriting the data in the shard.
     */
    private final Rounding.Prepared preparedRounding;
    private final BucketOrder order;
    private final boolean keyed;

    private final long minDocCount;
    private final LongBounds extendedBounds;
    private final LongBounds hardBounds;

    private final LongKeyedBucketOrds bucketOrds;

    DateHistogramAggregator(
        String name,
        AggregatorFactories factories,
        Rounding rounding,
        Rounding.Prepared preparedRounding,
        BucketOrder order,
        boolean keyed,
        long minDocCount,
        @Nullable LongBounds extendedBounds,
        @Nullable LongBounds hardBounds,
        ValuesSourceConfig valuesSourceConfig,
        SearchContext aggregationContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {

        super(name, factories, aggregationContext, parent, CardinalityUpperBound.MANY, metadata);
        this.rounding = rounding;
        this.preparedRounding = preparedRounding;
        this.order = order;
        order.validate(this);
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.extendedBounds = extendedBounds;
        this.hardBounds = hardBounds;
        // TODO: Stop using null here
        this.valuesSource = valuesSourceConfig.hasValues() ? (ValuesSource.Numeric) valuesSourceConfig.getValuesSource() : null;
        this.formatter = valuesSourceConfig.format();

        bucketOrds = LongKeyedBucketOrds.build(context.bigArrays(), cardinality);
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
        SortedNumericDocValues values = valuesSource.longValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (values.advanceExact(doc)) {
                    int valuesCount = values.docValueCount();

                    long previousRounded = Long.MIN_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        long value = values.nextValue();
                        long rounded = preparedRounding.round(value);
                        assert rounded >= previousRounded;
                        if (rounded == previousRounded) {
                            continue;
                        }
                        if (hardBounds == null || hardBounds.contain(rounded)) {
                            long bucketOrd = bucketOrds.add(owningBucketOrd, rounded);
                            if (bucketOrd < 0) { // already seen
                                bucketOrd = -1 - bucketOrd;
                                collectExistingBucket(sub, doc, bucketOrd);
                            } else {
                                collectBucket(sub, doc, bucketOrd);
                            }
                        }
                        previousRounded = rounded;
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForVariableBuckets(owningBucketOrds, bucketOrds,
            (bucketValue, docCount, subAggregationResults) -> {
                return new InternalDateHistogram.Bucket(bucketValue, docCount, keyed, formatter, subAggregationResults);
            }, (owningBucketOrd, buckets) -> {
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
