/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.BucketAndOrd;
import org.elasticsearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongConsumer;

/**
 * Aggregates data expressed as longs (for efficiency's sake) but formats results as aggregation-specific strings.
 */
public abstract class GeoGridAggregator<T extends InternalGeoGrid<?>> extends BucketsAggregator {

    protected final int requiredSize;
    protected final int shardSize;
    protected final ValuesSource.Numeric valuesSource;
    protected final LongKeyedBucketOrds bucketOrds;

    @SuppressWarnings("this-escape")
    protected GeoGridAggregator(
        String name,
        AggregatorFactories factories,
        Function<LongConsumer, ValuesSource.Numeric> valuesSource,
        int requiredSize,
        int shardSize,
        AggregationContext aggregationContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, aggregationContext, parent, CardinalityUpperBound.MANY, metadata);
        this.valuesSource = valuesSource.apply(this::addRequestCircuitBreakerBytes);
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        bucketOrds = LongKeyedBucketOrds.build(bigArrays(), cardinality);
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    public LeafBucketCollector getLeafCollector(final AggregationExecutionContext aggCtx, final LeafBucketCollector sub)
        throws IOException {
        final SortedNumericDocValues values = valuesSource.longValues(aggCtx.getLeafReaderContext());
        final NumericDocValues singleton = DocValues.unwrapSingleton(values);
        return singleton != null ? getLeafCollector(singleton, sub) : getLeafCollector(values, sub);
    }

    private LeafBucketCollector getLeafCollector(final NumericDocValues values, final LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (values.advanceExact(doc)) {
                    final long val = values.longValue();
                    long bucketOrdinal = bucketOrds.add(owningBucketOrd, val);
                    if (bucketOrdinal < 0) { // already seen
                        bucketOrdinal = -1 - bucketOrdinal;
                        collectExistingBucket(sub, doc, bucketOrdinal);
                    } else {
                        collectBucket(sub, doc, bucketOrdinal);
                    }
                }
            }
        };
    }

    private LeafBucketCollector getLeafCollector(final SortedNumericDocValues values, final LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();

                    long previous = Long.MAX_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        final long val = values.nextValue();
                        if (previous != val || i == 0) {
                            long bucketOrdinal = bucketOrds.add(owningBucketOrd, val);
                            if (bucketOrdinal < 0) { // already seen
                                bucketOrdinal = -1 - bucketOrdinal;
                                collectExistingBucket(sub, doc, bucketOrdinal);
                            } else {
                                collectBucket(sub, doc, bucketOrdinal);
                            }
                            previous = val;
                        }
                    }
                }
            }
        };
    }

    protected abstract T buildAggregation(String name, int requiredSize, List<InternalGeoGridBucket> buckets, Map<String, Object> metadata);

    /**
     * This method is used to return a re-usable instance of the bucket when building
     * the aggregation.
     * @return a new {@link InternalGeoGridBucket} implementation with empty parameters
     */
    protected abstract InternalGeoGridBucket newEmptyBucket();

    @Override
    public InternalAggregation[] buildAggregations(LongArray owningBucketOrds) throws IOException {

        try (ObjectArray<InternalGeoGridBucket[]> topBucketsPerOrd = bigArrays().newObjectArray(owningBucketOrds.size())) {
            try (IntArray bucketsSizePerOrd = bigArrays().newIntArray(owningBucketOrds.size())) {
                long ordsToCollect = 0;
                for (long ordIdx = 0; ordIdx < owningBucketOrds.size(); ordIdx++) {
                    int size = (int) Math.min(bucketOrds.bucketsInOrd(owningBucketOrds.get(ordIdx)), shardSize);
                    ordsToCollect += size;
                    bucketsSizePerOrd.set(ordIdx, size);
                }
                try (LongArray ordsArray = bigArrays().newLongArray(ordsToCollect)) {
                    long ordsCollected = 0;
                    for (long ordIdx = 0; ordIdx < topBucketsPerOrd.size(); ordIdx++) {
                        try (
                            BucketPriorityQueue<BucketAndOrd<InternalGeoGridBucket>, InternalGeoGridBucket> ordered =
                                new BucketPriorityQueue<>(bucketsSizePerOrd.get(ordIdx), bigArrays(), b -> b.bucket)
                        ) {
                            BucketAndOrd<InternalGeoGridBucket> spare = null;
                            LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds.get(ordIdx));
                            while (ordsEnum.next()) {
                                if (spare == null) {
                                    checkRealMemoryCBForInternalBucket();
                                    spare = new BucketAndOrd<>(newEmptyBucket());
                                }

                                // need a special function to keep the source bucket
                                // up-to-date so it can get the appropriate key
                                spare.bucket.hashAsLong = ordsEnum.value();
                                spare.bucket.docCount = bucketDocCount(ordsEnum.ord());
                                spare.ord = ordsEnum.ord();
                                spare = ordered.insertWithOverflow(spare);
                            }
                            final int orderedSize = (int) ordered.size();
                            final InternalGeoGridBucket[] buckets = new InternalGeoGridBucket[orderedSize];
                            for (int i = orderedSize - 1; i >= 0; --i) {
                                BucketAndOrd<InternalGeoGridBucket> bucketBucketAndOrd = ordered.pop();
                                buckets[i] = bucketBucketAndOrd.bucket;
                                ordsArray.set(ordsCollected + i, bucketBucketAndOrd.ord);
                            }
                            topBucketsPerOrd.set(ordIdx, buckets);
                            ordsCollected += orderedSize;
                        }
                    }
                    assert ordsCollected == ordsArray.size();
                    buildSubAggsForAllBuckets(topBucketsPerOrd, ordsArray, (b, aggs) -> b.aggregations = aggs);
                }
            }
            return buildAggregations(
                Math.toIntExact(owningBucketOrds.size()),
                ordIdx -> buildAggregation(name, requiredSize, Arrays.asList(topBucketsPerOrd.get(ordIdx)), metadata())
            );
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return buildAggregation(name, requiredSize, Collections.emptyList(), metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }
}
