/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Aggregates data expressed as longs (for efficiency's sake) but formats results as aggregation-specific strings.
 */
public abstract class GeoGridAggregator<T extends InternalGeoGrid<?>> extends BucketsAggregator {

    protected final int requiredSize;
    protected final int shardSize;
    protected final ValuesSource.Numeric valuesSource;
    protected final LongKeyedBucketOrds bucketOrds;

    GeoGridAggregator(String name, AggregatorFactories factories, ValuesSource.Numeric valuesSource,
                      int requiredSize, int shardSize, AggregationContext aggregationContext,
                      Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata) throws IOException {
        super(name, factories, aggregationContext, parent, CardinalityUpperBound.MANY, metadata);
        this.valuesSource = valuesSource;
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
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        SortedNumericDocValues values = valuesSource.longValues(ctx);
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

    abstract T buildAggregation(String name, int requiredSize, List<InternalGeoGridBucket> buckets, Map<String, Object> metadata);

    /**
     * This method is used to return a re-usable instance of the bucket when building
     * the aggregation.
     * @return a new {@link InternalGeoGridBucket} implementation with empty parameters
     */
    abstract InternalGeoGridBucket newEmptyBucket();

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        InternalGeoGridBucket[][] topBucketsPerOrd = new InternalGeoGridBucket[owningBucketOrds.length][];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            int size = (int) Math.min(bucketOrds.bucketsInOrd(owningBucketOrds[ordIdx]), shardSize);

            BucketPriorityQueue<InternalGeoGridBucket> ordered = new BucketPriorityQueue<>(size);
            InternalGeoGridBucket spare = null;
            LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds[ordIdx]);
            while (ordsEnum.next()) {
                if (spare == null) {
                    spare = newEmptyBucket();
                }

                // need a special function to keep the source bucket
                // up-to-date so it can get the appropriate key
                spare.hashAsLong = ordsEnum.value();
                spare.docCount = bucketDocCount(ordsEnum.ord());
                spare.bucketOrd = ordsEnum.ord();
                spare = ordered.insertWithOverflow(spare);
            }

            topBucketsPerOrd[ordIdx] = new InternalGeoGridBucket[ordered.size()];
            for (int i = ordered.size() - 1; i >= 0; --i) {
                topBucketsPerOrd[ordIdx][i] = ordered.pop();
            }
        }
        buildSubAggsForAllBuckets(topBucketsPerOrd, b -> b.bucketOrd, (b, aggs) -> b.aggregations = aggs);
        InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            results[ordIdx] = buildAggregation(name, requiredSize, Arrays.asList(topBucketsPerOrd[ordIdx]), metadata());
        }
        return results;
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
