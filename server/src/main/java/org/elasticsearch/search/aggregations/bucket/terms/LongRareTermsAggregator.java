/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.common.util.SetBackedScalingCuckooFilter;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BestBucketsDeferringCollector;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.LongUnaryOperator;

import static java.util.Collections.emptyList;

/**
 * An aggregator that finds "rare" string values (e.g. terms agg that orders ascending)
 */
public class LongRareTermsAggregator extends AbstractRareTermsAggregator {
    private final ValuesSource.Numeric valuesSource;
    private final IncludeExclude.LongFilter filter;
    private final LongKeyedBucketOrds bucketOrds;

    LongRareTermsAggregator(
        String name,
        AggregatorFactories factories,
        ValuesSource.Numeric valuesSource,
        DocValueFormat format,
        AggregationContext aggregationContext,
        Aggregator parent,
        IncludeExclude.LongFilter filter,
        int maxDocCount,
        double precision,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, aggregationContext, parent, metadata, maxDocCount, precision, format);
        this.valuesSource = valuesSource;
        this.filter = filter;
        this.bucketOrds = LongKeyedBucketOrds.build(bigArrays(), cardinality);
    }

    protected static SortedNumericDocValues getValues(ValuesSource.Numeric valuesSource, LeafReaderContext ctx) throws IOException {
        return valuesSource.longValues(ctx);
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        final SortedNumericDocValues values = getValues(valuesSource, aggCtx.getLeafReaderContext());
        final NumericDocValues singleton = DocValues.unwrapSingleton(values);
        return singleton != null ? getLeafCollector(singleton, sub) : getLeafCollector(values, sub);
    }

    private LeafBucketCollector getLeafCollector(SortedNumericDocValues values, LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int docId, long owningBucketOrd) throws IOException {
                if (values.advanceExact(docId)) {
                    long previous = Long.MAX_VALUE;
                    for (int i = 0; i < values.docValueCount(); ++i) {
                        long val = values.nextValue();
                        if (i == 0 && previous == val) {
                            continue;
                        }
                        collectValue(val, docId, owningBucketOrd, sub);
                        previous = val;
                    }
                }
            }
        };
    }

    private LeafBucketCollector getLeafCollector(NumericDocValues values, LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int docId, long owningBucketOrd) throws IOException {
                if (values.advanceExact(docId)) {
                    collectValue(values.longValue(), docId, owningBucketOrd, sub);
                }
            }
        };
    }

    private void collectValue(long val, int docId, long owningBucketOrd, LeafBucketCollector sub) throws IOException {
        if (filter == null || filter.accept(val)) {
            long bucketOrdinal = bucketOrds.add(owningBucketOrd, val);
            if (bucketOrdinal < 0) { // already seen
                bucketOrdinal = -1 - bucketOrdinal;
                collectExistingBucket(sub, docId, bucketOrdinal);
            } else {
                collectBucket(sub, docId, bucketOrdinal);
            }
        }

    }

    @Override
    public InternalAggregation[] buildAggregations(LongArray owningBucketOrds) throws IOException {
        /*
         * Collect the list of buckets, populate the filter with terms
         * that are too frequent, and figure out how to merge sub-buckets.
         */
        try (
            ObjectArray<LongRareTerms.Bucket[]> rarestPerOrd = bigArrays().newObjectArray(owningBucketOrds.size());
            ObjectArray<SetBackedScalingCuckooFilter> filters = bigArrays().newObjectArray(owningBucketOrds.size())
        ) {
            try (LongArray mergeMap = bigArrays().newLongArray(bucketOrds.size())) {
                mergeMap.fill(0, mergeMap.size(), -1);
                long keepCount = 0;
                long offset = 0;
                for (long owningOrdIdx = 0; owningOrdIdx < owningBucketOrds.size(); owningOrdIdx++) {
                    try (LongHash bucketsInThisOwningBucketToCollect = new LongHash(1, bigArrays())) {
                        filters.set(owningOrdIdx, newFilter());
                        List<LongRareTerms.Bucket> builtBuckets = new ArrayList<>();
                        LongKeyedBucketOrds.BucketOrdsEnum collectedBuckets = bucketOrds.ordsEnum(owningBucketOrds.get(owningOrdIdx));
                        while (collectedBuckets.next()) {
                            long docCount = bucketDocCount(collectedBuckets.ord());
                            // if the key is below threshold, reinsert into the new ords
                            if (docCount <= maxDocCount) {
                                checkRealMemoryCBForInternalBucket();
                                LongRareTerms.Bucket bucket = new LongRareTerms.Bucket(collectedBuckets.value(), docCount, null, format);
                                bucket.bucketOrd = offset + bucketsInThisOwningBucketToCollect.add(collectedBuckets.value());
                                mergeMap.set(collectedBuckets.ord(), bucket.bucketOrd);
                                builtBuckets.add(bucket);
                                keepCount++;
                            } else {
                                filters.get(owningOrdIdx).add(collectedBuckets.value());
                            }
                        }
                        rarestPerOrd.set(owningOrdIdx, builtBuckets.toArray(LongRareTerms.Bucket[]::new));
                        offset += bucketsInThisOwningBucketToCollect.size();
                    }
                }

                /*
                 * Only merge/delete the ordinals if we have actually deleted one,
                 * to save on some redundant work.
                 */
                if (keepCount != mergeMap.size()) {
                    LongUnaryOperator howToMerge = mergeMap::get;
                    rewriteBuckets(offset, howToMerge);
                    if (deferringCollector() != null) {
                        ((BestBucketsDeferringCollector) deferringCollector()).rewriteBuckets(howToMerge);
                    }
                }
            }

            /*
             * Now build the results!
             */
            buildSubAggsForAllBuckets(rarestPerOrd, b -> b.bucketOrd, (b, aggs) -> b.aggregations = aggs);

            return LongRareTermsAggregator.this.buildAggregations(Math.toIntExact(owningBucketOrds.size()), ordIdx -> {
                LongRareTerms.Bucket[] buckets = rarestPerOrd.get(ordIdx);
                Arrays.sort(buckets, ORDER.comparator());
                return new LongRareTerms(name, ORDER, metadata(), format, Arrays.asList(buckets), maxDocCount, filters.get(ordIdx));
            });
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new LongRareTerms(name, ORDER, metadata(), format, emptyList(), 0, newFilter());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }
}
