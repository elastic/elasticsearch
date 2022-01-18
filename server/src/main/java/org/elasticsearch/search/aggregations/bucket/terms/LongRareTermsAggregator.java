/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.SetBackedScalingCuckooFilter;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
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

    protected SortedNumericDocValues getValues(ValuesSource.Numeric valuesSource, LeafReaderContext ctx) throws IOException {
        return valuesSource.longValues(ctx);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        SortedNumericDocValues values = getValues(valuesSource, ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int docId, long owningBucketOrd) throws IOException {
                if (false == values.advanceExact(docId)) {
                    return;
                }
                int valuesCount = values.docValueCount();
                long previous = Long.MAX_VALUE;
                for (int i = 0; i < valuesCount; ++i) {
                    long val = values.nextValue();
                    if (i == 0 && previous == val) {
                        continue;
                    }
                    previous = val;
                    if (filter != null && false == filter.accept(val)) {
                        continue;
                    }
                    long bucketOrdinal = bucketOrds.add(owningBucketOrd, val);
                    if (bucketOrdinal < 0) { // already seen
                        bucketOrdinal = -1 - bucketOrdinal;
                        collectExistingBucket(sub, docId, bucketOrdinal);
                    } else {
                        collectBucket(sub, docId, bucketOrdinal);
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        /*
         * Collect the list of buckets, populate the filter with terms
         * that are too frequent, and figure out how to merge sub-buckets.
         */
        LongRareTerms.Bucket[][] rarestPerOrd = new LongRareTerms.Bucket[owningBucketOrds.length][];
        SetBackedScalingCuckooFilter[] filters = new SetBackedScalingCuckooFilter[owningBucketOrds.length];
        long keepCount = 0;
        long[] mergeMap = new long[(int) bucketOrds.size()];
        Arrays.fill(mergeMap, -1);
        long offset = 0;
        for (int owningOrdIdx = 0; owningOrdIdx < owningBucketOrds.length; owningOrdIdx++) {
            try (LongHash bucketsInThisOwningBucketToCollect = new LongHash(1, bigArrays())) {
                filters[owningOrdIdx] = newFilter();
                List<LongRareTerms.Bucket> builtBuckets = new ArrayList<>();
                LongKeyedBucketOrds.BucketOrdsEnum collectedBuckets = bucketOrds.ordsEnum(owningBucketOrds[owningOrdIdx]);
                while (collectedBuckets.next()) {
                    long docCount = bucketDocCount(collectedBuckets.ord());
                    // if the key is below threshold, reinsert into the new ords
                    if (docCount <= maxDocCount) {
                        LongRareTerms.Bucket bucket = new LongRareTerms.Bucket(collectedBuckets.value(), docCount, null, format);
                        bucket.bucketOrd = offset + bucketsInThisOwningBucketToCollect.add(collectedBuckets.value());
                        mergeMap[(int) collectedBuckets.ord()] = bucket.bucketOrd;
                        builtBuckets.add(bucket);
                        keepCount++;
                    } else {
                        filters[owningOrdIdx].add(collectedBuckets.value());
                    }
                }
                rarestPerOrd[owningOrdIdx] = builtBuckets.toArray(LongRareTerms.Bucket[]::new);
                offset += bucketsInThisOwningBucketToCollect.size();
            }
        }

        /*
         * Only merge/delete the ordinals if we have actually deleted one,
         * to save on some redundant work.
         */
        if (keepCount != mergeMap.length) {
            LongUnaryOperator howToMerge = b -> mergeMap[(int) b];
            rewriteBuckets(offset, howToMerge);
            if (deferringCollector() != null) {
                ((BestBucketsDeferringCollector) deferringCollector()).rewriteBuckets(howToMerge);
            }
        }

        /*
         * Now build the results!
         */
        buildSubAggsForAllBuckets(rarestPerOrd, b -> b.bucketOrd, (b, aggs) -> b.aggregations = aggs);
        InternalAggregation[] result = new InternalAggregation[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            Arrays.sort(rarestPerOrd[ordIdx], ORDER.comparator());
            result[ordIdx] = new LongRareTerms(
                name,
                ORDER,
                metadata(),
                format,
                Arrays.asList(rarestPerOrd[ordIdx]),
                maxDocCount,
                filters[ordIdx]
            );
        }
        return result;
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
