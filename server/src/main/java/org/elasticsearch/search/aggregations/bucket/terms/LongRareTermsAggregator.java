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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.SetBackedScalingCuckooFilter;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
        SearchContext aggregationContext,
        Aggregator parent,
        IncludeExclude.LongFilter filter,
        int maxDocCount,
        double precision,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(
            name,
            factories,
            aggregationContext,
            parent,
            metadata,
            maxDocCount,
            precision,
            format
        );
        this.valuesSource = valuesSource;
        this.filter = filter;
        this.bucketOrds = LongKeyedBucketOrds.build(context.bigArrays(), cardinality);
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
            try (LongHash bucketsInThisOwningBucketToCollect = new LongHash(1, context.bigArrays())) {
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
            mergeBuckets(mergeMap, offset);
            if (deferringCollector != null) {
                deferringCollector.mergeBuckets(mergeMap);
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
