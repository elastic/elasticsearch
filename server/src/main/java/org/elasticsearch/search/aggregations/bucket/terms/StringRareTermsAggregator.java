/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.common.util.SetBackedScalingCuckooFilter;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
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
public class StringRareTermsAggregator extends AbstractRareTermsAggregator {
    private final ValuesSource.Bytes valuesSource;
    private final IncludeExclude.StringFilter filter;
    private final BytesKeyedBucketOrds bucketOrds;

    StringRareTermsAggregator(
        String name,
        AggregatorFactories factories,
        ValuesSource.Bytes valuesSource,
        DocValueFormat format,
        IncludeExclude.StringFilter filter,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata,
        long maxDocCount,
        double precision,
        CardinalityUpperBound cardinality
    ) throws IOException {
        super(name, factories, context, parent, metadata, maxDocCount, precision, format);
        this.valuesSource = valuesSource;
        this.filter = filter;
        this.bucketOrds = BytesKeyedBucketOrds.build(bigArrays(), cardinality);
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
        final SortedBinaryDocValues values = valuesSource.bytesValues(aggCtx.getLeafReaderContext());
        final BinaryDocValues singleton = FieldData.unwrapSingleton(values);
        return singleton != null ? getLeafCollector(singleton, sub) : getLeafCollector(values, sub);
    }

    private LeafBucketCollector getLeafCollector(SortedBinaryDocValues values, LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, values) {
            final BytesRefBuilder previous = new BytesRefBuilder();

            @Override
            public void collect(int docId, long owningBucketOrd) throws IOException {
                if (values.advanceExact(docId)) {
                    previous.clear();
                    // SortedBinaryDocValues don't guarantee uniqueness so we
                    // need to take care of dups
                    for (int i = 0; i < values.docValueCount(); ++i) {
                        BytesRef bytes = values.nextValue();
                        if (i > 0 && previous.get().equals(bytes)) {
                            continue;
                        }
                        collectValue(bytes, docId, owningBucketOrd, sub);
                        previous.copyBytes(bytes);
                    }
                }

            }
        };
    }

    private LeafBucketCollector getLeafCollector(BinaryDocValues values, LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int docId, long owningBucketOrd) throws IOException {
                if (values.advanceExact(docId)) {
                    collectValue(values.binaryValue(), docId, owningBucketOrd, sub);
                }
            }
        };
    }

    private void collectValue(BytesRef val, int doc, long owningBucketOrd, LeafBucketCollector sub) throws IOException {
        if (filter == null || filter.accept(val)) {
            long bucketOrdinal = bucketOrds.add(owningBucketOrd, val);
            if (bucketOrdinal < 0) { // already seen
                bucketOrdinal = -1 - bucketOrdinal;
                collectExistingBucket(sub, doc, bucketOrdinal);
            } else {
                collectBucket(sub, doc, bucketOrdinal);
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
            ObjectArray<StringRareTerms.Bucket[]> rarestPerOrd = bigArrays().newObjectArray(owningBucketOrds.size());
            ObjectArray<SetBackedScalingCuckooFilter> filters = bigArrays().newObjectArray(owningBucketOrds.size())
        ) {
            try (LongArray mergeMap = bigArrays().newLongArray(bucketOrds.size())) {
                mergeMap.fill(0, mergeMap.size(), -1);
                long keepCount = 0;
                long offset = 0;
                for (long owningOrdIdx = 0; owningOrdIdx < owningBucketOrds.size(); owningOrdIdx++) {
                    try (BytesRefHash bucketsInThisOwningBucketToCollect = new BytesRefHash(1, bigArrays())) {
                        filters.set(owningOrdIdx, newFilter());
                        List<StringRareTerms.Bucket> builtBuckets = new ArrayList<>();
                        BytesKeyedBucketOrds.BucketOrdsEnum collectedBuckets = bucketOrds.ordsEnum(owningBucketOrds.get(owningOrdIdx));
                        BytesRef scratch = new BytesRef();
                        while (collectedBuckets.next()) {
                            collectedBuckets.readValue(scratch);
                            long docCount = bucketDocCount(collectedBuckets.ord());
                            // if the key is below threshold, reinsert into the new ords
                            if (docCount <= maxDocCount) {
                                checkRealMemoryCBForInternalBucket();
                                StringRareTerms.Bucket bucket = new StringRareTerms.Bucket(
                                    BytesRef.deepCopyOf(scratch),
                                    docCount,
                                    null,
                                    format
                                );
                                bucket.bucketOrd = offset + bucketsInThisOwningBucketToCollect.add(scratch);
                                mergeMap.set(collectedBuckets.ord(), bucket.bucketOrd);
                                builtBuckets.add(bucket);
                                keepCount++;
                            } else {
                                filters.get(owningOrdIdx).add(scratch);
                            }
                        }
                        rarestPerOrd.set(owningOrdIdx, builtBuckets.toArray(StringRareTerms.Bucket[]::new));
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

            return StringRareTermsAggregator.this.buildAggregations(Math.toIntExact(owningBucketOrds.size()), ordIdx -> {
                StringRareTerms.Bucket[] buckets = rarestPerOrd.get(ordIdx);
                Arrays.sort(buckets, ORDER.comparator());
                return new StringRareTerms(name, ORDER, metadata(), format, Arrays.asList(buckets), maxDocCount, filters.get(ordIdx));
            });
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new StringRareTerms(name, LongRareTermsAggregator.ORDER, metadata(), format, emptyList(), 0, newFilter());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }
}
