/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.SetBackedScalingCuckooFilter;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
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
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            final BytesRefBuilder previous = new BytesRefBuilder();

            @Override
            public void collect(int docId, long owningBucketOrd) throws IOException {
                if (false == values.advanceExact(docId)) {
                    return;
                }
                int valuesCount = values.docValueCount();
                previous.clear();

                // SortedBinaryDocValues don't guarantee uniqueness so we
                // need to take care of dups
                for (int i = 0; i < valuesCount; ++i) {
                    BytesRef bytes = values.nextValue();
                    if (filter != null && false == filter.accept(bytes)) {
                        continue;
                    }
                    if (i > 0 && previous.get().equals(bytes)) {
                        continue;
                    }
                    previous.copyBytes(bytes);
                    long bucketOrdinal = bucketOrds.add(owningBucketOrd, bytes);
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
        StringRareTerms.Bucket[][] rarestPerOrd = new StringRareTerms.Bucket[owningBucketOrds.length][];
        SetBackedScalingCuckooFilter[] filters = new SetBackedScalingCuckooFilter[owningBucketOrds.length];
        long keepCount = 0;
        long[] mergeMap = new long[(int) bucketOrds.size()];
        Arrays.fill(mergeMap, -1);
        long offset = 0;
        for (int owningOrdIdx = 0; owningOrdIdx < owningBucketOrds.length; owningOrdIdx++) {
            try (BytesRefHash bucketsInThisOwningBucketToCollect = new BytesRefHash(1, bigArrays())) {
                filters[owningOrdIdx] = newFilter();
                List<StringRareTerms.Bucket> builtBuckets = new ArrayList<>();
                BytesKeyedBucketOrds.BucketOrdsEnum collectedBuckets = bucketOrds.ordsEnum(owningBucketOrds[owningOrdIdx]);
                BytesRef scratch = new BytesRef();
                while (collectedBuckets.next()) {
                    collectedBuckets.readValue(scratch);
                    long docCount = bucketDocCount(collectedBuckets.ord());
                    // if the key is below threshold, reinsert into the new ords
                    if (docCount <= maxDocCount) {
                        StringRareTerms.Bucket bucket = new StringRareTerms.Bucket(BytesRef.deepCopyOf(scratch), docCount, null, format);
                        bucket.bucketOrd = offset + bucketsInThisOwningBucketToCollect.add(scratch);
                        mergeMap[(int) collectedBuckets.ord()] = bucket.bucketOrd;
                        builtBuckets.add(bucket);
                        keepCount++;
                    } else {
                        filters[owningOrdIdx].add(scratch);
                    }
                }
                rarestPerOrd[owningOrdIdx] = builtBuckets.toArray(StringRareTerms.Bucket[]::new);
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
            result[ordIdx] = new StringRareTerms(
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
        return new StringRareTerms(name, LongRareTermsAggregator.ORDER, metadata(), format, emptyList(), 0, newFilter());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }
}
