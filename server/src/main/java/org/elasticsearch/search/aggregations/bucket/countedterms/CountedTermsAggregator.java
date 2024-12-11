/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.countedterms;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.terms.BucketAndOrd;
import org.elasticsearch.search.aggregations.bucket.terms.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.bucket.terms.BytesKeyedBucketOrds;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.BiConsumer;

import static java.util.Collections.emptyList;
import static org.elasticsearch.search.aggregations.InternalOrder.isKeyOrder;

class CountedTermsAggregator extends TermsAggregator {
    private final BytesKeyedBucketOrds bucketOrds;
    protected final ValuesSource.Bytes.WithOrdinals valuesSource;

    @SuppressWarnings("this-escape")
    CountedTermsAggregator(
        String name,
        AggregatorFactories factories,
        ValuesSource.Bytes.WithOrdinals valuesSource,
        BucketOrder order,
        DocValueFormat format,
        BucketCountThresholds bucketCountThresholds,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, bucketCountThresholds, order, format, SubAggCollectionMode.DEPTH_FIRST, metadata);
        this.valuesSource = valuesSource;
        this.bucketOrds = BytesKeyedBucketOrds.build(context.bigArrays(), cardinality);
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        final SortedSetDocValues ords = valuesSource.ordinalsValues(aggCtx.getLeafReaderContext());
        final SortedDocValues singleton = DocValues.unwrapSingleton(ords);
        return singleton != null ? getLeafCollector(singleton, sub) : getLeafCollector(ords, sub);
    }

    private LeafBucketCollector getLeafCollector(SortedSetDocValues ords, LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, ords) {

            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (ords.advanceExact(doc)) {
                    for (int i = 0; i < ords.docValueCount(); i++) {
                        long ord = ords.nextOrd();
                        collectOrdinal(bucketOrds.add(owningBucketOrd, ords.lookupOrd(ord)), doc, sub);
                    }
                }
            }
        };
    }

    private LeafBucketCollector getLeafCollector(SortedDocValues ords, LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, ords) {

            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (ords.advanceExact(doc)) {
                    collectOrdinal(bucketOrds.add(owningBucketOrd, ords.lookupOrd(ords.ordValue())), doc, sub);
                }

            }
        };
    }

    private void collectOrdinal(long bucketOrdinal, int doc, LeafBucketCollector sub) throws IOException {
        if (bucketOrdinal < 0) { // already seen
            bucketOrdinal = -1 - bucketOrdinal;
            collectExistingBucket(sub, doc, bucketOrdinal);
        } else {
            collectBucket(sub, doc, bucketOrdinal);
        }
    }

    @Override
    public InternalAggregation[] buildAggregations(LongArray owningBucketOrds) throws IOException {
        try (
            LongArray otherDocCounts = bigArrays().newLongArray(owningBucketOrds.size());
            ObjectArray<StringTerms.Bucket[]> topBucketsPerOrd = bigArrays().newObjectArray(owningBucketOrds.size())
        ) {
            try (IntArray bucketsToCollect = bigArrays().newIntArray(owningBucketOrds.size())) {
                // find how many buckets we are going to collect
                long ordsToCollect = 0;
                for (long ordIdx = 0; ordIdx < owningBucketOrds.size(); ordIdx++) {
                    int size = (int) Math.min(bucketOrds.bucketsInOrd(owningBucketOrds.get(ordIdx)), bucketCountThresholds.getShardSize());
                    bucketsToCollect.set(ordIdx, size);
                    ordsToCollect += size;
                }
                try (LongArray ordsArray = bigArrays().newLongArray(ordsToCollect)) {
                    long ordsCollected = 0;
                    for (long ordIdx = 0; ordIdx < owningBucketOrds.size(); ordIdx++) {
                        // as users can't control sort order, in practice we'll always sort by doc count descending
                        try (
                            BucketPriorityQueue<StringTerms.Bucket> ordered = new BucketPriorityQueue<>(
                                bucketsToCollect.get(ordIdx),
                                bigArrays(),
                                order.partiallyBuiltBucketComparator(this)
                            )
                        ) {
                            BucketAndOrd<StringTerms.Bucket> spare = null;
                            BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds.get(ordIdx));
                            while (ordsEnum.next()) {
                                long docCount = bucketDocCount(ordsEnum.ord());
                                otherDocCounts.increment(ordIdx, docCount);
                                if (spare == null) {
                                    checkRealMemoryCBForInternalBucket();
                                    spare = new BucketAndOrd<>(new StringTerms.Bucket(new BytesRef(), 0, null, false, 0, format));
                                }
                                ordsEnum.readValue(spare.bucket.getTermBytes());
                                spare.bucket.setDocCount(docCount);
                                spare.ord = ordsEnum.ord();
                                spare = ordered.insertWithOverflow(spare);
                            }
                            final int orderedSize = (int) ordered.size();
                            final StringTerms.Bucket[] buckets = new StringTerms.Bucket[orderedSize];
                            for (int i = orderedSize - 1; i >= 0; --i) {
                                BucketAndOrd<StringTerms.Bucket> bucketAndOrd = ordered.pop();
                                buckets[i] = bucketAndOrd.bucket;
                                ordsArray.set(ordsCollected + i, bucketAndOrd.ord);
                                otherDocCounts.increment(ordIdx, -bucketAndOrd.bucket.getDocCount());
                                bucketAndOrd.bucket.setTermBytes(BytesRef.deepCopyOf(bucketAndOrd.bucket.getTermBytes()));
                            }
                            topBucketsPerOrd.set(ordIdx, buckets);
                            ordsCollected += orderedSize;
                        }
                    }
                    assert ordsCollected == ordsArray.size();
                    buildSubAggsForAllBuckets(topBucketsPerOrd, ordsArray, InternalTerms.Bucket::setAggregations);
                }
            }

            return buildAggregations(Math.toIntExact(owningBucketOrds.size()), ordIdx -> {
                final BucketOrder reduceOrder;
                if (isKeyOrder(order) == false) {
                    reduceOrder = InternalOrder.key(true);
                    Arrays.sort(topBucketsPerOrd.get(ordIdx), reduceOrder.comparator());
                } else {
                    reduceOrder = order;
                }
                return new StringTerms(
                    name,
                    reduceOrder,
                    order,
                    bucketCountThresholds.getRequiredSize(),
                    bucketCountThresholds.getMinDocCount(),
                    metadata(),
                    format,
                    bucketCountThresholds.getShardSize(),
                    false,
                    otherDocCounts.get(ordIdx),
                    Arrays.asList(topBucketsPerOrd.get(ordIdx)),
                    null
                );
            });
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new StringTerms(
            name,
            order,
            order,
            bucketCountThresholds.getRequiredSize(),
            bucketCountThresholds.getMinDocCount(),
            metadata(),
            format,
            bucketCountThresholds.getShardSize(),
            false,
            0,
            emptyList(),
            0L
        );
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        add.accept("total_buckets", bucketOrds.size());
    }

    @Override
    protected void doClose() {
        Releasables.close(bucketOrds);
    }

}
