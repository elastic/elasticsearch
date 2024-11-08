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
import java.util.function.Supplier;

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
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        StringTerms.Bucket[][] topBucketsPerOrd = new StringTerms.Bucket[owningBucketOrds.length][];
        long[] otherDocCounts = new long[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            int size = (int) Math.min(bucketOrds.size(), bucketCountThresholds.getShardSize());

            // as users can't control sort order, in practice we'll always sort by doc count descending
            try (
                BucketPriorityQueue<StringTerms.Bucket> ordered = new BucketPriorityQueue<>(
                    size,
                    bigArrays(),
                    partiallyBuiltBucketComparator
                )
            ) {
                StringTerms.Bucket spare = null;
                BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds[ordIdx]);
                Supplier<StringTerms.Bucket> emptyBucketBuilder = () -> new StringTerms.Bucket(new BytesRef(), 0, null, false, 0, format);
                while (ordsEnum.next()) {
                    long docCount = bucketDocCount(ordsEnum.ord());
                    otherDocCounts[ordIdx] += docCount;
                    if (spare == null) {
                        spare = emptyBucketBuilder.get();
                    }
                    ordsEnum.readValue(spare.getTermBytes());
                    spare.setDocCount(docCount);
                    spare.setBucketOrd(ordsEnum.ord());
                    spare = ordered.insertWithOverflow(spare);
                }

                topBucketsPerOrd[ordIdx] = new StringTerms.Bucket[(int) ordered.size()];
                for (int i = (int) ordered.size() - 1; i >= 0; --i) {
                    topBucketsPerOrd[ordIdx][i] = ordered.pop();
                    otherDocCounts[ordIdx] -= topBucketsPerOrd[ordIdx][i].getDocCount();
                    topBucketsPerOrd[ordIdx][i].setTermBytes(BytesRef.deepCopyOf(topBucketsPerOrd[ordIdx][i].getTermBytes()));
                }
            }
        }

        buildSubAggsForAllBuckets(topBucketsPerOrd, InternalTerms.Bucket::getBucketOrd, InternalTerms.Bucket::setAggregations);
        InternalAggregation[] result = new InternalAggregation[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            final BucketOrder reduceOrder;
            if (isKeyOrder(order) == false) {
                reduceOrder = InternalOrder.key(true);
                Arrays.sort(topBucketsPerOrd[ordIdx], reduceOrder.comparator());
            } else {
                reduceOrder = order;
            }
            result[ordIdx] = new StringTerms(
                name,
                reduceOrder,
                order,
                bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(),
                metadata(),
                format,
                bucketCountThresholds.getShardSize(),
                false,
                otherDocCounts[ordIdx],
                Arrays.asList(topBucketsPerOrd[ordIdx]),
                null
            );
        }
        return result;
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
