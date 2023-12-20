/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.countedkeyword;

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
import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_ORDS;
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
        SortedSetDocValues ords = valuesSource.ordinalsValues(aggCtx.getLeafReaderContext());
        return new LeafBucketCollectorBase(sub, ords) {

            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (ords.advanceExact(doc) == false) {
                    return;
                }
                for (long ord = ords.nextOrd(); ord != NO_MORE_ORDS; ord = ords.nextOrd()) {
                    long bucketOrdinal = bucketOrds.add(owningBucketOrd, ords.lookupOrd(ord));
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

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        StringTerms.Bucket[][] topBucketsPerOrd = new StringTerms.Bucket[owningBucketOrds.length][];
        long[] otherDocCounts = new long[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            int size = (int) Math.min(bucketOrds.size(), bucketCountThresholds.getShardSize());

            // as users can't control sort order, in practice we'll always sort by doc count descending
            BucketPriorityQueue<StringTerms.Bucket> ordered = new BucketPriorityQueue<>(size, partiallyBuiltBucketComparator);
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

            topBucketsPerOrd[ordIdx] = new StringTerms.Bucket[ordered.size()];
            for (int i = ordered.size() - 1; i >= 0; --i) {
                topBucketsPerOrd[ordIdx][i] = ordered.pop();
                otherDocCounts[ordIdx] -= topBucketsPerOrd[ordIdx][i].getDocCount();
                topBucketsPerOrd[ordIdx][i].setTermBytes(BytesRef.deepCopyOf(topBucketsPerOrd[ordIdx][i].getTermBytes()));
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
