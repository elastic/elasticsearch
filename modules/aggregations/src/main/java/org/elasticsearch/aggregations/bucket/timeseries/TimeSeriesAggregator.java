/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.bucket.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.BytesKeyedBucketOrds;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TimeSeriesAggregator extends BucketsAggregator {

    protected final BytesKeyedBucketOrds bucketOrds;
    private final boolean keyed;
    private final int size;

    public TimeSeriesAggregator(
        String name,
        AggregatorFactories factories,
        boolean keyed,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound bucketCardinality,
        Map<String, Object> metadata,
        int size
    ) throws IOException {
        super(name, factories, context, parent, CardinalityUpperBound.MANY, metadata);
        this.keyed = keyed;
        bucketOrds = BytesKeyedBucketOrds.build(bigArrays(), bucketCardinality);
        this.size = size;
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        BytesRef spare = new BytesRef();
        InternalTimeSeries.InternalBucket[][] allBucketsPerOrd = new InternalTimeSeries.InternalBucket[owningBucketOrds.length][];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds[ordIdx]);
            List<InternalTimeSeries.InternalBucket> buckets = new ArrayList<>();
            BytesRef prev = null;
            while (ordsEnum.next()) {
                long docCount = bucketDocCount(ordsEnum.ord());
                ordsEnum.readValue(spare);
                assert prev == null || spare.compareTo(prev) > 0
                    : "key [" + spare.utf8ToString() + "] is smaller than previous key [" + prev.utf8ToString() + "]";
                InternalTimeSeries.InternalBucket bucket = new InternalTimeSeries.InternalBucket(
                    prev = BytesRef.deepCopyOf(spare), // Closing bucketOrds will corrupt the bytes ref, so need to make a deep copy here.
                    docCount,
                    null,
                    keyed
                );
                bucket.bucketOrd = ordsEnum.ord();
                buckets.add(bucket);
                if (buckets.size() >= size) {
                    break;
                }
            }
            allBucketsPerOrd[ordIdx] = buckets.toArray(new InternalTimeSeries.InternalBucket[0]);
        }
        buildSubAggsForAllBuckets(allBucketsPerOrd, b -> b.bucketOrd, (b, a) -> b.aggregations = a);

        InternalAggregation[] result = new InternalAggregation[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            result[ordIdx] = buildResult(allBucketsPerOrd[ordIdx]);
        }
        return result;
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalTimeSeries(name, new ArrayList<>(), false, metadata());
    }

    @Override
    protected void doClose() {
        Releasables.close(bucketOrds);
    }

    @Override
    protected LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        return new LeafBucketCollectorBase(sub, null) {

            // Keeping track of these fields helps to reduce time spent attempting to add bucket + tsid combos that already were added.
            long currentTsidOrd = -1;
            long currentBucket = -1;
            long currentBucketOrdinal;

            @Override
            public void collect(int doc, long bucket) throws IOException {
                // Naively comparing bucket against currentBucket and tsid ord to currentBucket can work really well.
                // TimeSeriesIndexSearcher ensures that docs are emitted in tsid and timestamp order, so if tsid ordinal
                // changes to what is stored in currentTsidOrd then that ordinal well never occur again. Same applies
                // currentBucket if there is no parent aggregation or the immediate parent aggregation creates buckets
                // based on @timestamp field or dimension fields (fields that make up the tsid).
                if (currentBucket == bucket && currentTsidOrd == aggCtx.getTsidOrd()) {
                    collectExistingBucket(sub, doc, currentBucketOrdinal);
                    return;
                }

                long bucketOrdinal = bucketOrds.add(bucket, aggCtx.getTsid());
                if (bucketOrdinal < 0) { // already seen
                    bucketOrdinal = -1 - bucketOrdinal;
                    collectExistingBucket(sub, doc, bucketOrdinal);
                } else {
                    collectBucket(sub, doc, bucketOrdinal);
                }

                currentBucketOrdinal = bucketOrdinal;
                currentTsidOrd = aggCtx.getTsidOrd();
                currentBucket = bucket;
            }
        };
    }

    InternalTimeSeries buildResult(InternalTimeSeries.InternalBucket[] topBuckets) {
        return new InternalTimeSeries(name, List.of(topBuckets), keyed, metadata());
    }
}
