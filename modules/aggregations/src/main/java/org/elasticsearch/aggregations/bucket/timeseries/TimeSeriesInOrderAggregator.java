/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.bucket.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TimeSeriesInOrderAggregator extends BucketsAggregator {

    // reuse tsids between owning bucket ordinals:
    private final BytesRefArray collectedTsids;
    private final LongObjectPagedHashMap<List<InternalBucket>> results;
    private final boolean keyed;

    public TimeSeriesInOrderAggregator(
        String name,
        AggregatorFactories factories,
        boolean keyed,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, cardinality, metadata);
        this.keyed = keyed;
        this.results = new LongObjectPagedHashMap<>(1, context.bigArrays());
        this.collectedTsids = new BytesRefArray(1, context.bigArrays());
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        InternalAggregation[] result = new InternalAggregation[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            long owningOrdinal = owningBucketOrds[ordIdx];
            List<InternalBucket> internalBuckets = results.get(owningOrdinal);
            if (internalBuckets != null) {
                BytesRef spare = new BytesRef();
                List<InternalTimeSeries.InternalBucket> buckets = new ArrayList<>(internalBuckets.size());
                for (InternalBucket internalBucket : internalBuckets) {
                    BytesRef key = collectedTsids.get(internalBucket.tsidOffset, spare);
                    InternalAggregations internalAggregations = buildSubAggsForBuckets(new long[] { internalBucket.bucketOrd })[0];
                    buckets.add(
                        new InternalTimeSeries.InternalBucket(
                            BytesRef.deepCopyOf(key),
                            internalBucket.docCount,
                            internalAggregations,
                            keyed
                        )
                    );
                }
                result[ordIdx] = new InternalTimeSeries(name, buckets, keyed, metadata());
            } else {
                result[ordIdx] = buildEmptyAggregation();
            }
        }
        return result;
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalTimeSeries(name, new ArrayList<>(), false, metadata());
    }

    private BytesRef currentTsid;
    private int currentTsidOrd = -1;
    private long currentParentBucket = -1;
    private long docCount;
    // TODO use 0L as bucket ordinal and clear sub aggregations after bucket/parent bucket ordinal combination changes
    // Ideally use a constant ordinal (0) here and tsid or parent bucket change reset sub and
    // reuse the same ordinal. This is possible because a tsid / parent bucket ordinal are unique and
    // don't reappear when either one changes.
    private long bucketOrdinalGenerator;

    @Override
    protected LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, null) {

            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (currentTsidOrd == aggCtx.getTsidOrd() && currentParentBucket == bucket) {
                    docCount++;
                    sub.collect(doc, bucketOrdinalGenerator);
                    return;
                }
                if (currentTsid != null) {
                    completeBucket();
                    bucketOrdinalGenerator++;
                }
                if (currentTsidOrd != aggCtx.getTsidOrd()) {
                    currentTsidOrd = aggCtx.getTsidOrd();
                    currentTsid = aggCtx.getTsid();

                    collectedTsids.append(currentTsid);
                }
                if (currentParentBucket != bucket) {
                    currentParentBucket = bucket;
                }

                docCount = 1;
                sub.collect(doc, bucketOrdinalGenerator);
            }
        };
    }

    @Override
    protected void doPostCollection() {
        if (currentTsid != null) {
            completeBucket();
        }
    }

    private void completeBucket() {
        InternalBucket bucket = new InternalBucket(collectedTsids.size() - 1, bucketOrdinalGenerator, docCount);
        // TODO: instead of collecting all buckets, perform pipeline aggregations here:
        // (Then we don't need to keep all these buckets in memory)
        List<InternalBucket> result = results.get(currentParentBucket);
        if (result == null) {
            result = new ArrayList<>();
            results.put(currentParentBucket, result);
        }
        result.add(bucket);
    }

    @Override
    protected void doClose() {
        Releasables.close(results, collectedTsids);
    }

    record InternalBucket(long tsidOffset, long bucketOrd, long docCount) {}
}
