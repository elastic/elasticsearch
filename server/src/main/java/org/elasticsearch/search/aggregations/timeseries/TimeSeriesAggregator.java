/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.plain.SortedSetBytesLeafFieldData;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
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
import java.util.Objects;

public class TimeSeriesAggregator extends BucketsAggregator {

    private final IndexFieldData<SortedSetBytesLeafFieldData> tsidFieldData;
    protected final BytesKeyedBucketOrds bucketOrds;
    private final boolean keyed;

    @SuppressWarnings("unchecked")
    public TimeSeriesAggregator(
        String name,
        AggregatorFactories factories,
        boolean keyed,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound bucketCardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, bucketCardinality, metadata);
        this.keyed = keyed;
        tsidFieldData = (IndexFieldData<SortedSetBytesLeafFieldData>) Objects.requireNonNull(
            context.buildFieldContext("_tsid"),
            "Cannot obtain tsid field"
        ).indexFieldData();
        bucketOrds = BytesKeyedBucketOrds.build(bigArrays(), bucketCardinality);
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        InternalTimeSeries.InternalBucket[][] allBucketsPerOrd = new InternalTimeSeries.InternalBucket[owningBucketOrds.length][];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            BytesRef spareKey = new BytesRef();
            BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds[ordIdx]);
            List<InternalTimeSeries.InternalBucket> buckets = new ArrayList<>();
            while (ordsEnum.next()) {
                long docCount = bucketDocCount(ordsEnum.ord());
                ordsEnum.readValue(spareKey);
                InternalTimeSeries.InternalBucket bucket = new InternalTimeSeries.InternalBucket(
                    TimeSeriesIdFieldMapper.decodeTsid(spareKey),
                    docCount,
                    null,
                    keyed
                );
                bucket.bucketOrd = ordsEnum.ord();
                buckets.add(bucket);
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
    protected LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector sub) throws IOException {
        final SortedBinaryDocValues tsids = tsidFieldData.load(context).getBytesValues();
        return new LeafBucketCollectorBase(sub, null) {

            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (tsids.advanceExact(doc)) {
                    BytesRef newTsid = tsids.nextValue();
                    long bucketOrdinal = bucketOrds.add(bucket, newTsid);
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

    InternalTimeSeries buildResult(InternalTimeSeries.InternalBucket[] topBuckets) {
        return new InternalTimeSeries(name, List.of(topBuckets), keyed, metadata());
    }
}
