/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.aggregations.bucket.timeseries;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.RoutingPathFields;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
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
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class TimeSeriesAggregator extends BucketsAggregator {

    protected final BytesKeyedBucketOrds bucketOrds;
    private final boolean keyed;
    private final int size;

    private final SortedMap<String, ValuesSource> dimensionValueSources;

    @SuppressWarnings("this-escape")
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
        dimensionValueSources = new TreeMap<>();
        for (var dim : context.subSearchContext().getSearchExecutionContext().dimensionFields()) {
            var valueSource = ValuesSourceConfig.resolve(context, null, dim.name(), null, null, null, null, null).getValuesSource();
            dimensionValueSources.put(dim.name(), valueSource);
        }
    }

    @Override
    public InternalAggregation[] buildAggregations(LongArray owningBucketOrds) throws IOException {
        BytesRef spare = new BytesRef();
        try (ObjectArray<InternalTimeSeries.InternalBucket[]> allBucketsPerOrd = bigArrays().newObjectArray(owningBucketOrds.size())) {
            for (long ordIdx = 0; ordIdx < allBucketsPerOrd.size(); ordIdx++) {
                BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds.get(ordIdx));
                List<InternalTimeSeries.InternalBucket> buckets = new ArrayList<>();
                while (ordsEnum.next()) {
                    long docCount = bucketDocCount(ordsEnum.ord());
                    ordsEnum.readValue(spare);
                    checkRealMemoryCBForInternalBucket();
                    InternalTimeSeries.InternalBucket bucket = new InternalTimeSeries.InternalBucket(
                        BytesRef.deepCopyOf(spare), // Closing bucketOrds will corrupt the bytes ref, so need to make a deep copy here.
                        docCount,
                        null
                    );
                    bucket.bucketOrd = ordsEnum.ord();
                    buckets.add(bucket);
                    if (buckets.size() >= size) {
                        break;
                    }
                }
                // NOTE: after introducing _tsid hashing time series are sorted by (_tsid hash, @timestamp) instead of (_tsid, timestamp).
                // _tsid hash and _tsid might sort differently, and out of order data might result in incorrect buckets due to _tsid value
                // changes not matching _tsid hash changes. Changes in _tsid hash are handled creating a new bucket as a result of making
                // the assumption that sorting data results in new buckets whenever there is a change in _tsid hash. This is no true anymore
                // because we collect data sorted on (_tsid hash, timestamp) but build aggregation results sorted by (_tsid, timestamp).
                buckets.sort(Comparator.comparing(bucket -> bucket.key));
                allBucketsPerOrd.set(ordIdx, buckets.toArray(new InternalTimeSeries.InternalBucket[0]));
            }
            buildSubAggsForAllBuckets(allBucketsPerOrd, b -> b.bucketOrd, (b, a) -> b.aggregations = a);

            return buildAggregations(Math.toIntExact(allBucketsPerOrd.size()), ordIdx -> buildResult(allBucketsPerOrd.get(ordIdx)));
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalTimeSeries(name, List.of(), false, metadata());
    }

    @Override
    protected void doClose() {
        Releasables.close(bucketOrds);
    }

    @Override
    protected LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        SortedMap<String, TsidConsumer> dimensionConsumers = new TreeMap<>();
        for (var entry : dimensionValueSources.entrySet()) {
            String fieldName = entry.getKey();
            if (entry.getValue() instanceof ValuesSource.Numeric numericVS) {
                SortedNumericDocValues docValues = numericVS.longValues(aggCtx.getLeafReaderContext());
                dimensionConsumers.put(entry.getKey(), (docId, tsidBuilder) -> {
                    if (docValues.advanceExact(docId)) {
                        assert docValues.docValueCount() == 1 : "Dimension field cannot be a multi-valued field";
                        tsidBuilder.addLong(fieldName, docValues.nextValue());
                    }
                });
            } else {
                SortedBinaryDocValues docValues = entry.getValue().bytesValues(aggCtx.getLeafReaderContext());
                dimensionConsumers.put(entry.getKey(), (docId, tsidBuilder) -> {
                    if (docValues.advanceExact(docId)) {
                        assert docValues.docValueCount() == 1 : "Dimension field cannot be a multi-valued field";
                        tsidBuilder.addString(fieldName, docValues.nextValue());
                    }
                });
            }
        }
        return new LeafBucketCollectorBase(sub, null) {

            // Keeping track of these fields helps to reduce time spent attempting to add bucket + tsid combos that already were added.
            long currentTsidOrd = -1;
            long currentBucket = -1;
            long currentBucketOrdinal;
            BytesRef currentTsid;

            @Override
            public void collect(int doc, long bucket) throws IOException {
                // Naively comparing bucket against currentBucket and tsid ord to currentBucket can work really well.
                // TimeSeriesIndexSearcher ensures that docs are emitted in tsid and timestamp order, so if tsid ordinal
                // changes to what is stored in currentTsidOrd then that ordinal will never occur again. Same applies to
                // currentBucket, if there is no parent aggregation or the immediate parent aggregation creates buckets
                // based on @timestamp field or dimension fields (fields that make up the tsid).
                if (currentBucket == bucket && currentTsidOrd == aggCtx.getTsidHashOrd()) {
                    collectExistingBucket(sub, doc, currentBucketOrdinal);
                    return;
                }

                BytesRef tsid;
                if (currentTsidOrd == aggCtx.getTsidHashOrd()) {
                    tsid = currentTsid;
                } else {
                    RoutingPathFields routingPathFields = new RoutingPathFields(null);
                    for (TsidConsumer consumer : dimensionConsumers.values()) {
                        consumer.accept(doc, routingPathFields);
                    }
                    currentTsid = tsid = TimeSeriesIdFieldMapper.buildLegacyTsid(routingPathFields).toBytesRef();
                }
                long bucketOrdinal = bucketOrds.add(bucket, tsid);
                if (bucketOrdinal < 0) { // already seen
                    bucketOrdinal = -1 - bucketOrdinal;
                    collectExistingBucket(sub, doc, bucketOrdinal);
                } else {
                    collectBucket(sub, doc, bucketOrdinal);
                }

                currentBucketOrdinal = bucketOrdinal;
                currentTsidOrd = aggCtx.getTsidHashOrd();
                currentBucket = bucket;
            }

        };
    }

    InternalTimeSeries buildResult(InternalTimeSeries.InternalBucket[] topBuckets) {
        return new InternalTimeSeries(name, Arrays.asList(topBuckets), keyed, metadata());
    }

    @FunctionalInterface
    interface TsidConsumer {
        void accept(int docId, RoutingPathFields routingFields) throws IOException;
    }
}
