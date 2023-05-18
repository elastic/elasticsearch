/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

/**
 * A field data based aggregator that counts the number of values a specific field has within the aggregation context.
 *
 * This aggregator works in a multi-bucket mode, that is, when serves as a sub-aggregator, a single aggregator instance aggregates the
 * counts for all buckets owned by the parent aggregator)
 */
public class ValueCountAggregator extends NumericMetricsAggregator.SingleValue {

    final ValuesSource valuesSource;

    // a count per bucket
    LongArray counts;

    public ValueCountAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext aggregationContext,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, aggregationContext, parent, metadata);
        assert valuesSourceConfig.hasValues();
        this.valuesSource = valuesSourceConfig.getValuesSource();
        counts = bigArrays().newLongArray(1, true);
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
        if (valuesSource instanceof ValuesSource.Numeric) {
            final SortedNumericDocValues values = ((ValuesSource.Numeric) valuesSource).longValues(aggCtx.getLeafReaderContext());
            return new LeafBucketCollectorBase(sub, values) {

                @Override
                public void collect(int doc, long bucket) throws IOException {
                    counts = bigArrays().grow(counts, bucket + 1);
                    if (values.advanceExact(doc)) {
                        counts.increment(bucket, values.docValueCount());
                    }
                }
            };
        }
        if (valuesSource instanceof ValuesSource.Bytes.GeoPoint) {
            MultiGeoPointValues values = ((ValuesSource.GeoPoint) valuesSource).geoPointValues(aggCtx.getLeafReaderContext());
            return new LeafBucketCollectorBase(sub, null) {

                @Override
                public void collect(int doc, long bucket) throws IOException {
                    counts = bigArrays().grow(counts, bucket + 1);
                    if (values.advanceExact(doc)) {
                        counts.increment(bucket, values.docValueCount());
                    }
                }
            };
        }
        // The following is default collector. Including the keyword FieldType
        final SortedBinaryDocValues values = valuesSource.bytesValues(aggCtx.getLeafReaderContext());
        return new LeafBucketCollectorBase(sub, values) {

            @Override
            public void collect(int doc, long bucket) throws IOException {
                counts = bigArrays().grow(counts, bucket + 1);
                if (values.advanceExact(doc)) {
                    counts.increment(bucket, values.docValueCount());
                }
            }

        };
    }

    @Override
    public double metric(long owningBucketOrd) {
        return owningBucketOrd >= counts.size() ? 0 : counts.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (bucket >= counts.size()) {
            return buildEmptyAggregation();
        }
        return new InternalValueCount(name, counts.get(bucket), metadata());
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return InternalValueCount.empty(name, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(counts);
    }

}
