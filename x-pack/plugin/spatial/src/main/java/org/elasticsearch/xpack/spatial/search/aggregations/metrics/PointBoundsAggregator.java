/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.FloatArray;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.index.fielddata.MultiPointValues;
import org.elasticsearch.xpack.spatial.search.aggregations.support.PointValuesSource;

import java.io.IOException;
import java.util.Map;

public final class PointBoundsAggregator extends MetricsAggregator {
    private final PointValuesSource valuesSource;
    private FloatArray tops;
    private FloatArray bottoms;
    private FloatArray posLefts;
    private FloatArray posRights;

    public PointBoundsAggregator(
        String name,
        SearchContext aggregationContext,
        Aggregator parent,
        ValuesSourceConfig valuesSourceConfig,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, aggregationContext, parent, metadata);
        this.valuesSource = valuesSourceConfig.hasValues() ? (PointValuesSource) valuesSourceConfig.getValuesSource() : null;
        if (valuesSource != null) {
            final BigArrays bigArrays = context.bigArrays();
            tops = bigArrays.newFloatArray(1, false);
            tops.fill(0, tops.size(), Float.NEGATIVE_INFINITY);
            bottoms = bigArrays.newFloatArray(1, false);
            bottoms.fill(0, bottoms.size(), Float.POSITIVE_INFINITY);
            posLefts = bigArrays.newFloatArray(1, false);
            posLefts.fill(0, posLefts.size(), Float.POSITIVE_INFINITY);
            posRights = bigArrays.newFloatArray(1, false);
            posRights.fill(0, posRights.size(), Float.NEGATIVE_INFINITY);
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            LeafBucketCollector sub) {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final MultiPointValues values = valuesSource.geoPointValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (bucket >= tops.size()) {
                    long from = tops.size();
                    tops = bigArrays.grow(tops, bucket + 1);
                    tops.fill(from, tops.size(), Float.NEGATIVE_INFINITY);
                    bottoms = bigArrays.resize(bottoms, tops.size());
                    bottoms.fill(from, bottoms.size(), Float.POSITIVE_INFINITY);
                    posLefts = bigArrays.resize(posLefts, tops.size());
                    posLefts.fill(from, posLefts.size(), Float.POSITIVE_INFINITY);
                    posRights = bigArrays.resize(posRights, tops.size());
                    posRights.fill(from, posRights.size(), Float.NEGATIVE_INFINITY);
                }

                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();

                    for (int i = 0; i < valuesCount; ++i) {
                        CartesianPoint value = values.nextValue();
                        float top = Math.max(tops.get(bucket), value.getY());
                        float bottom = Math.min(bottoms.get(bucket), value.getY());
                        float posLeft = Math.min(posLefts.get(bucket), value.getX());
                        float posRight = Math.max(posRights.get(bucket), value.getX());
                        tops.set(bucket, top);
                        bottoms.set(bucket, bottom);
                        posLefts.set(bucket, posLeft);
                        posRights.set(bucket, posRight);
                    }
                }
            }
        };
    }


    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (valuesSource == null) {
            return buildEmptyAggregation();
        }
        float top = tops.get(owningBucketOrdinal);
        float bottom = bottoms.get(owningBucketOrdinal);
        float posLeft = posLefts.get(owningBucketOrdinal);
        float posRight = posRights.get(owningBucketOrdinal);
        return new InternalBounds(name, top, bottom, posLeft, posRight, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalBounds(name, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY,
            Float.NEGATIVE_INFINITY, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(tops, bottoms, posLefts, posRights);
    }
}
