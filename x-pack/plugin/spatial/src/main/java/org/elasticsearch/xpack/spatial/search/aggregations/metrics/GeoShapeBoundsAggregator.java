/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.InternalGeoBounds;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSource;

import java.io.IOException;
import java.util.Map;

public final class GeoShapeBoundsAggregator extends MetricsAggregator {
    private final GeoShapeValuesSource valuesSource;
    private final boolean wrapLongitude;
    private DoubleArray tops;
    private DoubleArray bottoms;
    private DoubleArray posLefts;
    private DoubleArray posRights;
    private DoubleArray negLefts;
    private DoubleArray negRights;

    public GeoShapeBoundsAggregator(
        String name,
        AggregationContext context,
        Aggregator parent,
        ValuesSourceConfig valuesSourceConfig,
        boolean wrapLongitude,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        assert valuesSourceConfig.hasValues();
        this.valuesSource = (GeoShapeValuesSource) valuesSourceConfig.getValuesSource();
        this.wrapLongitude = wrapLongitude;
        tops = bigArrays().newDoubleArray(1, false);
        tops.fill(0, tops.size(), Double.NEGATIVE_INFINITY);
        bottoms = bigArrays().newDoubleArray(1, false);
        bottoms.fill(0, bottoms.size(), Double.POSITIVE_INFINITY);
        posLefts = bigArrays().newDoubleArray(1, false);
        posLefts.fill(0, posLefts.size(), Double.POSITIVE_INFINITY);
        posRights = bigArrays().newDoubleArray(1, false);
        posRights.fill(0, posRights.size(), Double.NEGATIVE_INFINITY);
        negLefts = bigArrays().newDoubleArray(1, false);
        negLefts.fill(0, negLefts.size(), Double.POSITIVE_INFINITY);
        negRights = bigArrays().newDoubleArray(1, false);
        negRights.fill(0, negRights.size(), Double.NEGATIVE_INFINITY);
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) {
        final GeoShapeValues values = valuesSource.shapeValues(aggCtx.getLeafReaderContext());
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    maybeResize(bucket);
                    final GeoShapeValues.GeoShapeValue value = values.value();
                    final GeoShapeValues.BoundingBox bounds = value.boundingBox();
                    tops.set(bucket, Math.max(tops.get(bucket), bounds.top));
                    bottoms.set(bucket, Math.min(bottoms.get(bucket), bounds.bottom));
                    posLefts.set(bucket, Math.min(posLefts.get(bucket), bounds.posLeft));
                    posRights.set(bucket, Math.max(posRights.get(bucket), bounds.posRight));
                    negLefts.set(bucket, Math.min(negLefts.get(bucket), bounds.negLeft));
                    negRights.set(bucket, Math.max(negRights.get(bucket), bounds.negRight));
                }
            }

            private void maybeResize(long bucket) {
                if (bucket >= tops.size()) {
                    final long from = tops.size();
                    tops = bigArrays().grow(tops, bucket + 1);
                    tops.fill(from, tops.size(), Double.NEGATIVE_INFINITY);
                    bottoms = bigArrays().resize(bottoms, tops.size());
                    bottoms.fill(from, bottoms.size(), Double.POSITIVE_INFINITY);
                    posLefts = bigArrays().resize(posLefts, tops.size());
                    posLefts.fill(from, posLefts.size(), Double.POSITIVE_INFINITY);
                    posRights = bigArrays().resize(posRights, tops.size());
                    posRights.fill(from, posRights.size(), Double.NEGATIVE_INFINITY);
                    negLefts = bigArrays().resize(negLefts, tops.size());
                    negLefts.fill(from, negLefts.size(), Double.POSITIVE_INFINITY);
                    negRights = bigArrays().resize(negRights, tops.size());
                    negRights.fill(from, negRights.size(), Double.NEGATIVE_INFINITY);
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        double top = tops.get(owningBucketOrdinal);
        double bottom = bottoms.get(owningBucketOrdinal);
        double posLeft = posLefts.get(owningBucketOrdinal);
        double posRight = posRights.get(owningBucketOrdinal);
        double negLeft = negLefts.get(owningBucketOrdinal);
        double negRight = negRights.get(owningBucketOrdinal);
        return new InternalGeoBounds(name, top, bottom, posLeft, posRight, negLeft, negRight, wrapLongitude, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return InternalGeoBounds.empty(name, wrapLongitude, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(tops, bottoms, posLefts, posRights, negLefts, negRights);
    }
}
