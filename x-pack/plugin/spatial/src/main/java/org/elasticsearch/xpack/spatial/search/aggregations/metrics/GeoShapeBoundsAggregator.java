/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.InternalGeoBounds;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.xpack.spatial.index.fielddata.MultiGeoShapeValues;
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

    public GeoShapeBoundsAggregator(String name, SearchContext aggregationContext, Aggregator parent,
                             GeoShapeValuesSource valuesSource, boolean wrapLongitude, Map<String, Object> metadata) throws IOException {
        super(name, aggregationContext, parent, metadata);
        this.valuesSource = valuesSource;
        this.wrapLongitude = wrapLongitude;
        if (valuesSource != null) {
            final BigArrays bigArrays = context.bigArrays();
            tops = bigArrays.newDoubleArray(1, false);
            tops.fill(0, tops.size(), Double.NEGATIVE_INFINITY);
            bottoms = bigArrays.newDoubleArray(1, false);
            bottoms.fill(0, bottoms.size(), Double.POSITIVE_INFINITY);
            posLefts = bigArrays.newDoubleArray(1, false);
            posLefts.fill(0, posLefts.size(), Double.POSITIVE_INFINITY);
            posRights = bigArrays.newDoubleArray(1, false);
            posRights.fill(0, posRights.size(), Double.NEGATIVE_INFINITY);
            negLefts = bigArrays.newDoubleArray(1, false);
            negLefts.fill(0, negLefts.size(), Double.POSITIVE_INFINITY);
            negRights = bigArrays.newDoubleArray(1, false);
            negRights.fill(0, negRights.size(), Double.NEGATIVE_INFINITY);
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            LeafBucketCollector sub) {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final MultiGeoShapeValues values = valuesSource.geoShapeValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (bucket >= tops.size()) {
                    long from = tops.size();
                    tops = bigArrays.grow(tops, bucket + 1);
                    tops.fill(from, tops.size(), Double.NEGATIVE_INFINITY);
                    bottoms = bigArrays.resize(bottoms, tops.size());
                    bottoms.fill(from, bottoms.size(), Double.POSITIVE_INFINITY);
                    posLefts = bigArrays.resize(posLefts, tops.size());
                    posLefts.fill(from, posLefts.size(), Double.POSITIVE_INFINITY);
                    posRights = bigArrays.resize(posRights, tops.size());
                    posRights.fill(from, posRights.size(), Double.NEGATIVE_INFINITY);
                    negLefts = bigArrays.resize(negLefts, tops.size());
                    negLefts.fill(from, negLefts.size(), Double.POSITIVE_INFINITY);
                    negRights = bigArrays.resize(negRights, tops.size());
                    negRights.fill(from, negRights.size(), Double.NEGATIVE_INFINITY);
                }

                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();

                    for (int i = 0; i < valuesCount; ++i) {
                        MultiGeoShapeValues.GeoShapeValue value = values.nextValue();
                        MultiGeoShapeValues.BoundingBox bounds = value.boundingBox();
                        double top = Math.max(tops.get(bucket), bounds.top);
                        double bottom = Math.min(bottoms.get(bucket), bounds.bottom);
                        double posLeft = Math.min(posLefts.get(bucket), bounds.posLeft);
                        double posRight = Math.max(posRights.get(bucket), bounds.posRight);
                        double negLeft = Math.min(negLefts.get(bucket), bounds.negLeft);
                        double negRight = Math.max(negRights.get(bucket), bounds.negRight);
                        tops.set(bucket, top);
                        bottoms.set(bucket, bottom);
                        posLefts.set(bucket, posLeft);
                        posRights.set(bucket, posRight);
                        negLefts.set(bucket, negLeft);
                        negRights.set(bucket, negRight);
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
        return new InternalGeoBounds(name, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
            Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, wrapLongitude, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(tops, bottoms, posLefts, posRights, negLefts, negRights);
    }
}
