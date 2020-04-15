/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.GeoBoundsAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.xpack.spatial.index.mapper.GeoShapeValuesSource;
import org.elasticsearch.xpack.spatial.index.mapper.MultiGeoShapeValues;

import java.io.IOException;
import java.util.Map;

public final class GeoShapeBoundsAggregator extends GeoBoundsAggregator {
    private final GeoShapeValuesSource valuesSource;

    public GeoShapeBoundsAggregator(String name, SearchContext aggregationContext, Aggregator parent,
                             GeoShapeValuesSource valuesSource, boolean wrapLongitude, Map<String, Object> metadata) throws IOException {
        // the valuesSource as GeoPoint passed to GeoBoundsAggregator is not used
        super(name, aggregationContext, parent, ValuesSource.GeoPoint.EMPTY, wrapLongitude, metadata);
        this.valuesSource = valuesSource;
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
}
