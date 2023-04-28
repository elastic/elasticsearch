/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;

import java.util.function.LongConsumer;

/**
 * {@link CellIdSource} implementation for GeoTile aggregation
 */
public class GeoTileCellIdSource extends CellIdSource {

    public GeoTileCellIdSource(GeoPoint valuesSource, int precision, GeoBoundingBox geoBoundingBox, LongConsumer circuitBreakerConsumer) {
        super(valuesSource, precision, geoBoundingBox, circuitBreakerConsumer);
    }

    @Override
    protected NumericDocValues unboundedCellSingleValue(GeoPointValues values) {
        return new CellSingleValue(values, precision()) {
            @Override
            protected boolean advance(org.elasticsearch.common.geo.GeoPoint target) {
                value = GeoTileUtils.longEncode(target.getLon(), target.getLat(), precision);
                return true;
            }
        };
    }

    @Override
    protected NumericDocValues boundedCellSingleValue(GeoPointValues values, GeoBoundingBox boundingBox) {
        final GeoTileBoundedPredicate predicate = new GeoTileBoundedPredicate(precision(), boundingBox);
        final int tiles = 1 << precision();
        return new CellSingleValue(values, precision()) {
            @Override
            protected boolean advance(org.elasticsearch.common.geo.GeoPoint target) {
                final int x = GeoTileUtils.getXTile(target.getLon(), tiles);
                final int y = GeoTileUtils.getYTile(target.getLat(), tiles);
                if (predicate.validTile(x, y, precision)) {
                    value = GeoTileUtils.longEncodeTiles(precision, x, y);
                    return true;
                }
                return false;
            }
        };
    }

    @Override
    protected SortedNumericDocValues unboundedCellMultiValues(MultiGeoPointValues values) {
        return new CellMultiValues(values, precision(), circuitBreakerConsumer) {
            @Override
            protected int advanceValue(org.elasticsearch.common.geo.GeoPoint target, int valuesIdx) {
                values[valuesIdx] = GeoTileUtils.longEncode(target.getLon(), target.getLat(), precision);
                return valuesIdx + 1;
            }
        };
    }

    @Override
    protected SortedNumericDocValues boundedCellMultiValues(MultiGeoPointValues values, GeoBoundingBox boundingBox) {
        final GeoTileBoundedPredicate predicate = new GeoTileBoundedPredicate(precision(), boundingBox);
        final int tiles = 1 << precision();
        return new CellMultiValues(values, precision(), circuitBreakerConsumer) {
            @Override
            protected int advanceValue(org.elasticsearch.common.geo.GeoPoint target, int valuesIdx) {
                final int x = GeoTileUtils.getXTile(target.getLon(), tiles);
                final int y = GeoTileUtils.getYTile(target.getLat(), tiles);
                if (predicate.validTile(x, y, precision)) {
                    values[valuesIdx] = GeoTileUtils.longEncodeTiles(precision, x, y);
                    return valuesIdx + 1;
                }
                return valuesIdx;
            }
        };
    }
}
