/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.h3.H3;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.search.aggregations.bucket.geogrid.CellIdSource;
import org.elasticsearch.xpack.spatial.common.H3SphericalUtil;

import java.util.function.LongConsumer;

/**
 * {@link CellIdSource} implementation for GeoHex aggregation
 */
public class GeoHexCellIdSource extends CellIdSource {

    public GeoHexCellIdSource(GeoPoint valuesSource, int precision, GeoBoundingBox geoBoundingBox, LongConsumer circuitBreakerConsumer) {
        super(valuesSource, precision, geoBoundingBox, circuitBreakerConsumer);
    }

    @Override
    protected NumericDocValues unboundedCellSingleValue(GeoPointValues values) {
        return new CellSingleValue(values, precision()) {
            @Override
            protected boolean advance(org.elasticsearch.common.geo.GeoPoint target) {
                value = H3.geoToH3(target.getLat(), target.getLon(), precision);
                return true;
            }
        };
    }

    @Override
    protected NumericDocValues boundedCellSingleValue(GeoPointValues values, GeoBoundingBox boundingBox) {
        final GeoHexPredicate predicate = new GeoHexPredicate(boundingBox);
        return new CellSingleValue(values, precision()) {
            @Override
            protected boolean advance(org.elasticsearch.common.geo.GeoPoint target) {
                final double lat = target.getLat();
                final double lon = target.getLon();
                final long hex = H3.geoToH3(lat, lon, precision);
                // pointInBounds is a fast check, validHex is slow
                if (pointInBounds(lon, lat) || predicate.validHex(hex)) {
                    assert predicate.validHex(hex) : H3.h3ToString(hex) + " should be valid but it is not";
                    value = hex;
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
                values[valuesIdx] = H3.geoToH3(target.getLat(), target.getLon(), precision);
                return valuesIdx + 1;
            }
        };
    }

    @Override
    protected SortedNumericDocValues boundedCellMultiValues(MultiGeoPointValues values, GeoBoundingBox boundingBox) {
        final GeoHexPredicate predicate = new GeoHexPredicate(boundingBox);
        return new CellMultiValues(values, precision(), circuitBreakerConsumer) {
            @Override
            protected int advanceValue(org.elasticsearch.common.geo.GeoPoint target, int valuesIdx) {
                final double lat = target.getLat();
                final double lon = target.getLon();
                final long hex = H3.geoToH3(lat, lon, precision);
                // validPoint is a fast check, validHex is slow
                if (pointInBounds(lon, lat) || predicate.validHex(hex)) {
                    values[valuesIdx] = hex;
                    return valuesIdx + 1;
                }
                return valuesIdx;
            }
        };
    }

    private static class GeoHexPredicate {

        private final boolean crossesDateline;
        private final GeoBoundingBox bbox, scratch;

        GeoHexPredicate(GeoBoundingBox bbox) {
            this.crossesDateline = bbox.right() < bbox.left();
            this.bbox = bbox;
            scratch = new GeoBoundingBox(new org.elasticsearch.common.geo.GeoPoint(), new org.elasticsearch.common.geo.GeoPoint());
        }

        public boolean validHex(long hex) {
            H3SphericalUtil.computeGeoBounds(hex, scratch);
            if (bbox.top() > scratch.bottom() && bbox.bottom() < scratch.top()) {
                if (scratch.left() > scratch.right()) {
                    return intersects(-180, scratch.right()) || intersects(scratch.left(), 180);
                } else {
                    return intersects(scratch.left(), scratch.right());
                }
            }
            return false;
        }

        private boolean intersects(double minLon, double maxLon) {
            if (crossesDateline) {
                return bbox.left() < maxLon || bbox.right() > minLon;
            } else {
                return bbox.left() < maxLon && bbox.right() > minLon;
            }
        }
    }
}
