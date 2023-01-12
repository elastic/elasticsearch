/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.spatial3d.geom.LatLonBounds;
import org.apache.lucene.spatial3d.geom.Plane;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.apache.lucene.spatial3d.geom.SidedPlane;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.search.aggregations.bucket.geogrid.CellIdSource;

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
        final GeoHexPredicate predicate = new GeoHexPredicate(boundingBox, precision());
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
        final GeoHexPredicate predicate = new GeoHexPredicate(boundingBox, precision());
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

    // package private for testing
    static LatLonBounds getGeoBounds(CellBoundary cellBoundary) {
        final LatLonBounds bounds = new LatLonBounds();
        org.apache.lucene.spatial3d.geom.GeoPoint start = getGeoPoint(cellBoundary.getLatLon(cellBoundary.numPoints() - 1));
        for (int i = 0; i < cellBoundary.numPoints(); i++) {
            final org.apache.lucene.spatial3d.geom.GeoPoint end = getGeoPoint(cellBoundary.getLatLon(i));
            bounds.addPoint(end);
            final Plane plane = new Plane(start, end);
            bounds.addPlane(PlanetModel.SPHERE, plane, new SidedPlane(start, plane, end), new SidedPlane(end, start, plane));
            start = end;
        }
        return bounds;
    }

    private static org.apache.lucene.spatial3d.geom.GeoPoint getGeoPoint(LatLng latLng) {
        return new org.apache.lucene.spatial3d.geom.GeoPoint(PlanetModel.SPHERE, latLng.getLatRad(), latLng.getLonRad());
    }

    private static class GeoHexPredicate {

        private final boolean crossesDateline;
        private final GeoBoundingBox bbox;
        private final long northPoleHex, southPoleHex;

        GeoHexPredicate(GeoBoundingBox bbox, int precision) {
            this.crossesDateline = bbox.right() < bbox.left();
            this.bbox = bbox;
            northPoleHex = H3.northPolarH3(precision);
            southPoleHex = H3.southPolarH3(precision);
        }

        public boolean validHex(long hex) {
            final LatLonBounds bounds = getGeoBounds(H3.h3ToGeoBoundary(hex));
            final double minLat = bounds.checkNoBottomLatitudeBound() ? GeoUtils.MIN_LAT_INCL : Math.toDegrees(bounds.getMinLatitude());
            final double maxLat = bounds.checkNoTopLatitudeBound() ? GeoUtils.MAX_LAT_INCL : Math.toDegrees(bounds.getMaxLatitude());
            final double minLon;
            final double maxLon;
            if (bounds.checkNoLongitudeBound()) {
                assert northPoleHex == hex || southPoleHex == hex;
                minLon = GeoUtils.MIN_LON_INCL;
                maxLon = GeoUtils.MAX_LON_INCL;
            } else {
                minLon = Math.toDegrees(bounds.getLeftLongitude());
                maxLon = Math.toDegrees(bounds.getRightLongitude());
            }
            if (northPoleHex == hex) {
                return minLat < bbox.top();
            } else if (southPoleHex == hex) {
                return maxLat > bbox.bottom();
            } else if (maxLon < minLon) {
                return intersects(-180, maxLon, minLat, maxLat) || intersects(minLon, 180, minLat, maxLat);
            } else {
                return intersects(minLon, maxLon, minLat, maxLat);
            }
        }

        private boolean intersects(double minLon, double maxLon, double minLat, double maxLat) {
            if (bbox.top() > minLat && bbox.bottom() < maxLat) {
                if (crossesDateline) {
                    return bbox.left() < maxLon || bbox.right() > minLon;
                } else {
                    return bbox.left() < maxLon && bbox.right() > minLon;
                }
            }
            return false;
        }
    }
}
