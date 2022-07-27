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
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.search.aggregations.bucket.geogrid.CellIdSource;

/**
* {@link CellIdSource} implementation for GeoHex aggregation
*/
public class GeoHexCellIdSource extends CellIdSource {

    public GeoHexCellIdSource(GeoPoint valuesSource, int precision, GeoBoundingBox geoBoundingBox) {
        super(valuesSource, precision, geoBoundingBox);
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
                // validPoint is a fast check, validHex is slow
                if (validPoint(lon, lat) || predicate.validHex(hex)) {
                    value = hex;
                    return true;
                }
                return false;
            }
        };
    }

    @Override
    protected SortedNumericDocValues unboundedCellMultiValues(MultiGeoPointValues values) {
        return new CellMultiValues(values, precision()) {
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
        return new CellMultiValues(values, precision()) {
            @Override
            protected int advanceValue(org.elasticsearch.common.geo.GeoPoint target, int valuesIdx) {
                final double lat = target.getLat();
                final double lon = target.getLon();
                final long hex = H3.geoToH3(lat, lon, precision);
                // validPoint is a fast check, validHex is slow
                if (validPoint(lon, lat) || predicate.validHex(hex)) {
                    values[valuesIdx] = hex;
                    return valuesIdx + 1;
                }
                return valuesIdx;
            }
        };
    }

    private static class GeoHexPredicate {

        private final boolean crossesDateline;
        private final GeoBoundingBox bbox;
        private final long northPoleHex, southPoleHex;

        GeoHexPredicate(GeoBoundingBox bbox, int precision) {
            this.crossesDateline = bbox.right() < bbox.left();
            this.bbox = bbox;
            northPoleHex = H3.geoToH3(90, 0, precision);
            southPoleHex = H3.geoToH3(-90, 0, precision);
        }

        public boolean validHex(long hex) {
            CellBoundary boundary = H3.h3ToGeoBoundary(hex);
            double minLat = Double.POSITIVE_INFINITY;
            double minLon = Double.POSITIVE_INFINITY;
            double maxLat = Double.NEGATIVE_INFINITY;
            double maxLon = Double.NEGATIVE_INFINITY;
            for (int i = 0; i < boundary.numPoints(); i++) {
                double boundaryLat = boundary.getLatLon(i).getLatDeg();
                double boundaryLon = boundary.getLatLon(i).getLonDeg();
                minLon = Math.min(minLon, boundaryLon);
                maxLon = Math.max(maxLon, boundaryLon);
                minLat = Math.min(minLat, boundaryLat);
                maxLat = Math.max(maxLat, boundaryLat);
            }
            if (northPoleHex == hex) {
                return minLat < bbox.top();
            } else if (southPoleHex == hex) {
                return maxLat > bbox.bottom();
            } else if (maxLon - minLon > 180) {
                return intersects(-180, minLon, minLat, maxLat) || intersects(maxLon, 180, minLat, maxLat);
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
