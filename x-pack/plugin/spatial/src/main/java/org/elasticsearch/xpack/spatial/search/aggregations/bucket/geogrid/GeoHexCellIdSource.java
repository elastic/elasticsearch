/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.bucket.geogrid.CellValues;
import org.elasticsearch.search.aggregations.support.ValuesSource;

/**
 * Class to help convert {@link MultiGeoPointValues}
 * to GeoHex bucketing.
 */
public class GeoHexCellIdSource extends ValuesSource.Numeric {
    private final GeoPoint valuesSource;
    private final int precision;
    private final GeoBoundingBox geoBoundingBox;

    public GeoHexCellIdSource(GeoPoint valuesSource, int precision, GeoBoundingBox geoBoundingBox) {
        this.valuesSource = valuesSource;
        this.precision = precision;
        this.geoBoundingBox = geoBoundingBox;
    }

    public int precision() {
        return precision;
    }

    @Override
    public boolean isFloatingPoint() {
        return false;
    }

    @Override
    public SortedNumericDocValues longValues(LeafReaderContext ctx) {
        return geoBoundingBox.isUnbounded()
            ? new UnboundedCellValues(valuesSource.geoPointValues(ctx), precision)
            : new BoundedCellValues(valuesSource.geoPointValues(ctx), precision, geoBoundingBox);
    }

    @Override
    public SortedNumericDoubleValues doubleValues(LeafReaderContext ctx) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedBinaryDocValues bytesValues(LeafReaderContext ctx) {
        throw new UnsupportedOperationException();
    }

    private static class UnboundedCellValues extends CellValues {

        UnboundedCellValues(MultiGeoPointValues geoValues, int precision) {
            super(geoValues, precision);
        }

        @Override
        protected int advanceValue(org.elasticsearch.common.geo.GeoPoint target, int valuesIdx) {
            values[valuesIdx] = H3.geoToH3(target.getLat(), target.getLon(), precision);
            return valuesIdx + 1;
        }
    }

    private static class BoundedCellValues extends CellValues {

        private final boolean crossesDateline;
        private final GeoBoundingBox bbox;

        private final long northPoleHex, southPoleHex;

        protected BoundedCellValues(MultiGeoPointValues geoValues, int precision, GeoBoundingBox bbox) {
            super(geoValues, precision);
            this.crossesDateline = bbox.right() < bbox.left();
            this.bbox = bbox;
            northPoleHex = H3.geoToH3(90, 0, precision);
            southPoleHex = H3.geoToH3(-90, 0, precision);
        }

        @Override
        public int advanceValue(org.elasticsearch.common.geo.GeoPoint target, int valuesIdx) {
            final double lat = target.getLat();
            final double lon = target.getLon();
            final long hex = H3.geoToH3(lat, lon, precision);
            // validPoint is a fast check, validHex is slow
            if (validPoint(lat, lon) || validHex(hex)) {
                values[valuesIdx] = hex;
                return valuesIdx + 1;
            }
            return valuesIdx;
        }

        private boolean validPoint(double lat, double lon) {
            if (bbox.top() > lat && bbox.bottom() < lat) {
                if (crossesDateline) {
                    return bbox.left() < lon || bbox.right() > lon;
                } else {
                    return bbox.left() < lon && bbox.right() > lon;
                }
            }
            return false;
        }

        private boolean validHex(long hex) {
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
