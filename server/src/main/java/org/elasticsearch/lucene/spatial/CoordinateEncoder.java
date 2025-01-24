/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.spatial;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.XYEncodingUtils;
import org.elasticsearch.common.geo.GeoUtils;

/**
 * Abstract API for classes that help encode double-valued spatial coordinates x/y to
 * their integer-encoded serialized form and decode them back
 */
public abstract class CoordinateEncoder {

    /** Encodes lat/lon values into / from lucene encoded format */
    public static final CoordinateEncoder GEO = new GeoShapeCoordinateEncoder();
    /** Encodes arbitrary x/y values in the float space into / from sortable integers */
    public static final CoordinateEncoder CARTESIAN = new CartesianShapeCoordinateEncoder();

    /** encode X value */
    public abstract int encodeX(double x);

    /** encode Y value */
    public abstract int encodeY(double y);

    /** decode X value */
    public abstract double decodeX(int x);

    /** decode Y value */
    public abstract double decodeY(int y);

    /** normalize X value */
    public abstract double normalizeX(double x);

    /** normalize Y value */
    public abstract double normalizeY(double y);

    private static class GeoShapeCoordinateEncoder extends CoordinateEncoder {

        @Override
        public int encodeX(double x) {
            if (x == Double.NEGATIVE_INFINITY) {
                return Integer.MIN_VALUE;
            }
            if (x == Double.POSITIVE_INFINITY) {
                return Integer.MAX_VALUE;
            }
            return GeoEncodingUtils.encodeLongitude(x);
        }

        @Override
        public int encodeY(double y) {
            if (y == Double.NEGATIVE_INFINITY) {
                return Integer.MIN_VALUE;
            }
            if (y == Double.POSITIVE_INFINITY) {
                return Integer.MAX_VALUE;
            }
            return GeoEncodingUtils.encodeLatitude(y);
        }

        @Override
        public double decodeX(int x) {
            return GeoEncodingUtils.decodeLongitude(x);
        }

        @Override
        public double decodeY(int y) {
            return GeoEncodingUtils.decodeLatitude(y);
        }

        @Override
        public double normalizeX(double x) {
            return GeoUtils.normalizeLon(x);
        }

        @Override
        public double normalizeY(double y) {
            return GeoUtils.normalizeLat(y);
        }
    }

    private static class CartesianShapeCoordinateEncoder extends CoordinateEncoder {

        private int encode(double value) {
            if (value == Double.NEGATIVE_INFINITY) {
                return Integer.MIN_VALUE;
            }
            if (value == Double.POSITIVE_INFINITY) {
                return Integer.MAX_VALUE;
            }
            return XYEncodingUtils.encode((float) value);
        }

        private double decode(int value) {
            if (value == Integer.MIN_VALUE) {
                return Double.NEGATIVE_INFINITY;
            }
            if (value == Integer.MAX_VALUE) {
                return Double.POSITIVE_INFINITY;
            }
            return XYEncodingUtils.decode(value);
        }

        @Override
        public int encodeX(double x) {
            return encode(x);
        }

        @Override
        public int encodeY(double y) {
            return encode(y);
        }

        @Override
        public double decodeX(int x) {
            return decode(x);
        }

        @Override
        public double decodeY(int y) {
            return decode(y);
        }

        @Override
        public double normalizeX(double x) {
            return x;
        }

        @Override
        public double normalizeY(double y) {
            return y;
        }
    }
}
