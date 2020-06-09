/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.geo.GeoEncodingUtils;

public final class GeoShapeCoordinateEncoder implements CoordinateEncoder {
    public static final GeoShapeCoordinateEncoder INSTANCE = new GeoShapeCoordinateEncoder();

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
}
