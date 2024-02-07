/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.lucene.spatial;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.geo.GeoUtils;

final class GeoShapeCoordinateEncoder implements CoordinateEncoder {

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
