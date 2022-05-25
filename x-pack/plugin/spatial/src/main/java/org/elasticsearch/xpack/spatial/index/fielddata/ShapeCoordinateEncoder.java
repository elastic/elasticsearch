/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.geo.XYEncodingUtils;

// TODO make cartesian
final class ShapeCoordinateEncoder implements CoordinateEncoder {

    @Override
    public int encodeX(double x) {
        if (x == Double.NEGATIVE_INFINITY) {
            return Integer.MIN_VALUE;
        }
        if (x == Double.POSITIVE_INFINITY) {
            return Integer.MAX_VALUE;
        }
        return XYEncodingUtils.encode((float) x);
    }

    @Override
    public int encodeY(double y) {
        if (y == Double.NEGATIVE_INFINITY) {
            return Integer.MIN_VALUE;
        }
        if (y == Double.POSITIVE_INFINITY) {
            return Integer.MAX_VALUE;
        }
        return XYEncodingUtils.encode((float) y);
    }

    @Override
    public double decodeX(int x) {
        return XYEncodingUtils.decode(x);
    }

    @Override
    public double decodeY(int y) {
        return XYEncodingUtils.decode(y);
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
