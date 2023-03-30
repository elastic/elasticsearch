/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.geo.XYEncodingUtils;

final class CartesianShapeCoordinateEncoder implements CoordinateEncoder {

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
