/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

/**
 * {@link CoordinateEncoder} used for tests that is an identity-encoder-decoder
 */
public class TestCoordinateEncoder implements CoordinateEncoder {

    public static final TestCoordinateEncoder INSTANCE = new TestCoordinateEncoder();

    @Override
    public int encodeX(double x) {
        return (int) x;
    }

    @Override
    public int encodeY(double y) {
        return (int) y;
    }

    @Override
    public double decodeX(int x) {
        return x;
    }

    @Override
    public double decodeY(int y) {
        return y;
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
