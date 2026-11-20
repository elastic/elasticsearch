/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.calibrate;

import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;

import java.io.IOException;

/**
 * Presents {@link ByteVectorValues} as {@link FloatVectorValues}, widening each vector exactly into a reusable
 * scratch buffer on read. Calibration math is float-only, and this lets byte vectors be kept at one byte per
 * dimension until the moment they are read, instead of materialising a four times larger float copy.
 *
 * <p>Like the other calibration sources, the array returned by {@link #vectorValue(int)} is a live buffer that the
 * next call on this instance overwrites; use {@link #copy()} for an independent reader.
 */
final class WidenedByteVectorValues extends FloatVectorValues {
    private final ByteVectorValues delegate;
    private final float[] scratch;

    WidenedByteVectorValues(ByteVectorValues delegate) {
        this.delegate = delegate;
        this.scratch = new float[delegate.dimension()];
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
        byte[] bytes = delegate.vectorValue(ord);
        for (int i = 0; i < scratch.length; i++) {
            scratch[i] = bytes[i];
        }
        return scratch;
    }

    @Override
    public FloatVectorValues copy() throws IOException {
        return new WidenedByteVectorValues(delegate.copy());
    }

    @Override
    public int dimension() {
        return delegate.dimension();
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public DocIndexIterator iterator() {
        return delegate.iterator();
    }
}
