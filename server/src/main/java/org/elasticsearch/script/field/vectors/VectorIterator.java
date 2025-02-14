/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.field.vectors;

import java.util.Iterator;

public interface VectorIterator<E> extends Iterator<E> {
    Iterator<E> copy();

    void reset();

    static VectorIterator<float[]> from(float[][] vectors) {
        return new VectorIterator<>() {
            private int i = 0;

            @Override
            public boolean hasNext() {
                return i < vectors.length;
            }

            @Override
            public float[] next() {
                return vectors[i++];
            }

            @Override
            public Iterator<float[]> copy() {
                return from(vectors);
            }

            @Override
            public void reset() {
                i = 0;
            }
        };
    }

    static VectorIterator<byte[]> from(byte[][] vectors) {
        return new VectorIterator<>() {
            private int i = 0;

            @Override
            public boolean hasNext() {
                return i < vectors.length;
            }

            @Override
            public byte[] next() {
                return vectors[i++];
            }

            @Override
            public Iterator<byte[]> copy() {
                return from(vectors);
            }

            @Override
            public void reset() {
                i = 0;
            }
        };
    }
}
