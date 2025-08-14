/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.index.FloatVectorValues;

import java.io.IOException;
import java.util.List;

public abstract class PrefetchingFloatVectorValues extends FloatVectorValues {
    public abstract void prefetch(int... ord) throws IOException;

    @Override
    public abstract PrefetchingFloatVectorValues copy() throws IOException;

    public static PrefetchingFloatVectorValues floats(List<float[]> vectors, int dimension, int[] ordToDoc) {
        return new PrefetchingFloatVectorValues() {
            @Override
            public void prefetch(int... ord) {
                // no-op
            }

            @Override
            public float[] vectorValue(int ord) {
                return vectors.get(ord);
            }

            @Override
            public PrefetchingFloatVectorValues copy() {
                return this;
            }

            @Override
            public int dimension() {
                return dimension;
            }

            @Override
            public int size() {
                return vectors.size();
            }

            @Override
            public int ordToDoc(int ord) {
                return ordToDoc[ord];
            }
        };
    }

    public static PrefetchingFloatVectorValues floats(List<float[]> vectors, int dimension) {
        return new PrefetchingFloatVectorValues() {
            @Override
            public void prefetch(int... ord) {
                // no-op
            }

            @Override
            public float[] vectorValue(int ord) {
                return vectors.get(ord);
            }

            @Override
            public PrefetchingFloatVectorValues copy() {
                return this;
            }

            @Override
            public int dimension() {
                return dimension;
            }

            @Override
            public int size() {
                return vectors.size();
            }

            @Override
            public int ordToDoc(int ord) {
                return ord;
            }
        };
    }
}
