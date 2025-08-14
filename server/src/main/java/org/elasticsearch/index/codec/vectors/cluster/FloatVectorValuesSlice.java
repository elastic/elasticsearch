/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import java.io.IOException;

class FloatVectorValuesSlice extends PrefetchingFloatVectorValues {

    private final PrefetchingFloatVectorValues allValues;
    private final int[] slice;

    FloatVectorValuesSlice(PrefetchingFloatVectorValues allValues, int[] slice) {
        assert slice != null;
        assert slice.length <= allValues.size();
        this.allValues = allValues;
        this.slice = slice;
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
        return this.allValues.vectorValue(this.slice[ord]);
    }

    @Override
    public int dimension() {
        return this.allValues.dimension();
    }

    @Override
    public int size() {
        return slice.length;
    }

    @Override
    public int ordToDoc(int ord) {
        return this.slice[ord];
    }

    @Override
    public PrefetchingFloatVectorValues copy() throws IOException {
        return new FloatVectorValuesSlice(this.allValues.copy(), this.slice);
    }

    @Override
    public void prefetch(int... ord) throws IOException {
        for (int o : ord) {
            this.allValues.prefetch(slice[o]);
        }
    }
}
