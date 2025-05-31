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
import java.util.stream.IntStream;

class FloatVectorValuesSlice extends FloatVectorValues {

    final FloatVectorValues allValues;
    final int[] slice;

    FloatVectorValuesSlice(FloatVectorValues allValues, int[] slice) {
        this.allValues = allValues;
        this.slice = slice;
    }

    FloatVectorValuesSlice(FloatVectorValues allValues) {
        this.allValues = allValues;
        this.slice = IntStream.range(0, allValues.size()).toArray();
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
    public FloatVectorValues copy() throws IOException {
        return new FloatVectorValuesSlice(this.allValues.copy(), this.slice);
    }
}
