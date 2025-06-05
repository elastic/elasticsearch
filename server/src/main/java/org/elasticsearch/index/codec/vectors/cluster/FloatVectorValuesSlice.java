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

class FloatVectorValuesSlice extends FloatVectorValues {

    private final FloatVectorValues allValues;
    private final int[] slice;

    FloatVectorValuesSlice(FloatVectorValues allValues, int[] slice) {
        assert slice == null || slice.length <= allValues.size();
        this.allValues = allValues;
        if (slice != null && slice.length == allValues.size()) {
            this.slice = null;
        } else {
            this.slice = slice;
        }
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
        if (this.slice == null) {
            return this.allValues.vectorValue(ord);
        } else {
            return this.allValues.vectorValue(this.slice[ord]);
        }
    }

    @Override
    public int dimension() {
        return this.allValues.dimension();
    }

    @Override
    public int size() {
        if (slice == null) {
            return allValues.size();
        } else {
            return slice.length;
        }
    }

    @Override
    public int ordToDoc(int ord) {
        if (this.slice == null) {
            return ord;
        } else {
            return this.slice[ord];
        }
    }

    @Override
    public FloatVectorValues copy() throws IOException {
        return new FloatVectorValuesSlice(this.allValues.copy(), this.slice);
    }
}
