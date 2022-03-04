/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

import java.io.IOException;

public class FloatDocValuesSupplier implements DocValuesSupplier<Double>, FieldSupplier.FloatSupplier {

    protected final SortedNumericDoubleValues input;

    protected float[] values = new float[0];
    protected int count;

    public FloatDocValuesSupplier(SortedNumericDoubleValues input) {
        this.input = input;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        if (input.advanceExact(docId)) {
            resize(input.docValueCount());
            for (int i = 0; i < count; i++) {
                values[i] = (float)input.nextValue();
            }
        } else {
            resize(0);
        }
    }

    protected void resize(int newSize) {
        count = newSize;
        values = ArrayUtil.grow(values, count);
    }

    @Override
    public Double getCompatible(int index) {
        return (double)get(index);
    }

    @Override
    public int size() {
        return count;
    }

    public float get(int index) {
        return values[index];
    }
}
