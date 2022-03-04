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

import java.io.IOException;
import java.util.Arrays;

public class BooleanFieldDocValuesSupplier
    implements ScriptFieldDocValuesSupplier, ScriptDocValuesSupplier<Boolean>, ScriptFieldSupplier.BooleanSupplier {

    protected final SortedNumericDocValues input;

    protected boolean[] values = new boolean[0];
    protected int count;

    public BooleanFieldDocValuesSupplier(SortedNumericDocValues input) {
        this.input = input;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        if (input.advanceExact(docId)) {
            resize(input.docValueCount());
            for (int i = 0; i < count; i++) {
                values[i] = parseValue(input.nextValue());
            }
        } else {
            resize(0);
        }
    }

    protected void resize(int newSize) {
        count = newSize;

        assert count >= 0 : "size must be positive (got " + count + "): likely integer overflow?";
        if (values.length < count) {
            values = Arrays.copyOf(values, ArrayUtil.oversize(count, 1));
        }
    }

    public boolean parseValue(long value) {
        return value == 1;
    }

    @Override
    public Boolean getCompatible(int index) {
        return values[index];
    }

    @Override
    public int size() {
        return count;
    }

    @Override
    public boolean get(int index) {
        return values[index];
    }
}
