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
import org.elasticsearch.index.fielddata.ScriptDocValues;

import java.io.IOException;
import java.util.Arrays;

public class BooleanDocValues implements ScriptFieldDocValues, ScriptFieldValues.BooleanValues {

    private final SortedNumericDocValues input;

    private boolean[] values = new boolean[0];
    private int count;

    public BooleanDocValues(SortedNumericDocValues input) {
        this.input = input;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        if (input.advanceExact(docId)) {
            resize(input.docValueCount());
            for (int i = 0; i < count; i++) {
                values[i] = input.nextValue() == 1;
            }
        } else {
            resize(0);
        }
    }

    private void resize(int newSize) {
        count = newSize;

        assert count >= 0 : "size must be positive (got " + count + "): likely integer overflow?";
        if (values.length < count) {
            values = Arrays.copyOf(values, ArrayUtil.oversize(count, 1));
        }
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
