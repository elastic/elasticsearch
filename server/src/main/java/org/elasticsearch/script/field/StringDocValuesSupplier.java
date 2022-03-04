/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.io.IOException;
import java.util.Arrays;

public class StringDocValuesSupplier
    implements ScriptFieldDocValuesSupplier, ScriptDocValuesSupplier<String>, ScriptFieldSupplier.Supplier<String> {

    private final SortedBinaryDocValues input;

    private String[] values = new String[0];
    private int count;

    public StringDocValuesSupplier(SortedBinaryDocValues input) {
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

    private void resize(int newSize) {
        count = newSize;

        assert count >= 0 : "size must be positive (got " + count + "): likely integer overflow?";
        if (values.length < count) {
            values = Arrays.copyOf(values, ArrayUtil.oversize(count, 1));
        }
    }

    public String parseValue(BytesRef value) {
        return value.utf8ToString();
    }

    @Override
    public String getCompatible(int index) {
        return values[index];
    }

    @Override
    public int size() {
        return count;
    }

    @Override
    public String get(int index) {
        return values[index];
    }
}
