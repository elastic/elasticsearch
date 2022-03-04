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

public class StringDocValuesSupplier implements DocValuesSupplier<String>, FieldSupplier.Supplier<String> {

    protected final SortedBinaryDocValues input;

    protected String[] values = new String[0];
    protected int count;

    public StringDocValuesSupplier(SortedBinaryDocValues input) {
        this.input = input;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        if (input.advanceExact(docId)) {
            resize(input.docValueCount());
            for (int i = 0; i < count; i++) {
                values[i] = formatValue(input.nextValue());
            }
        } else {
            resize(0);
        }
    }

    protected void resize(int newSize) {
        count = newSize;
        values = ArrayUtil.grow(values, count);
    }

    public String formatValue(BytesRef raw) {
        return raw.utf8ToString();
    }

    @Override
    public String getCompatible(int index) {
        return get(index);
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
