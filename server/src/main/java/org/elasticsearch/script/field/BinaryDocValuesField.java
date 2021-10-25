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
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.ScriptDocValues.BytesRefs;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BinaryDocValuesField implements DocValuesField {

    private final SortedBinaryDocValues input;
    private final String name;

    private BytesRefBuilder[] values = new BytesRefBuilder[0];
    private int count;

    // used for backwards compatibility for old-style "doc" access
    // as a delegate to this field class
    private BytesRefs bytesRefs = null;

    public BinaryDocValuesField(SortedBinaryDocValues input, String name) {
        this.input = input;
        this.name = name;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        if (input.advanceExact(docId)) {
            resize(input.docValueCount());
            for (int i = 0; i < count; i++) {
                // We need to make a copy here, because BytesBinaryDVLeafFieldData's SortedBinaryDocValues
                // implementation reuses the returned BytesRef. Otherwise, we would end up with the same BytesRef
                // instance for all slots in the values array.
                values[i].copyBytes(input.nextValue());
            }
        } else {
            resize(0);
        }
    }

    private void resize(int newSize) {
        count = newSize;

        if (newSize > values.length) {
            int oldLength = values.length;
            values = ArrayUtil.grow(values, count);

            for (int i = oldLength; i < values.length; ++i) {
                values[i] = new BytesRefBuilder();
            }
        }
    }

    @Override
    public ScriptDocValues<?> getScriptDocValues() {
        if (bytesRefs == null) {
            bytesRefs = new BytesRefs(this);
        }

        return bytesRefs;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isEmpty() {
        return count == 0;
    }

    @Override
    public int size() {
        return count;
    }

    public List<BytesRef> getValues() {
        if (isEmpty()) {
            return Collections.emptyList();
        }

        List<BytesRef> values = new ArrayList<>(count);

        for (int index = 0; index < count; ++index) {
            values.add(this.values[index].toBytesRef());
        }

        return values;
    }

    public BytesRef getValue(BytesRef defaultValue) {
        return getValue(0, defaultValue);
    }

    public BytesRef getValue(int index, BytesRef defaultValue) {
        if (isEmpty() || index < 0 || index >= count) {
            return defaultValue;
        }

        return values[index].toBytesRef();
    }
}
