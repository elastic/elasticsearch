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
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class BinaryDocValuesSupplier implements DocValuesSupplier<BytesRef>, FieldSupplier.Supplier<ByteBuffer> {

    protected final SortedBinaryDocValues input;

    protected BytesRefBuilder[] values = new BytesRefBuilder[0];
    protected int count;

    public BinaryDocValuesSupplier(SortedBinaryDocValues input) {
        this.input = input;
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

    protected void resize(int newSize) {
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
    public BytesRef getCompatible(int index) {
        return values[index].toBytesRef();
    }

    @Override
    public int size() {
        return count;
    }

    @Override
    public ByteBuffer get(int index) {
        return ByteBuffer.wrap(values[index].toBytesRef().bytes);
    }
}
