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

public class BinaryDocValuesField extends AbstractScriptFieldFactory<ByteBuffer>
    implements
        Field<ByteBuffer>,
        DocValuesScriptFieldFactory,
        ScriptDocValues.Supplier<BytesRef> {

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
    public ScriptDocValues<?> toScriptDocValues() {
        if (bytesRefs == null) {
            bytesRefs = new BytesRefs(this);
        }

        return bytesRefs;
    }

    // this method is required to support the Boolean return values
    // for the old-style "doc" access in ScriptDocValues
    @Override
    public BytesRef getInternal(int index) {
        return values[index].toBytesRef();
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

    protected ByteBuffer toWrapped(int index) {
        return ByteBuffer.wrap(values[index].toBytesRef().bytes);
    }

    public ByteBuffer get(ByteBuffer defaultValue) {
        return get(0, defaultValue);
    }

    public ByteBuffer get(int index, ByteBuffer defaultValue) {
        if (isEmpty() || index < 0 || index >= count) {
            return defaultValue;
        }

        return toWrapped(index);
    }

    @Override
    public Iterator<ByteBuffer> iterator() {
        return new Iterator<ByteBuffer>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < count;
            }

            @Override
            public ByteBuffer next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }

                return toWrapped(index++);
            }
        };
    }
}
