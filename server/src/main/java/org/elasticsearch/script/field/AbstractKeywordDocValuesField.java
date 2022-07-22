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
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class AbstractKeywordDocValuesField extends AbstractScriptFieldFactory<String>
    implements
        Field<String>,
        DocValuesScriptFieldFactory,
        ScriptDocValues.Supplier<String> {

    protected final SortedBinaryDocValues input;
    protected final String name;

    protected BytesRefBuilder[] values = new BytesRefBuilder[0];
    protected int count;

    // used for backwards compatibility for old-style "doc" access
    // as a delegate to this field class
    protected ScriptDocValues.Strings strings = null;

    public AbstractKeywordDocValuesField(SortedBinaryDocValues input, String name) {
        this.input = input;
        this.name = name;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        if (input.advanceExact(docId)) {
            resize(input.docValueCount());
            for (int i = 0; i < count; i++) {
                // We need to make a copy here, because BytesBinaryDVLeafFieldData's SortedBinaryDocValues
                // implementation reuses the returned BytesRef. Otherwise we would end up with the same BytesRef
                // instance for all slots in the values array.
                values[i].copyBytes(input.nextValue());
            }
        } else {
            resize(0);
        }
    }

    private void resize(int newSize) {
        count = newSize;
        assert count >= 0 : "size must be positive (got " + count + "): likely integer overflow?";
        if (newSize > values.length) {
            final int oldLength = values.length;
            values = ArrayUtil.grow(values, count);
            for (int i = oldLength; i < values.length; ++i) {
                values[i] = new BytesRefBuilder();
            }
        }
    }

    @Override
    public ScriptDocValues<String> toScriptDocValues() {
        if (strings == null) {
            strings = new ScriptDocValues.Strings(this);
        }

        return strings;
    }

    // this method is required to support the Boolean return values
    // for the old-style "doc" access in ScriptDocValues
    @Override
    public String getInternal(int index) {
        return bytesToString(values[index].toBytesRef());
    }

    protected static String bytesToString(BytesRef bytesRef) {
        return bytesRef.utf8ToString();
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

    public String get(String defaultValue) {
        return get(0, defaultValue);
    }

    public String get(int index, String defaultValue) {
        if (isEmpty() || index < 0 || index >= count) {
            return defaultValue;
        }

        return bytesToString(values[index].toBytesRef());
    }

    @Override
    public Iterator<String> iterator() {
        return new Iterator<String>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < count;
            }

            @Override
            public String next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                return bytesToString(values[index++].toBytesRef());
            }
        };
    }
}
