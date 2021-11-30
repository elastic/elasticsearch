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
import java.util.Iterator;
import java.util.NoSuchElementException;

public class LongDocValuesField implements DocValuesField<Long>, ScriptDocValues.Supplier<Long> {

    protected final SortedNumericDocValues input;
    protected final String name;

    protected long[] values = new long[0];
    protected int count;

    private ScriptDocValues.Longs longs = null;

    public LongDocValuesField(SortedNumericDocValues input, String name) {
        this.input = input;
        this.name = name;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        if (input.advanceExact(docId)) {
            resize(input.docValueCount());
            for (int i = 0; i < count; i++) {
                values[i] = input.nextValue();
            }
        } else {
            resize(0);
        }
    }

    protected void resize(int newSize) {
        count = newSize;

        assert count >= 0 : "size must be positive (got " + count + "): likely integer overflow?";
        values = ArrayUtil.grow(values, count);
    }

    /**
     * Returns a {@code ScriptDocValues} of the appropriate type for this field.
     * This is used to support backwards compatibility for accessing field values
     * through the {@code doc} variable.
     */
    @Override
    public ScriptDocValues<Long> getScriptDocValues() {
        if (longs == null) {
            longs = new ScriptDocValues.Longs(this);
        }

        return longs;
    }

    @Override
    public Long getInternal(int index) {
        return values[index];
    }

    /**
     * Returns the name of this field.
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Returns {@code true} if this field has no values, otherwise {@code false}.
     */
    @Override
    public boolean isEmpty() {
        return count == 0;
    }

    /**
     * Returns the number of values this field has.
     */
    @Override
    public int size() {
        return count;
    }

    @Override
    public Iterator<Long> iterator() {
        return new Iterator<Long>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < count;
            }

            @Override
            public Long next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                return values[index++];
            }
        };
    }

    public long get(long defaultValue) {
        return get(0, defaultValue);
    }

    public long get(int index, long defaultValue) {
        if (isEmpty() || index < 0 || index >= count) {
            return defaultValue;
        }

        return values[index];
    }
}
