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
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

public abstract class AbstractLongDocValuesField extends AbstractScriptFieldFactory<Long>
    implements
        Field<Long>,
        DocValuesScriptFieldFactory,
        ScriptDocValues.Supplier<Long> {

    protected final String name;
    // used for backwards compatibility for old-style "doc" access
    // as a delegate to this field class
    protected ScriptDocValues<?> scriptDocValues = null;

    protected final SortedNumericDocValues input;
    protected long[] values = new long[0];
    protected int count;

    public AbstractLongDocValuesField(SortedNumericDocValues input, String name) {
        this.input = input;
        this.name = name;
    }

    /**
     * Override if not using {@link ScriptDocValues.Longs}
     */
    protected ScriptDocValues<?> newScriptDocValues() {
        return new ScriptDocValues.Longs(this);
    }

    /**
     * Override if long has special formatting.
     */
    protected long formatLong(long raw) {
        return raw;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        if (input.advanceExact(docId)) {
            resize(input.docValueCount());
            for (int i = 0; i < count; i++) {
                values[i] = formatLong(input.nextValue());
            }
        } else {
            resize(0);
        }
    }

    @Override
    public ScriptDocValues<?> toScriptDocValues() {
        if (scriptDocValues == null) {
            scriptDocValues = newScriptDocValues();
        }

        return scriptDocValues;
    }

    /**
     * Set the {@link #size()} and ensure that the {@link #values} array can
     * store at least that many entries.
     */
    private void resize(int newSize) {
        count = newSize;
        values = ArrayUtil.grow(values, count);
    }

    // this method is required to support the Long return values
    // for the old-style "doc" access in ScriptDocValues
    @Override
    public Long getInternal(int index) {
        return getLong(index);
    }

    protected long getLong(int index) {
        return values[index];
    }

    @Override
    public int size() {
        return count;
    }

    @Override
    public boolean isEmpty() {
        return count == 0;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public PrimitiveIterator.OfLong iterator() {
        return new PrimitiveIterator.OfLong() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < count;
            }

            @Override
            public Long next() {
                return nextLong();
            }

            @Override
            public long nextLong() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }

                return getLong(index++);
            }
        };
    }

    /** Returns the 0th index value as an {@code long} if it exists, otherwise {@code defaultValue}. */
    public long get(long defaultValue) {
        return get(0, defaultValue);
    }

    /** Returns the value at {@code index} as an {@code long} if it exists, otherwise {@code defaultValue}. */
    public long get(int index, long defaultValue) {
        if (isEmpty() || index < 0 || index >= count) {
            return defaultValue;
        }

        return getLong(index);
    }
}
