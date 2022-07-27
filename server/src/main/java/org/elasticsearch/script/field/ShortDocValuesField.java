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

public class ShortDocValuesField extends AbstractScriptFieldFactory<Short>
    implements
        Field<Short>,
        DocValuesScriptFieldFactory,
        ScriptDocValues.Supplier<Long> {

    protected final SortedNumericDocValues input;
    protected final String name;

    protected short[] values = new short[0];
    protected int count;

    private ScriptDocValues.Longs longs = null;

    public ShortDocValuesField(SortedNumericDocValues input, String name) {
        this.input = input;
        this.name = name;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        if (input.advanceExact(docId)) {
            resize(input.docValueCount());
            for (int i = 0; i < count; i++) {
                values[i] = (short) input.nextValue();
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
    public ScriptDocValues<Long> toScriptDocValues() {
        if (longs == null) {
            longs = new ScriptDocValues.Longs(this);
        }

        return longs;
    }

    @Override
    public Long getInternal(int index) {
        return (long) values[index];
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
    public Iterator<Short> iterator() {
        return new Iterator<Short>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < count;
            }

            @Override
            public Short next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                return values[index++];
            }
        };
    }

    public short get(int defaultValue) {
        return get(0, defaultValue);
    }

    /**
     * Note: Constants in java and painless are ints, so letting the defaultValue be an int allows users to
     *       call this without casting. A short variable will be automatically widened to an int.
     *       If the user does pass a value outside the range, it will be cast down to a short.
     */
    public short get(int index, int defaultValue) {
        if (isEmpty() || index < 0 || index >= count) {
            return (short) defaultValue;
        }

        return values[index];
    }
}
