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
import java.util.Iterator;
import java.util.NoSuchElementException;

public class BooleanDocValuesField extends AbstractScriptFieldFactory<Boolean>
    implements
        Field<Boolean>,
        DocValuesScriptFieldFactory,
        ScriptDocValues.Supplier<Boolean> {

    private final SortedNumericDocValues input;
    private final String name;

    private boolean[] values = new boolean[0];
    private int count;

    // used for backwards compatibility for old-style "doc" access
    // as a delegate to this field class
    private ScriptDocValues.Booleans booleans = null;

    public BooleanDocValuesField(SortedNumericDocValues input, String name) {
        this.input = input;
        this.name = name;
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
    public ScriptDocValues<Boolean> toScriptDocValues() {
        if (booleans == null) {
            booleans = new ScriptDocValues.Booleans(this);
        }

        return booleans;
    }

    // this method is required to support the Boolean return values
    // for the old-style "doc" access in ScriptDocValues
    @Override
    public Boolean getInternal(int index) {
        return values[index];
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

    public boolean get(boolean defaultValue) {
        return get(0, defaultValue);
    }

    public boolean get(int index, boolean defaultValue) {
        if (isEmpty() || index < 0 || index >= count) {
            return defaultValue;
        }

        return values[index];
    }

    @Override
    public Iterator<Boolean> iterator() {
        return new Iterator<Boolean>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < count;
            }

            @Override
            public Boolean next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                return values[index++];
            }
        };
    }
}
