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

public class BooleanDocValuesField implements DocValuesField {

    private final SortedNumericDocValues input;
    private final String name;

    private boolean[] values = new boolean[0];
    private int count;

    private ScriptDocValues.Booleans booleansSDV = null;

    public BooleanDocValuesField(SortedNumericDocValues input, String name) {
        this.input = input;
        this.name = name;
    }

    /**
     * Set the current document ID.
     *
     * @param docId
     */
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

    /**
     * Returns a {@code ScriptDocValues} of the appropriate type for this field.
     * This is used to support backwards compatibility for accessing field values
     * through the {@code doc} variable.
     */
    @Override
    public ScriptDocValues<?> getScriptDocValues() {
        if (booleansSDV == null) {
            booleansSDV = new ScriptDocValues.Booleans(this);
        }

        return booleansSDV;
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

    public boolean[] getValues() {
        if (isEmpty()) {
            return new boolean[0];
        }
        return Arrays.copyOf(values, count);
    }

    public boolean getValue(boolean defaultValue) {
        return getValue(0, defaultValue);
    }

    public boolean getValue(int index, boolean defaultValue) {
        if (isEmpty() || index < 0 || index >= count) {
            return defaultValue;
        }

        return values[index];
    }

    public boolean[] getInternalValues() {
        return values;
    }

}
