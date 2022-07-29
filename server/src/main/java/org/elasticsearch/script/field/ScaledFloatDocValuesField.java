/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

public class ScaledFloatDocValuesField extends AbstractScriptFieldFactory<Double>
    implements
        Field<Double>,
        DocValuesScriptFieldFactory,
        ScriptDocValues.Supplier<Double> {

    protected final SortedNumericDoubleValues input;
    protected final String name;

    protected double[] values = new double[0];
    protected int count;

    private ScriptDocValues.Doubles doubles = null;

    public ScaledFloatDocValuesField(SortedNumericDoubleValues input, String name) {
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

    @Override
    public ScriptDocValues<Double> toScriptDocValues() {
        if (doubles == null) {
            doubles = new ScriptDocValues.Doubles(this);
        }

        return doubles;
    }

    @Override
    public Double getInternal(int index) {
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

    public double get(double defaultValue) {
        return get(0, defaultValue);
    }

    public double get(int index, double defaultValue) {
        if (isEmpty() || index < 0 || index >= count) {
            return defaultValue;
        }

        return values[index];
    }

    @Override
    public PrimitiveIterator.OfDouble iterator() {
        return new PrimitiveIterator.OfDouble() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < count;
            }

            @Override
            public Double next() {
                return nextDouble();
            }

            @Override
            public double nextDouble() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }

                return values[index++];
            }
        };
    }
}
