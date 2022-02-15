/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.elasticsearch.script.field.DoubleDocValuesField;
import org.elasticsearch.script.field.FloatDocValuesField;
import org.elasticsearch.script.field.HalfFloatDocValuesField;
import org.elasticsearch.script.field.ScaledFloatDocValuesField;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.function.DoubleSupplier;

public class FloatDocValuesFieldTests extends ESTestCase {
    public void testFloatField() throws IOException {
        double[][] values = generate(ESTestCase::randomFloat);
        FloatDocValuesField floatField = new FloatDocValuesField(wrap(values), "test");
        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            floatField.setNextDocId(d);
            if (values[d].length > 0) {
                assertEquals(values[d][0], floatField.get(Float.MIN_VALUE), 0.0);
                assertEquals(values[d][0], floatField.get(0, Float.MIN_VALUE), 0.0);
            }
            assertEquals(values[d].length, floatField.size());
            for (int i = 0; i < values[d].length; i++) {
                assertEquals(values[d][i], floatField.get(i, Float.MIN_VALUE), 0.0);
            }
            int i = 0;
            for (float f : floatField) {
                assertEquals(values[d][i++], f, 0.0);
            }
        }
    }

    public void testDoubleField() throws IOException {
        double[][] values = generate(ESTestCase::randomFloat);
        DoubleDocValuesField doubleField = new DoubleDocValuesField(wrap(values), "test");
        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            doubleField.setNextDocId(d);
            if (values[d].length > 0) {
                assertEquals(values[d][0], doubleField.get(Float.MIN_VALUE), 0.0);
                assertEquals(values[d][0], doubleField.get(0, Float.MIN_VALUE), 0.0);
            }
            assertEquals(values[d].length, doubleField.size());
            for (int i = 0; i < values[d].length; i++) {
                assertEquals(values[d][i], doubleField.get(i, Float.MIN_VALUE), 0.0);
            }
            int i = 0;
            for (double dbl : doubleField) {
                assertEquals(values[d][i++], dbl, 0.0);
            }
        }
    }

    public void testScaledFloatField() throws IOException {
        double[][] values = generate(ESTestCase::randomDouble);
        ScaledFloatDocValuesField scaledFloatField = new ScaledFloatDocValuesField(wrap(values), "test");
        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            scaledFloatField.setNextDocId(d);
            if (values[d].length > 0) {
                assertEquals(values[d][0], scaledFloatField.get(Double.MIN_VALUE), 0.0);
                assertEquals(values[d][0], scaledFloatField.get(0, Double.MIN_VALUE), 0.0);
            }
            assertEquals(values[d].length, scaledFloatField.size());
            for (int i = 0; i < values[d].length; i++) {
                assertEquals(values[d][i], scaledFloatField.get(i, Double.MIN_VALUE), 0.0);
            }
            int i = 0;
            for (double dbl : scaledFloatField) {
                assertEquals(values[d][i++], dbl, 0.0);
            }
        }
    }

    public void testHalfFloatField() throws IOException {
        double[][] values = generate(ESTestCase::randomDouble);
        HalfFloatDocValuesField halfFloatField = new HalfFloatDocValuesField(wrap(values), "test");
        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            halfFloatField.setNextDocId(d);
            if (values[d].length > 0) {
                assertEquals((float) values[d][0], halfFloatField.get(Float.MIN_VALUE), 0.0f);
                assertEquals((float) values[d][0], halfFloatField.get(0, Float.MIN_VALUE), 0.0f);
                assertEquals(values[d][0], halfFloatField.asDouble(Double.MIN_VALUE), 0.0);
                assertEquals(values[d][0], halfFloatField.asDouble(0, Double.MIN_VALUE), 0.0);
            }
            assertEquals(values[d].length, halfFloatField.size());
            for (int i = 0; i < values[d].length; i++) {
                assertEquals((float) values[d][i], halfFloatField.get(i, Float.MIN_VALUE), 0.0f);
                assertEquals(values[d][i], halfFloatField.asDouble(i, Double.MIN_VALUE), 0.0);
            }
            int i = 0;
            for (float flt : halfFloatField) {
                assertEquals((float) values[d][i++], flt, 0.0f);
            }
            i = 0;
            for (double dbl : halfFloatField.asDoubles()) {
                assertEquals(values[d][i++], dbl, 0.0);
            }
        }
    }

    protected double[][] generate(DoubleSupplier supplier) {
        double[][] values = new double[between(3, 10)][];
        for (int d = 0; d < values.length; d++) {
            values[d] = new double[randomBoolean() ? randomBoolean() ? 0 : 1 : between(2, 100)];
            for (int i = 0; i < values[d].length; i++) {
                values[d][i] = supplier.getAsDouble();
            }
        }
        return values;
    }

    protected SortedNumericDoubleValues wrap(double[][] values) {
        return new SortedNumericDoubleValues() {
            double[] current;
            int i;

            @Override
            public boolean advanceExact(int doc) {
                i = 0;
                current = values[doc];
                return current.length > 0;
            }

            @Override
            public int docValueCount() {
                return current.length;
            }

            @Override
            public double nextValue() {
                return current[i++];
            }
        };
    }
}
