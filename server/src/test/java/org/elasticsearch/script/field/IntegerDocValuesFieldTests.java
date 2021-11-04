/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.index.fielddata.AbstractSortedNumericDocValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class IntegerDocValuesFieldTests extends ESTestCase {
    public void testIntegerField() throws IOException {
        int[][] values = new int[between(3, 10)][];
        for (int d = 0; d < values.length; d++) {
            values[d] = new int[randomBoolean() ? randomBoolean() ? 0 : 1 : between(2, 100)];
            for (int i = 0; i < values[d].length; i++) {
                values[d][i] = randomInt();
            }
        }

        IntegerDocValuesField integerField = wrap(values);

        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            integerField.setNextDocId(d);
            if (values[d].length > 0) {
                assertEquals(values[d][0], integerField.get(Integer.MIN_VALUE));
                assertEquals(values[d][0], integerField.get(0, Integer.MIN_VALUE));
            }
            int count = 0;
            for (int ignored : integerField) {
                count++;
            }
            assertEquals(count, integerField.size());
            assertEquals(values[d].length, integerField.size());
            for (int i = 0; i < values[d].length; i++) {
                assertEquals(values[d][i], integerField.get(i, Integer.MIN_VALUE));
            }
        }
    }

    private IntegerDocValuesField wrap(int[][] values) {
        return new IntegerDocValuesField(new AbstractSortedNumericDocValues() {
            int[] current;
            int i;

            @Override
            public boolean advanceExact(int target) throws IOException {
                i = 0;
                current = values[target];
                return current.length > 0;
            }

            @Override
            public long nextValue() throws IOException {
                return current[i++];
            }

            @Override
            public int docValueCount() {
                return current.length;
            }
        }, "myfield");
    }
}
