/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.elasticsearch.script.field.ByteDocValuesField;
import org.elasticsearch.script.field.IntegerDocValuesField;
import org.elasticsearch.script.field.ShortDocValuesField;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.function.LongSupplier;

public class IntegralDocValuesFieldTests extends ESTestCase {
    public void testByteField() throws IOException {
        long[][] values = generate(ESTestCase::randomByte);
        ByteDocValuesField byteField = new ByteDocValuesField(wrap(values), "test");
        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            byteField.setNextDocId(d);
            if (values[d].length > 0) {
                assertEquals(values[d][0], byteField.get(Byte.MIN_VALUE));
                assertEquals(values[d][0], byteField.get(0, Byte.MIN_VALUE));
            }
            assertEquals(values[d].length, byteField.size());
            for (int i = 0; i < values[d].length; i++) {
                assertEquals(values[d][i], byteField.get(i, Byte.MIN_VALUE));
            }
            int i = 0;
            for (byte b : byteField) {
                assertEquals(values[d][i++], b);
            }
        }
    }

    public void testShortField() throws IOException {
        long[][] values = generate(ESTestCase::randomShort);
        ShortDocValuesField shortField = new ShortDocValuesField(wrap(values), "test");
        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            shortField.setNextDocId(d);
            if (values[d].length > 0) {
                assertEquals(values[d][0], shortField.get(Short.MIN_VALUE));
                assertEquals(values[d][0], shortField.get(0, Short.MIN_VALUE));
            }
            assertEquals(values[d].length, shortField.size());
            for (int i = 0; i < values[d].length; i++) {
                assertEquals(values[d][i], shortField.get(i, Short.MIN_VALUE));
            }
            int i = 0;
            for (short s : shortField) {
                assertEquals(values[d][i++], s);
            }
        }
    }

    public void testIntegerField() throws IOException {
        long[][] values = generate(ESTestCase::randomInt);
        IntegerDocValuesField intField = new IntegerDocValuesField(wrap(values), "test");
        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            intField.setNextDocId(d);
            if (values[d].length > 0) {
                assertEquals(values[d][0], intField.get(Integer.MIN_VALUE));
                assertEquals(values[d][0], intField.get(0, Integer.MIN_VALUE));
            }
            assertEquals(values[d].length, intField.size());
            for (int i = 0; i < values[d].length; i++) {
                assertEquals(values[d][i], intField.get(i, Integer.MIN_VALUE));
            }
            int i = 0;
            for (int ii : intField) {
                assertEquals(values[d][i++], ii);
            }
        }
    }

    protected long[][] generate(LongSupplier supplier) {
        long[][] values = new long[between(3, 10)][];
        for (int d = 0; d < values.length; d++) {
            values[d] = new long[randomBoolean() ? randomBoolean() ? 0 : 1 : between(2, 100)];
            for (int i = 0; i < values[d].length; i++) {
                values[d][i] = supplier.getAsLong();
            }
        }
        return values;
    }

    protected AbstractSortedNumericDocValues wrap(long[][] values) {
        return new AbstractSortedNumericDocValues() {
            long[] current;
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
            public long nextValue() {
                return current[i++];
            }
        };
    }
}
