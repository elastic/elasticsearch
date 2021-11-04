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

public class ByteDocValuesFieldTests extends ESTestCase {
    public void testByteField() throws IOException {
        byte[][] values = new byte[between(3, 10)][];
        for (int d = 0; d < values.length; d++) {
            values[d] = new byte[randomBoolean() ? randomBoolean() ? 0 : 1 : between(2, 100)];
            for (int i = 0; i < values[d].length; i++) {
                values[d][i] = randomByte();
            }
        }

        ByteDocValuesField byteField = wrap(values);

        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            byteField.setNextDocId(d);
            if (values[d].length > 0) {
                assertEquals(values[d][0], byteField.get(Byte.MIN_VALUE));
                assertEquals(values[d][0], byteField.get(0, Byte.MIN_VALUE));
            }
            int count = 0;
            for (int ignored : byteField) {
                count++;
            }
            assertEquals(count, byteField.size());
            assertEquals(values[d].length, byteField.size());
            for (int i = 0; i < values[d].length; i++) {
                assertEquals(values[d][i], byteField.get(i, Byte.MIN_VALUE));
            }
        }
    }

    private ByteDocValuesField wrap(byte[][] values) {
        return new ByteDocValuesField(new AbstractSortedNumericDocValues() {
            byte[] current;
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
