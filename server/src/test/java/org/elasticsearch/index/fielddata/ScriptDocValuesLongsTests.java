/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.elasticsearch.index.fielddata.ScriptDocValues.Longs;
import org.elasticsearch.script.field.LongDocValuesField;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class ScriptDocValuesLongsTests extends ESTestCase {
    public void testLongs() throws IOException {
        long[][] values = new long[between(3, 10)][];
        for (int d = 0; d < values.length; d++) {
            values[d] = new long[randomBoolean() ? randomBoolean() ? 0 : 1 : between(2, 100)];
            for (int i = 0; i < values[d].length; i++) {
                values[d][i] = randomLong();
            }
        }

        LongDocValuesField longField = wrap(values);
        Longs longs = (Longs) longField.toScriptDocValues();

        for (int round = 0; round < 10; round++) {
            int d = between(0, values.length - 1);
            longField.setNextDocId(d);
            if (values[d].length > 0) {
                assertEquals(values[d][0], longs.getValue());
                assertEquals(values[d][0], (long) longs.get(0));
                assertEquals(values[d][0], longField.get(Long.MIN_VALUE));
                assertEquals(values[d][0], longField.get(0, Long.MIN_VALUE));
            } else {
                Exception e = expectThrows(IllegalStateException.class, longs::getValue);
                assertEquals(
                    "A document doesn't have a value for a field! "
                        + "Use doc[<field>].size()==0 to check if a document is missing a field!",
                    e.getMessage()
                );
                e = expectThrows(IllegalStateException.class, () -> longs.get(0));
                assertEquals(
                    "A document doesn't have a value for a field! "
                        + "Use doc[<field>].size()==0 to check if a document is missing a field!",
                    e.getMessage()
                );
            }
            assertEquals(values[d].length, longField.size());
            for (int i = 0; i < values[d].length; i++) {
                assertEquals(values[d][i], longs.get(i).longValue());
                assertEquals(values[d][i], longField.get(i, Long.MIN_VALUE));
            }

            Exception e = expectThrows(UnsupportedOperationException.class, () -> longs.add(100L));
            assertEquals("doc values are unmodifiable", e.getMessage());
        }
    }

    private LongDocValuesField wrap(long[][] values) {
        return new LongDocValuesField(new AbstractSortedNumericDocValues() {
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
        }, "test");
    }
}
