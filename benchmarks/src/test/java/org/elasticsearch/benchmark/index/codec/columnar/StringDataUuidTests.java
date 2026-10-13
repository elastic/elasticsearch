/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

public class StringDataUuidTests extends ESTestCase {

    private static final int DOCS = 20_000;
    private static final int INTERVAL = 4096;

    public void testV4ValuesAreDistinctRandomUuids() {
        final BytesRef[] values = StringData.UUID_V4.generate(DOCS, new Random(randomLong()));
        final Set<String> distinct = new HashSet<>();
        for (BytesRef value : values) {
            final UUID uuid = UUID.fromString(value.utf8ToString());
            assertEquals(4, uuid.version());
            assertEquals(2, uuid.variant());
            assertEquals(36, value.length);
            distinct.add(value.utf8ToString());
        }
        assertEquals(DOCS, distinct.size());
    }

    public void testV7ValuesAreDistinctV7UuidsThatRiseWithoutBeingInOrder() {
        final BytesRef[] values = StringData.UUID_V7.generate(DOCS, new Random(randomLong()));
        final Set<String> distinct = new HashSet<>();
        boolean someValueSortsBeforeItsPredecessor = false;
        for (int doc = 0; doc < DOCS; doc++) {
            final UUID uuid = UUID.fromString(values[doc].utf8ToString());
            assertEquals(7, uuid.version());
            assertEquals(2, uuid.variant());
            distinct.add(values[doc].utf8ToString());
            someValueSortsBeforeItsPredecessor |= doc > 0 && values[doc].compareTo(values[doc - 1]) < 0;
        }
        assertEquals(DOCS, distinct.size());
        assertTrue("a column in term order is bisected and never reaches the skip index", someValueSortsBeforeItsPredecessor);
    }

    public void testV7IntervalsCoverANarrowBandAndV4IntervalsDoNot() {
        final BytesRef[] v7 = StringData.UUID_V7.generate(DOCS, new Random(randomLong()));
        final BytesRef[] v4 = StringData.UUID_V4.generate(DOCS, new Random(randomLong()));
        for (int from = 0; from + INTERVAL <= DOCS; from += INTERVAL) {
            BytesRef v7Min = v7[from];
            BytesRef v7Max = v7[from];
            BytesRef v4Min = v4[from];
            BytesRef v4Max = v4[from];
            for (int doc = from; doc < from + INTERVAL; doc++) {
                v7Min = v7[doc].compareTo(v7Min) < 0 ? v7[doc] : v7Min;
                v7Max = v7[doc].compareTo(v7Max) > 0 ? v7[doc] : v7Max;
                v4Min = v4[doc].compareTo(v4Min) < 0 ? v4[doc] : v4Min;
                v4Max = v4[doc].compareTo(v4Max) > 0 ? v4[doc] : v4Max;
            }
            final long v7Millis = timestamp(v7Max) - timestamp(v7Min);
            assertTrue("a v7 interval spans [" + v7Millis + "] ms", v7Millis <= INTERVAL + 64);
            assertTrue(
                "a v4 interval spans only [" + v4Min.utf8ToString() + ", " + v4Max.utf8ToString() + "]",
                v4Min.bytes[v4Min.offset] < '1' && v4Max.bytes[v4Max.offset] > 'e'
            );
        }
    }

    private static long timestamp(BytesRef value) {
        return UUID.fromString(value.utf8ToString()).getMostSignificantBits() >>> 16;
    }
}
