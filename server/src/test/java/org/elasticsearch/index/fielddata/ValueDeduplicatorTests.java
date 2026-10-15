/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues.ValueOrder;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * How far back a duplicate can be depends on the order the values arrive in, which is the whole reason this
 * exists: sorted values put equal ones next to each other, values in array order do not.
 */
public class ValueDeduplicatorTests extends ESTestCase {

    public void testSortedValuesDropTheirNeighbours() {
        assertEquals(List.of("a", "b", "c"), kept(ValueOrder.SORTED, "a", "a", "b", "b", "b", "c"));
    }

    /**
     * Comparing against the value just seen is all sorted values need, and all they get: a repeat further back
     * cannot happen, so nothing is spent looking for one.
     */
    public void testSortedValuesOnlyLookAtTheirNeighbours() {
        assertEquals(List.of("a", "b", "a"), kept(ValueOrder.SORTED, "a", "b", "a"));
    }

    public void testArrayValuesDropARepeatAtAnyDistance() {
        assertEquals(List.of("c", "a", "b"), kept(ValueOrder.ARRAY, "c", "a", "b", "a", "c", "c"));
    }

    public void testEachDocumentStartsOver() {
        final ValueDeduplicator duplicates = new ValueDeduplicator(ValueOrder.ARRAY);
        duplicates.reset(2);
        assertFalse(duplicates.seen(new BytesRef("a")));
        assertTrue(duplicates.seen(new BytesRef("a")));
        duplicates.reset(1);
        assertFalse("the value belongs to the previous document", duplicates.seen(new BytesRef("a")));
    }

    /** The values are copied as they are recorded, so a caller reusing one buffer is still deduplicated. */
    public void testAReusedBufferIsStillDeduplicated() {
        final ValueDeduplicator duplicates = new ValueDeduplicator(ValueOrder.ARRAY);
        final BytesRef reused = new BytesRef(new byte[] { 'a' });
        duplicates.reset(3);
        assertFalse(duplicates.seen(reused));
        reused.bytes[0] = 'b';
        assertFalse(duplicates.seen(reused));
        reused.bytes[0] = 'a';
        assertTrue(duplicates.seen(reused));
    }

    /** The two ways a document's duplicates are found have to agree, whichever side of the threshold it lands on. */
    public void testTheScanAndTheSetAgree() {
        for (int valueCount : new int[] { 1, ValueDeduplicator.SCAN_LIMIT, ValueDeduplicator.SCAN_LIMIT + 1, 200 }) {
            final String[] values = new String[valueCount];
            for (int i = 0; i < valueCount; i++) {
                // Every value repeats once, so exactly half of them survive, and the repeats are far apart.
                values[i] = "v" + (i % Math.max(1, valueCount / 2));
            }
            final List<String> expected = new ArrayList<>();
            for (int i = 0; i < Math.max(1, valueCount / 2); i++) {
                expected.add("v" + i);
            }
            assertEquals("for " + valueCount + " values", expected, kept(ValueOrder.ARRAY, values));
        }
    }

    /** The set is stamped rather than emptied, so one instance has to stay right across many documents. */
    public void testTheSetIsReusedAcrossDocuments() {
        final ValueDeduplicator duplicates = new ValueDeduplicator(ValueOrder.ARRAY);
        for (int doc = 0; doc < 100; doc++) {
            duplicates.reset(20);
            for (int i = 0; i < 20; i++) {
                assertFalse("doc " + doc + " value " + i, duplicates.seen(new BytesRef("d" + doc + "v" + i)));
            }
            assertTrue(duplicates.seen(new BytesRef("d" + doc + "v0")));
        }
    }

    /**
     * The value count only sizes the set, so a document holding far more than it promised has to grow it. Getting
     * this wrong fills the table and the probe never finds a free slot, which hangs rather than fails.
     */
    public void testADocumentLargerThanItPromisedDoesNotFillTheSet() {
        final ValueDeduplicator duplicates = new ValueDeduplicator(ValueOrder.ARRAY);
        duplicates.reset(ValueDeduplicator.SCAN_LIMIT + 1);
        for (int i = 0; i < 5000; i++) {
            assertFalse("value " + i, duplicates.seen(new BytesRef("v" + i)));
        }
        for (int i = 0; i < 5000; i++) {
            assertTrue("value " + i, duplicates.seen(new BytesRef("v" + i)));
        }
    }

    public void testManyValuesGrowTheScratch() {
        final String[] values = new String[64];
        for (int i = 0; i < values.length; i++) {
            values[i] = "v" + i;
        }
        assertEquals(List.of(values), kept(ValueOrder.ARRAY, values));
        // and every one of them is now a known repeat
        final ValueDeduplicator duplicates = new ValueDeduplicator(ValueOrder.ARRAY);
        duplicates.reset(values.length);
        for (String value : values) {
            assertFalse(duplicates.seen(new BytesRef(value)));
        }
        for (String value : values) {
            assertTrue(duplicates.seen(new BytesRef(value)));
        }
    }

    /**
     * The constructor the aggregations use, which takes the order off the values rather than being told it. Passing
     * the wrong one here is the whole bug: values in array order would be deduplicated as though they ascended.
     */
    public void testTheOrderIsTakenFromTheValues() {
        final ValueDeduplicator fromArrayValues = new ValueDeduplicator(valuesReporting(ValueOrder.ARRAY));
        fromArrayValues.reset(3);
        assertFalse(fromArrayValues.seen(new BytesRef("a")));
        assertFalse(fromArrayValues.seen(new BytesRef("b")));
        assertTrue("a repeat two values back is still a repeat", fromArrayValues.seen(new BytesRef("a")));

        final ValueDeduplicator fromSortedValues = new ValueDeduplicator(valuesReporting(ValueOrder.SORTED));
        fromSortedValues.reset(3);
        assertFalse(fromSortedValues.seen(new BytesRef("a")));
        assertFalse(fromSortedValues.seen(new BytesRef("b")));
        assertFalse("sorted values never repeat at a distance, so nothing looks for one", fromSortedValues.seen(new BytesRef("a")));
    }

    private static SortableBinaryDocValues valuesReporting(ValueOrder order) {
        return new SortableBinaryDocValues(null) {
            @Override
            public boolean advanceExact(int doc) {
                return false;
            }

            @Override
            public int docValueCount() {
                return 0;
            }

            @Override
            public BytesRef nextValue() {
                throw new UnsupportedOperationException();
            }

            @Override
            public ValueOrder getValueOrder() {
                return order;
            }
        };
    }

    private static List<String> kept(ValueOrder order, String... values) {
        final ValueDeduplicator duplicates = new ValueDeduplicator(order);
        duplicates.reset(values.length);
        final List<String> kept = new ArrayList<>();
        for (String value : values) {
            if (duplicates.seen(new BytesRef(value)) == false) {
                kept.add(value);
            }
        }
        return kept;
    }
}
