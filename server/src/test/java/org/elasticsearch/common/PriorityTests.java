/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PriorityTests extends ESTestCase {

    public void testValueOf() {
        for (Priority p : Priority.values()) {
            assertSame(p, Priority.valueOf(p.toString()));
        }

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            Priority.valueOf("foobar");
        });
        assertEquals("No enum constant org.elasticsearch.common.Priority.foobar", exception.getMessage());
    }

    public void testToString() {
        assertEquals("IMMEDIATE", Priority.IMMEDIATE.toString());
        assertEquals("HIGH", Priority.HIGH.toString());
        assertEquals("LANGUID", Priority.LANGUID.toString());
        assertEquals("LOW", Priority.LOW.toString());
        assertEquals("URGENT", Priority.URGENT.toString());
        assertEquals("NORMAL", Priority.NORMAL.toString());
        assertEquals(6, Priority.values().length);
    }

    public void testSerialization() throws IOException {
        for (Priority p : Priority.values()) {
            BytesStreamOutput out = new BytesStreamOutput();
            Priority.writeTo(p, out);
            Priority priority = Priority.readFrom(out.bytes().streamInput());
            assertSame(p, priority);
        }
        assertSame(Priority.IMMEDIATE, Priority.fromByte((byte) 0));
        assertSame(Priority.HIGH, Priority.fromByte((byte) 2));
        assertSame(Priority.LANGUID, Priority.fromByte((byte) 5));
        assertSame(Priority.LOW, Priority.fromByte((byte) 4));
        assertSame(Priority.NORMAL, Priority.fromByte((byte) 3));
        assertSame(Priority.URGENT,Priority.fromByte((byte) 1));
        assertEquals(6, Priority.values().length);
    }

    public void testCompareTo() {
        assertTrue(Priority.IMMEDIATE.compareTo(Priority.URGENT) < 0);
        assertTrue(Priority.URGENT.compareTo(Priority.HIGH) < 0);
        assertTrue(Priority.HIGH.compareTo(Priority.NORMAL) < 0);
        assertTrue(Priority.NORMAL.compareTo(Priority.LOW) < 0);
        assertTrue(Priority.LOW.compareTo(Priority.LANGUID) < 0);

        assertTrue(Priority.URGENT.compareTo(Priority.IMMEDIATE) > 0);
        assertTrue(Priority.HIGH.compareTo(Priority.URGENT) > 0);
        assertTrue(Priority.NORMAL.compareTo(Priority.HIGH) > 0);
        assertTrue(Priority.LOW.compareTo(Priority.NORMAL) > 0);
        assertTrue(Priority.LANGUID.compareTo(Priority.LOW) > 0);

        for (Priority p : Priority.values()) {
            assertEquals(0, p.compareTo(p));
        }
        List<Priority> shuffeledAndSorted = Arrays.asList(Priority.values());
        Collections.shuffle(shuffeledAndSorted, random());
        Collections.sort(shuffeledAndSorted);
        for (List<Priority> priorities : Arrays.asList(shuffeledAndSorted,
            Arrays.asList(Priority.values()))) { // #values() guarantees order!
            assertSame(Priority.IMMEDIATE, priorities.get(0));
            assertSame(Priority.URGENT, priorities.get(1));
            assertSame(Priority.HIGH, priorities.get(2));
            assertSame(Priority.NORMAL, priorities.get(3));
            assertSame(Priority.LOW, priorities.get(4));
            assertSame(Priority.LANGUID, priorities.get(5));
        }
    }
}
