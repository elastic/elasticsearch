/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.sort;

import org.elasticsearch.test.ESTestCase;

import java.util.Locale;

public class SortModeTests extends ESTestCase {

    public void testSortMode() {
        // we rely on these ordinals in serialization, so changing them breaks bwc.
        assertEquals(0, SortMode.MIN.ordinal());
        assertEquals(1, SortMode.MAX.ordinal());
        assertEquals(2, SortMode.SUM.ordinal());
        assertEquals(3, SortMode.AVG.ordinal());
        assertEquals(4, SortMode.MEDIAN.ordinal());

        assertEquals("min", SortMode.MIN.toString());
        assertEquals("max", SortMode.MAX.toString());
        assertEquals("sum", SortMode.SUM.toString());
        assertEquals("avg", SortMode.AVG.toString());
        assertEquals("median", SortMode.MEDIAN.toString());

        for (SortMode mode : SortMode.values()) {
            assertEquals(mode, SortMode.fromString(mode.toString()));
            assertEquals(mode, SortMode.fromString(mode.toString().toUpperCase(Locale.ROOT)));
        }
    }

    public void testParsingFromStringExceptions() {
        Exception e = expectThrows(NullPointerException.class, () -> SortMode.fromString(null));
        assertEquals("input string is null", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> SortMode.fromString("xyz"));
        assertEquals("Unknown SortMode [xyz]", e.getMessage());
    }
}
