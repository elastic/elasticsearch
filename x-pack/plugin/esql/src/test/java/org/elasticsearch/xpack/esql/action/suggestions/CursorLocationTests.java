/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action.suggestions;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.suggestions.CursorLocation.OffsetRange;
import org.elasticsearch.xpack.esql.core.tree.Location;
import org.elasticsearch.xpack.esql.core.tree.Source;

public class CursorLocationTests extends ESTestCase {

    public void testSingleLineOffsets() {
        String query = "FROM foo | KEEP a";
        CursorLocation locations = new CursorLocation(query);

        Location start = locations.toLocation(0);
        assertEquals(1, start.getLineNumber());
        assertEquals(1, start.getColumnNumber()); // 1-based column, 0-based charPositionInLine

        Location mid = locations.toLocation(5); // 'f' of foo
        assertEquals(1, mid.getLineNumber());
        assertEquals(6, mid.getColumnNumber());

        Location eof = locations.toLocation(query.length());
        assertEquals(1, eof.getLineNumber());
        assertEquals(query.length() + 1, eof.getColumnNumber());
    }

    public void testMultiLineOffsets() {
        // Offsets: 0123456 7890123456789012 34...
        String query = "FROM foo\n| WHERE a == 1\n| KEEP a";
        CursorLocation locations = new CursorLocation(query);

        // Offset 9 is '|' at the start of line 2 (offset 8 is the newline).
        Location line2 = locations.toLocation(9);
        assertEquals(2, line2.getLineNumber());
        assertEquals(1, line2.getColumnNumber());

        // Start of line 3.
        int line3Start = query.indexOf("| KEEP a");
        Location line3 = locations.toLocation(line3Start);
        assertEquals(3, line3.getLineNumber());
        assertEquals(1, line3.getColumnNumber());

        // Round trip: offset -> (line,col) -> offset.
        for (int offset = 0; offset <= query.length(); offset++) {
            Location location = locations.toLocation(offset);
            int back = locations.toOffset(location.getLineNumber(), location.getColumnNumber() - 1);
            assertEquals("round trip at offset " + offset, offset, back);
        }
    }

    public void testSourceRange() {
        String query = "FROM foo\n| WHERE agent == \"as\"";
        CursorLocation locations = new CursorLocation(query);

        // A synthetic source covering the literal "as" on line 2.
        int litStart = query.indexOf("\"as\"");
        String litText = "\"as\"";
        Source source = new Source(new Location(2, litStart - query.indexOf('\n') - 1), litText);

        OffsetRange range = locations.range(source);
        assertEquals(litStart, range.start());
        assertEquals(litStart + litText.length(), range.end());
        assertTrue(range.contains(litStart));
        assertTrue(range.contains(litStart + 1));
        assertFalse(range.contains(range.end()));
        assertTrue(range.containsInclusive(range.end()));
    }

    public void testOffsetOutOfBoundsThrows() {
        CursorLocation locations = new CursorLocation("FROM foo");
        expectThrows(IllegalArgumentException.class, () -> locations.toLocation(-1));
        expectThrows(IllegalArgumentException.class, () -> locations.toLocation(100));
    }
}
