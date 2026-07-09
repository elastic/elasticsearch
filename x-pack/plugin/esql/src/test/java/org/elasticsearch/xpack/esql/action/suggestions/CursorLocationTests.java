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
        // The marker sits right at the start of line 2, on the '|'.
        CursorMarker marker = CursorMarker.of("FROM foo\n<*>| WHERE a == 1\n| KEEP a");
        String query = marker.query();
        CursorLocation locations = new CursorLocation(query);

        Location line2 = locations.toLocation(marker.cursor());
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

    public void testSupplementaryPlaneCharacterBeforeCursorRoundTrips() {
        // U+1F600 GRINNING FACE is a supplementary-plane character: 1 code point, 2 UTF-16 units.
        String emoji = "\uD83D\uDE00";
        String query = "FROM foo | WHERE a == \"" + emoji + "x\"";
        CursorLocation locations = new CursorLocation(query);

        // Cursor (UTF-16 offset) placed right after the emoji, before 'x'.
        int cursor = query.indexOf(emoji) + emoji.length();
        Location location = locations.toLocation(cursor);
        int back = locations.toOffset(location.getLineNumber(), location.getColumnNumber() - 1);
        assertEquals(cursor, back);

        // Every UTF-16 offset that sits on a code-point boundary (i.e. not splitting a surrogate
        // pair — the position a real caret could occupy) round-trips through (line, code-point
        // column) and back.
        int offset = 0;
        while (true) {
            Location loc = locations.toLocation(offset);
            int roundTripped = locations.toOffset(loc.getLineNumber(), loc.getColumnNumber() - 1);
            assertEquals("round trip at offset " + offset, offset, roundTripped);
            if (offset == query.length()) {
                break;
            }
            offset += Character.charCount(query.codePointAt(offset));
        }
    }

    public void testSupplementaryPlaneCharacterAfterCursorRoundTrips() {
        String emoji = "\uD83D\uDE00";
        String query = "FROM foo | WHERE a == \"x" + emoji + "\"";
        CursorLocation locations = new CursorLocation(query);

        // Cursor placed right before 'x', which is before the emoji.
        int cursor = query.indexOf("x" + emoji);
        Location location = locations.toLocation(cursor);
        int back = locations.toOffset(location.getLineNumber(), location.getColumnNumber() - 1);
        assertEquals(cursor, back);
    }

    public void testSupplementaryPlaneCharacterInStringLiteralRange() {
        // The literal itself contains the surrogate pair; the Source's captured text length is in
        // UTF-16 units (same space as the query string), so range() must line up with it exactly.
        String emoji = "\uD83D\uDE00";
        String query = "FROM foo\n| WHERE agent == \"a" + emoji + "b\"";
        CursorLocation locations = new CursorLocation(query);

        int litStart = query.indexOf("\"a" + emoji + "b\"");
        String litText = "\"a" + emoji + "b\"";
        Source source = new Source(new Location(2, litStart - query.indexOf('\n') - 1), litText);

        OffsetRange range = locations.range(source);
        assertEquals(litStart, range.start());
        assertEquals(litStart + litText.length(), range.end());
        // Cursor right after the emoji (still inside the literal) must be contained.
        int cursorAfterEmoji = query.indexOf(emoji) + emoji.length();
        assertTrue(range.contains(cursorAfterEmoji));
    }

    public void testBmpNonAsciiCharacterRoundTrips() {
        // Accented Latin and CJK characters within the BMP: UTF-16 units and code points coincide,
        // so this already works today. Contrast with the supplementary-plane cases above.
        String query = "FROM foo | WHERE café == \"日本語\"";
        CursorLocation locations = new CursorLocation(query);

        for (int offset = 0; offset <= query.length(); offset++) {
            Location loc = locations.toLocation(offset);
            int roundTripped = locations.toOffset(loc.getLineNumber(), loc.getColumnNumber() - 1);
            assertEquals("round trip at offset " + offset, offset, roundTripped);
        }
    }
}
