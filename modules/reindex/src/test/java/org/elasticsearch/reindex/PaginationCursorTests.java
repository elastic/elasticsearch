/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for {@link PaginationCursor}.
 */
public class PaginationCursorTests extends ESTestCase {

    /** Verifies that {@link PaginationCursor#forScroll} creates a cursor with the given scroll ID and scroll semantics. */
    public void testForScrollCreatesScrollCursor() {
        String scrollId = randomAlphaOfLengthBetween(1, 20);
        PaginationCursor cursor = PaginationCursor.forScroll(scrollId);
        assertThat(cursor.scrollId(), equalTo(scrollId));
        assertThat(cursor.searchAfter(), nullValue());
        assertTrue(cursor.isScroll());
        assertFalse(cursor.isSearchAfter());
    }

    /** Verifies that {@link PaginationCursor#forScroll} rejects null and empty scroll IDs. */
    public void testForScrollRejectsNullAndEmpty() {
        expectThrows(NullPointerException.class, () -> PaginationCursor.forScroll(null));
        expectThrows(IllegalArgumentException.class, () -> PaginationCursor.forScroll(""));
    }

    /** Verifies that {@link PaginationCursor#forSearchAfter} rejects null search_after arrays. */
    public void testForSearchAfterRejectsNull() {
        expectThrows(IllegalArgumentException.class, () -> PaginationCursor.forSearchAfter(null));
    }

    /** Verifies that the constructor requires exactly one of scrollId or searchAfter to be non-null, and rejects both or neither. */
    public void testConstructorEnforcesExactlyOneNonNull() {
        expectThrows(IllegalArgumentException.class, () -> new PaginationCursor(null, null));
        expectThrows(IllegalArgumentException.class, () -> new PaginationCursor("scroll", new Object[] { 1 }));
        expectThrows(IllegalArgumentException.class, () -> new PaginationCursor("", null));
    }

    /** Verifies that {@link PaginationCursor#forSearchAfter} creates a cursor with the
     * given search_after values and search_after semantics.
     */
    public void testForSearchAfterCreatesSearchAfterCursor() {
        long sortLong = randomLong();
        String sortString = randomAlphaOfLengthBetween(1, 10);
        Object[] searchAfter = new Object[] { sortLong, sortString };
        PaginationCursor cursor = PaginationCursor.forSearchAfter(searchAfter);

        assertThat(cursor.scrollId(), nullValue());
        assertThat(cursor.searchAfter(), equalTo(searchAfter));
        assertFalse(cursor.isScroll());
        assertTrue(cursor.isSearchAfter());

        // Test that the search after values are preserved
        assertNotNull(cursor.searchAfter());
        assertEquals(2, cursor.searchAfter().length);
        assertEquals(sortLong, cursor.searchAfter()[0]);
        assertEquals(sortString, cursor.searchAfter()[1]);
    }

    /** Verifies that {@link PaginationCursor#forSearchAfter} accepts an empty search_after array. */
    public void testEmptySearchAfterArray() {
        Object[] searchAfter = new Object[0];
        PaginationCursor cursor = PaginationCursor.forSearchAfter(searchAfter);
        assertThat(cursor.scrollId(), nullValue());
        assertThat(cursor.searchAfter(), equalTo(searchAfter));
        assertFalse(cursor.isScroll());
        assertTrue(cursor.isSearchAfter());
    }
}
