/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class StringsTests extends ESTestCase {

    public void testIncorrectPattern() {
        AssertionError assertionError = expectThrows(AssertionError.class, () -> Strings.format("%s %s", 1));
        assertThat(
            assertionError.getMessage(),
            equalTo("Exception thrown when formatting [%s %s]. java.util.MissingFormatArgumentException. Format specifier '%s'")
        );
    }

    public void testTruncateForError() {
        assertNull(Strings.truncateForError(null));
        String exact = "x".repeat(Strings.MAX_INPUT_LENGTH_IN_ERROR);
        assertSame(exact, Strings.truncateForError(exact));
        assertEquals(exact + "...", Strings.truncateForError(exact + "y"));
        // A surrogate pair across the cut is dropped whole
        String emoji = "x".repeat(Strings.MAX_INPUT_LENGTH_IN_ERROR - 1) + "\uD83D\uDE00";
        assertEquals(emoji.substring(0, Strings.MAX_INPUT_LENGTH_IN_ERROR - 1) + "...", Strings.truncateForError(emoji));
    }
}
