/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.bytes;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.StringDescription;

import static org.elasticsearch.common.bytes.BytesReferenceTestUtils.equalBytes;

public class BytesReferenceTestUtilsTests extends ESTestCase {
    public void testEqualBytes() {
        final var expectedBytes = randomBoolean()
            ? new BytesArray(new byte[] { 0, 1, 2, 3, 99 })
            : CompositeBytesReference.of(new BytesArray(new byte[] { 0, 1 }), new BytesArray(new byte[] { 2, 3, 99 }));

        final var matcher = equalBytes(expectedBytes);
        assertTrue(matcher.matches(expectedBytes));
        assertTrue(matcher.matches(new BytesArray(BytesReference.toBytes(expectedBytes))));
        assertTrue(matcher.matches(CompositeBytesReference.of(expectedBytes.slice(0, 3), expectedBytes.slice(3, 2))));
        assertFalse(matcher.matches(randomBytesReference(5)));

        final var description = new StringDescription();
        matcher.describeTo(description);
        assertEquals("BytesReference[00 01 02 03 63]", description.toString());

        final var nullMismatchDescription = new StringDescription();
        matcher.describeMismatch(null, nullMismatchDescription);
        assertEquals("was null", nullMismatchDescription.toString());

        final var unequalMismatchDescription = new StringDescription();
        matcher.describeMismatch(new BytesArray(new byte[] { 0, 1, 2, 3, 15 }), unequalMismatchDescription);
        assertEquals("was BytesReference[00 01 02 03 0f]", unequalMismatchDescription.toString());

        final var nullMatcher = equalBytes(null);
        assertTrue(nullMatcher.matches(null));
        assertFalse(nullMatcher.matches(randomBytesReference(5)));

        final var nullMatcherDescription = new StringDescription();
        nullMatcher.describeTo(nullMatcherDescription);
        assertEquals("null", nullMatcherDescription.toString());
    }
}
