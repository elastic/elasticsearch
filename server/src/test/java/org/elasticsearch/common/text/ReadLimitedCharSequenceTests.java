/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.text;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class ReadLimitedCharSequenceTests extends ESTestCase {
    public void testBadFactor() {
        IllegalArgumentException badArg = expectThrows(IllegalArgumentException.class, () -> new ReadLimitedCharSequence("abc", -1));
        assertEquals("readLimitFactor must be greater than 0", badArg.getMessage());

        badArg = expectThrows(IllegalArgumentException.class, () -> new ReadLimitedCharSequence("abc", 0));
        assertEquals("readLimitFactor must be greater than 0", badArg.getMessage());
    }

    public void testLength() {
        String str = "abc";
        assertEquals(str.length(), new ReadLimitedCharSequence("abc", 1).length());
    }

    public void testCharAtEqualLimit() {
        String str = "abc";
        for (int limitFactor = 1; limitFactor < 4; limitFactor++) {
            CharSequence seq = new ReadLimitedCharSequence(str, limitFactor);
            for (int i = 0; i < str.length() * limitFactor; i++) {
                seq.charAt(0);
            }
        }
    }

    public void testCharAtAboveLimit() {
        String str = "abc";
        final CharSequence seq = new ReadLimitedCharSequence(str, 2);
        for (int i = 0; i < 6; i++) {
            seq.charAt(0);
        }
        var ex = expectThrows(ReadLimitedCharSequence.LimitExceededException.class, () -> seq.charAt(0));
        assertThat(ex.readLimit(), equalTo(6));
    }

    public void testToString() {
        String str = "abc";
        assertThat(new ReadLimitedCharSequence(str, 1), hasToString(str));
    }
}
