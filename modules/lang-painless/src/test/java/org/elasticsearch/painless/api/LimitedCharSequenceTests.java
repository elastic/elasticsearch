/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.api;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.test.ESTestCase;

import java.util.regex.Pattern;

public class LimitedCharSequenceTests extends ESTestCase {
    public void testBadFactor() {
        IllegalArgumentException badArg = expectThrows(IllegalArgumentException.class,
            () -> new LimitedCharSequence("abc", null, -1)
        );
        assertEquals("limitFactor must be positive", badArg.getMessage());

        badArg = expectThrows(IllegalArgumentException.class,
            () -> new LimitedCharSequence("abc", null, 0)
        );
        assertEquals("limitFactor must be positive", badArg.getMessage());
    }

    public void testLength() {
        String str = "abc";
        assertEquals(str.length(), new LimitedCharSequence("abc", null, 1).length());
    }

    public void testCharAtEqualLimit() {
        String str = "abc";
        for (int limitFactor=1; limitFactor < 4; limitFactor++){
            CharSequence seq = new LimitedCharSequence(str, null, limitFactor);
            for (int i=0; i<str.length() * limitFactor; i++) {
                seq.charAt(0);
            }
        }
    }

    public void testCharAtAboveLimit() {
        String str = "abc";
        String patternStr = "a.*bc";
        Pattern p = Pattern.compile(patternStr);
        final CharSequence seq = new LimitedCharSequence(str, p, 2);
        for (int i = 0; i < 6; i++) {
            seq.charAt(0);
        }
        CircuitBreakingException circuitBreakingException = expectThrows(CircuitBreakingException.class, () -> seq.charAt(0));
        assertEquals(
            "[scripting] Regular expression considered too many characters, " +
                "pattern: [a.*bc], " +
                "limit factor: [2], " +
                "char limit: [6], " +
                "count: [7], " +
                "wrapped: [abc], " +
                "this limit can be changed by changed by the [script.painless.regex.limit-factor] setting",
            circuitBreakingException.getMessage());

        final CharSequence seqNullPattern = new LimitedCharSequence(str, null, 2);
        for (int i = 0; i < 6; i++) {
            seqNullPattern.charAt(0);
        }
        circuitBreakingException = expectThrows(CircuitBreakingException.class, () -> seqNullPattern.charAt(0));
        assertEquals(
            "[scripting] Regular expression considered too many characters, " +
                "limit factor: [2], " +
                "char limit: [6], " +
                "count: [7], " +
                "wrapped: [abc], " +
                "this limit can be changed by changed by the [script.painless.regex.limit-factor] setting",
            circuitBreakingException.getMessage());
    }

    public void testSubSequence() {
        assertEquals("def", (new LimitedCharSequence("abcdef", null, 1)).subSequence(3, 6));
    }

    public void testToString() {
        String str = "abc";
        assertEquals(str, new LimitedCharSequence(str, null, 1).toString());
    }
}
