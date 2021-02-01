/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
