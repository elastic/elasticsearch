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

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.painless.CompilerSettings;

import java.util.regex.Pattern;

/*
 * CharSequence that wraps another sequence and limits the number of times charAt can be
 */
public class LimitedCharSequence implements CharSequence {
    private final CharSequence wrapped;
    private final Counter counter;

    // for errors
    private final Pattern pattern;
    private final int limitFactor;

    public static final int MAX_STR_LENGTH = 64;
    private static final String SNIPPET = "...";

    public LimitedCharSequence(CharSequence wrap, Pattern pattern, int limitFactor) {
        if (limitFactor <= 0) {
            throw new IllegalArgumentException("limitFactor must be positive");
        }
        this.wrapped = wrap;
        this.counter = new Counter(limitFactor * wrapped.length());

        this.pattern = pattern;
        this.limitFactor = limitFactor;
    }

    public String details() {
        return (pattern != null ? "pattern: [" +  pattern.pattern() + "], " : "") +
            "limit factor: [" + limitFactor + "], " +
            "char limit: [" + counter.charAtLimit + "], " +
            "count: [" + counter.count + "], " +
            "wrapped: [" + snippet(MAX_STR_LENGTH) + "]";
    }

    /**
     * Snip a long wrapped CharSequences for error messages
     */
    String snippet(int maxStrLength) {
        if (maxStrLength < SNIPPET.length() * 6) {
            throw new IllegalArgumentException("max str length must be large enough to include three snippets and three context chars, " +
                "at least [" + SNIPPET.length() * 6 +"], not [" + maxStrLength + "]");
        }

        if (wrapped.length() <= maxStrLength) {
            return wrapped.toString();
        }

        return wrapped.subSequence(0, maxStrLength - SNIPPET.length()) + "..." ;
    }

    @Override
    public int length() {
        return wrapped.length();
    }

    @Override
    public char charAt(int index) {
        counter.count++;
        if (counter.hitLimit()) {
            throw new CircuitBreakingException("[scripting] Regular expression considered too many characters, " + details() +
            ", this limit can be changed by changed by the [" + CompilerSettings.REGEX_LIMIT_FACTOR.getKey() + "] setting",
                CircuitBreaker.Durability.TRANSIENT);
        }
        return wrapped.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return wrapped.subSequence(start, end);
    }

    @Override
    public String toString() {
        return wrapped.toString();
    }

    // Counter object to keep track of charAts for original sequence and all subsequences
    private static class Counter {
        public final int charAtLimit;
        public int count;

        Counter(int charAtLimit) {
            this.charAtLimit = charAtLimit;
            this.count = 0;
        }

        boolean hitLimit() {
            return count > charAtLimit;
        }
    }
}
