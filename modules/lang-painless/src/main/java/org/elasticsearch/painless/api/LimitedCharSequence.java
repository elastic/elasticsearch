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
    private final boolean isSubSequence;
    private final int offset; // if this is a subSequence, the start of the subSequence
    private final CharSequence original;
    private final Pattern pattern;
    private final int limitFactor;

    private static final int SNIPPET_LIMIT = 64;

    private LimitedCharSequence(CharSequence wrap, Pattern pattern, int limitFactor) {
        this.wrapped = wrap;
        this.counter = new Counter(limitFactor * wrapped.length());

        this.isSubSequence = false;
        this.offset = 0;
        this.original = wrap;
        this.pattern = pattern;
        this.limitFactor = limitFactor;
    }

    // subSequence constructor
    private LimitedCharSequence(LimitedCharSequence superSequence, int start, int end) {
        this.wrapped = superSequence.wrapped.subSequence(start, end);
        this.counter = superSequence.counter;

        this.isSubSequence = true;
        this.offset = superSequence.offset + start;
        this.original = superSequence.original;
        this.pattern = superSequence.pattern;
        this.limitFactor = superSequence.limitFactor;
    }

    public static CharSequence limitedCharSequence(CharSequence wrapped, Pattern pattern, int limitFactor) {
        if (limitFactor <= 0) {
            throw new IllegalArgumentException("limitFactor must be positive");
        }
        if (wrapped instanceof LimitedCharSequence) {
            return wrapped;
        }
        return new LimitedCharSequence(wrapped, pattern, limitFactor);
    }

    public String details(int index) {
        // TODO(stu): pattern may be null
        return "pattern: [" + pattern.pattern() + "], " +
            "limit factor: [" + limitFactor + "], " +
            "char limit: [" + counter.charAtLimit + "], " +
            // TODO(stu): add ... for long fields
            // "snippet: [" + sequenceSnippet(index) + "], " +
            "count: [" + counter.count + "], " +
            "isSubSequence: [" + isSubSequence + "], " +
            "offset: [" + offset + "], " +
            "wrapped: [" + wrapped.toString() + "]";
    }

    @Override
    public int length() {
        return wrapped.length();
    }

    @Override
    public char charAt(int index) {
        counter.count++;
        if (counter.hitLimit()) {
            throw new CircuitBreakingException("[scripting] Regular expression considered too many characters, " + details(index) +
            ", this limit can be changed by changed by the [" + CompilerSettings.REGEX_LIMIT_FACTOR.getKey() + "] setting",
                CircuitBreaker.Durability.TRANSIENT);
        }
        return wrapped.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return new LimitedCharSequence(this, start, end);
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
