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

import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class LimitedPattern {
    protected final String regex;
    protected final Pattern pattern;
    protected final int flags;
    protected final int limit;

    public LimitedPattern(String regex, int flags, int limit) {
        this.regex = regex;
        this.pattern = Pattern.compile(regex, flags);
        this.flags = flags;
        this.limit = limit;
    }

    public static LimitedPattern compile(String regex, int flags, int limit) {
        return new LimitedPattern(regex, flags, limit);
    }

    public Predicate<String> asPredicate() {
        return s -> matcher​(s).find();
    }

    public int flags() {
        return flags;
    }

    public Matcher matcher​(CharSequence input) {
        return pattern.matcher(new LimitedCharSequence(input, pattern, limit));
    }

    public String pattern() {
        return pattern.pattern();
    }

    public String[] split​(CharSequence input) {
        return pattern.split(new LimitedCharSequence(input, pattern, limit));
    }

    public String[] split​(CharSequence input, int limit) {
        return pattern.split(new LimitedCharSequence(input, pattern, this.limit), limit);
    }

    public Stream<String> splitAsStream​(CharSequence input)  {
        return pattern.splitAsStream(new LimitedCharSequence(input, pattern, limit));
    }

    public String toString() {
        return pattern.toString();
    }

    /**
     * CharSequence that wraps another sequence and limits the number of times charAt can be
     */
    public static class LimitedCharSequence implements CharSequence {
        private final LimitedCharSequence parent;
        private final CharSequence current;
        private final Pattern pattern;
        private final Counter counter;
        private static final int UNLIMITED = -1;

        public LimitedCharSequence(CharSequence wrap, Pattern pattern) {
            this(wrap, pattern, UNLIMITED);
        }

        public LimitedCharSequence(CharSequence wrap, Pattern pattern, int limit) {
            if (wrap instanceof LimitedCharSequence) {
                this.parent = (LimitedCharSequence) wrap;
                this.current = parent.current;
                this.pattern = parent.pattern;
                this.counter = parent.counter;
            } else {
                this.parent = this;
                this.current = wrap;
                this.pattern = pattern;
                this.counter = new Counter(limit);
            }
        }

        private LimitedCharSequence(LimitedCharSequence parent, int start, int end) {
            this.parent = parent;
            this.current = parent.current.subSequence(start, end);
            this.pattern = parent.pattern;
            this.counter = parent.counter;
        }

        @Override
        public int length() {
            return current.length();
        }

        @Override
        public char charAt(int index) {
            if (counter.limit == UNLIMITED) {
                return current.charAt(index);
            }
            counter.count++;
            if (counter.hitLimit()) {
                throw new RuntimeException(
                    "Pattern [" + pattern.pattern() + "] considered too many characters [" + counter.count + "] " +
                        "compared to limit [" + counter.limit + "] when fetching character [" + current.charAt(index) + "]" +
                        "at index [" + index + "] for character sequence [" + parent + "]"
                );
            }
            return current.charAt(index);
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return new LimitedCharSequence(this, start, end);
        }

        @Override
        public String toString() {
            return current.toString();
        }
    }

    // Counter object to keep track of charAts for original sequence and all subsequences
    private static class Counter {
        public final int limit;
        public int count;

        Counter(int limit) {
            this.limit = limit;
            this.count = 0;
        }

        boolean hitLimit() {
            return count >= limit;
        }
    }
}
