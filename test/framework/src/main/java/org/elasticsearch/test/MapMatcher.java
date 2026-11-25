/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.hamcrest.StringDescription;
import org.hamcrest.TypeSafeMatcher;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Matcher for {@link Map Maps} that reports all errors at once.
 */
public class MapMatcher extends TypeSafeMatcher<Map<?, ?>> {
    private static final int INDENT = 2;

    /**
     * Create a {@linkplain MapMatcher} that matches empty {@link Map}s.
     */
    public static MapMatcher matchesMap() {
        return new MapMatcher(emptyMap(), emptyMap(), false);
    }

    /**
     * Create a {@linkplain MapMatcher} that matches a {@link Map}.
     * <p>The description and mismatch message are sorted as {@link Map#entrySet}
     * because error messages with a consistent order are easier to debug.
     * So you should care about this order and provide {@link LinkedHashMap} or a
     * {@link TreeMap} or some other {@link Map} that has a nice order. Or build
     * an empty matcher with {@link #matchesMap()} and fill it in the order you
     * like by calling {@link #entry entry}.
     */
    public static MapMatcher matchesMap(Map<?, ?> map) {
        MapMatcher matcher = matchesMap();
        for (Map.Entry<?, ?> e : map.entrySet()) {
            matcher = matcher.entry(e.getKey(), e.getValue());
        }
        return matcher;
    }

    /**
     * Assert match. Shorter output on failure than
     * {@link MatcherAssert#assertThat(Object, Matcher)} that looks better for
     * {@link MapMatcher} and {@link ListMatcher}.
     */
    public static <T> void assertMap(T actual, Matcher<? super T> matcher) {
        assertMap("", actual, matcher);
    }

    /**
     * Assert match. Shorter output on failure than
     * {@link MatcherAssert#assertThat(Object, Matcher)} that looks better for
     * {@link MapMatcher} and {@link ListMatcher}.
     */
    public static <T> void assertMap(String reason, T actual, Matcher<? super T> matcher) {
        if (matcher.matches(actual)) {
            return;
        }

        Description description = new StringDescription();
        description.appendText(reason).appendText("Expected ");
        matcher.describeMismatch(actual, description);

        throw new AssertionError(description.toString());
    }

    private final Map<Object, Matcher<?>> matchers;
    private final Map<Object, Matcher<?>> optionalMatchers;
    private final boolean extraOk;

    private MapMatcher(Map<Object, Matcher<?>> matchers, Map<Object, Matcher<?>> optionalMatchers, boolean extraOk) {
        this.matchers = matchers;
        this.optionalMatchers = optionalMatchers;
        this.extraOk = extraOk;
    }

    /**
     * Ignore extra entries.
     *
     * @return a new {@link MapMatcher} that will not fail if it encounters extra entries
     */
    public MapMatcher extraOk() {
        return new MapMatcher(matchers, optionalMatchers, true);
    }

    /**
     * Expect a value.
     * <p>
     * Passing a {@link Matcher} to this method will function as though you
     * passed it directly to {@link #entry(Object, Matcher)}.
     *
     * @return a new {@link MapMatcher} that expects another entry
     */
    public MapMatcher entry(Object key, Object value) {
        return entry(key, matcherFor(value));
    }

    /**
     * Expect a {@link Matcher}.
     *
     * @return a new {@link MapMatcher} that expects another entry
     */
    public MapMatcher entry(Object key, Matcher<?> valueMatcher) {
        Map<Object, Matcher<?>> matchers = new LinkedHashMap<>(this.matchers);
        matchers.put(key, validated(key, valueMatcher));
        return new MapMatcher(matchers, optionalMatchers, extraOk);
    }

    /**
     * Expect a value if the key is present. If the key is absent, the match succeeds.
     * <p>
     * Passing a {@link Matcher} to this method will function as though you
     * passed it directly to {@link #optionalEntry(Object, Matcher)}.
     *
     * @return a new {@link MapMatcher} that expects another entry
     */
    public MapMatcher optionalEntry(Object key, Object value) {
        return optionalEntry(key, matcherFor(value));
    }

    /**
     * Expect a {@link Matcher} if the key is present. If the key is absent, the match succeeds.
     *
     * @return a new {@link MapMatcher} that expects another entry
     */
    public MapMatcher optionalEntry(Object key, Matcher<?> valueMatcher) {
        Map<Object, Matcher<?>> optionalMatchers = new LinkedHashMap<>(this.optionalMatchers);
        optionalMatchers.put(key, validated(key, valueMatcher));
        return new MapMatcher(matchers, optionalMatchers, extraOk);
    }

    private Matcher<?> validated(Object key, Matcher<?> valueMatcher) {
        if (matchers.containsKey(key)) {
            throw new IllegalArgumentException("Already had an entry for [" + key + "]: " + matchers.get(key));
        }
        if (optionalMatchers.containsKey(key)) {
            throw new IllegalArgumentException("Already had an optional entry for [" + key + "]: " + optionalMatchers.get(key));
        }
        return valueMatcher != null ? valueMatcher : nullValue();
    }

    /**
     * {@inheritDoc}
     *
     * @hidden
     */
    @Override
    public void describeTo(Description description) {
        describeTo(keyWidth(emptyMap()), description);
    }

    int keyWidth(Map<?, ?> item) {
        int max = 0;
        for (Object k : item.keySet()) {
            max = Math.max(max, k.toString().length());
        }
        for (Map.Entry<Object, Matcher<?>> e : matchers.entrySet()) {
            max = Math.max(max, e.getKey().toString().length());
            max = Math.max(max, maxKeyWidthForMatcher(item.get(e.getKey()), e.getValue()));
        }
        for (Map.Entry<Object, Matcher<?>> e : optionalMatchers.entrySet()) {
            max = Math.max(max, e.getKey().toString().length());
            max = Math.max(max, maxKeyWidthForMatcher(item.get(e.getKey()), e.getValue()));
        }
        return max;
    }

    static int maxKeyWidthForMatcher(Object item, Matcher<?> matcher) {
        if (matcher instanceof MapMatcher) {
            Map<?, ?> longestSubMap = item instanceof Map ? (Map<?, ?>) item : emptyMap();
            return ((MapMatcher) matcher).keyWidth(longestSubMap) - INDENT;
        }
        if (matcher instanceof ListMatcher) {
            List<?> longestSubList = item instanceof List ? (List<?>) item : emptyList();
            return ((ListMatcher) matcher).keyWidth(longestSubList) - INDENT;
        }
        return 0;
    }

    void describeTo(int keyWidth, Description description) {
        boolean empty = matchers.isEmpty() && optionalMatchers.isEmpty();
        description.appendText(empty ? "an empty map" : "a map containing");
        for (Map.Entry<?, Matcher<?>> e : matchers.entrySet()) {
            describeMatcher(keyWidth, e.getKey(), e.getValue(), description);
        }
        for (Map.Entry<?, Matcher<?>> e : optionalMatchers.entrySet()) {
            describeMatcher(keyWidth, e.getKey(), e.getValue(), description);
        }
    }

    static void describeMatcher(int keyWidth, Object key, Matcher<?> matcher, Description description) {
        String keyFormat = "\n%" + keyWidth + "s";
        description.appendText(String.format(Locale.ROOT, keyFormat, key)).appendText(": ");
        if (matcher instanceof MapMatcher) {
            ((MapMatcher) matcher).describeTo(keyWidth + INDENT, description);
            return;
        }
        if (matcher instanceof ListMatcher) {
            ((ListMatcher) matcher).describeTo(keyWidth + INDENT, description);
            return;
        }
        description.appendDescriptionOf(matcher);
    }

    @Override
    protected boolean matchesSafely(Map<?, ?> item) {
        for (Map.Entry<Object, Matcher<?>> matcher : matchers.entrySet()) {
            if (!item.containsKey(matcher.getKey()) || !matcher.getValue().matches(item.get(matcher.getKey()))) {
                return false;
            }
        }
        for (Map.Entry<Object, Matcher<?>> optionalMatcher : optionalMatchers.entrySet()) {
            if (item.containsKey(optionalMatcher.getKey()) && !optionalMatcher.getValue().matches(item.get(optionalMatcher.getKey()))) {
                return false;
            }
        }
        if (!extraOk) {
            for (Object key : item.keySet()) {
                if (!matchers.containsKey(key) && !optionalMatchers.containsKey(key)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    protected void describeMismatchSafely(Map<?, ?> item, Description description) {
        describePotentialMismatch(keyWidth(item), item, description);
    }

    void describePotentialMismatch(int keyWidth, Map<?, ?> item, Description description) {
        boolean empty = matchers.isEmpty() && optionalMatchers.isEmpty();
        description.appendText(empty ? "an empty map" : "a map containing");
        int maxKeyWidth = Stream.concat(
            Stream.concat(matchers.keySet().stream(), optionalMatchers.keySet().stream()),
            item.keySet().stream()
        ).mapToInt(k -> k.toString().length()).max().orElse(keyWidth);
        String keyFormat = "%" + maxKeyWidth + "s";

        for (Map.Entry<Object, Matcher<?>> matcher : matchers.entrySet()) {
            Object key = matcher.getKey();
            describeEntry(keyWidth, String.format(Locale.ROOT, keyFormat, matcher.getKey()), description);
            if (false == item.containsKey(key)) {
                describeEntryMissing(matcher.getValue(), description);
                continue;
            }
            describeEntryValue(keyWidth, matcher.getValue(), item.get(key), description);
        }
        for (Map.Entry<Object, Matcher<?>> matcher : optionalMatchers.entrySet()) {
            Object key = matcher.getKey();
            if (false == item.containsKey(key)) {
                continue;
            }
            describeEntry(keyWidth, String.format(Locale.ROOT, keyFormat, matcher.getKey()), description);
            describeEntryValue(keyWidth, matcher.getValue(), item.get(key), description);
        }
        for (Map.Entry<?, ?> e : item.entrySet()) {
            if (false == matchers.containsKey(e.getKey()) && false == optionalMatchers.containsKey(e.getKey())) {
                describeEntry(keyWidth, String.format(Locale.ROOT, keyFormat, e.getKey()), description);
                if (extraOk) {
                    describeEntryUnexepectedButOk(e.getValue(), description);
                } else {
                    describeEntryUnexepected(e.getValue(), description);
                }
            }
        }
    }

    /**
     * Converts an unknown {@link Object} to an equality {@link Matcher}
     * for the public API methods that take {@linkplain Object}.
     */
    static Matcher<?> matcherFor(Object value) {
        if (value == null) {
            return nullValue();
        }
        if (value instanceof List) {
            return ListMatcher.matchesList((List<?>) value);
        }
        if (value instanceof Map) {
            return matchesMap((Map<?, ?>) value);
        }
        if (value instanceof Matcher) {
            return (Matcher<?>) value;
        }
        return equalTo(value);
    }

    static void describeEntry(int keyWidth, Object key, Description description) {
        String keyFormat = "\n%" + keyWidth + "s";
        description.appendText(String.format(Locale.ROOT, keyFormat, key)).appendText(": ");
    }

    static void describeEntryMissing(Matcher<?> matcher, Description description) {
        description.appendText("expected ");
        /*
         * Use a short description for multi-line matchers so the "but was <missing>"
         * bit of the erro is more prominent. It's the more important part.
         */
        if (matcher instanceof MapMatcher) {
            description.appendText("a map");
        } else if (matcher instanceof ListMatcher) {
            description.appendText("a list");
        } else {
            description.appendDescriptionOf(matcher);
        }
        description.appendText(" but was <missing>");
    }

    static void describeEntryUnexepected(Object value, Description description) {
        description.appendText("<unexpected> but was ");
        description.appendValue(value);
    }

    static void describeEntryUnexepectedButOk(Object value, Description description) {
        description.appendValue(value);
        description.appendText(" unexpected but ok");
    }

    static void describeEntryValue(int keyWidth, Matcher<?> matcher, Object v, Description description) {
        if (v instanceof Map && matcher instanceof MapMatcher mm) {
            mm.describePotentialMismatch(keyWidth + INDENT, (Map<?, ?>) v, description);
            return;
        }
        if (v instanceof List && matcher instanceof ListMatcher lm) {
            lm.describePotentialMismatch(keyWidth + INDENT, (List<?>) v, description);
            return;
        }
        if (false == matcher.matches(v)) {
            try {
                description.appendText("expected ").appendDescriptionOf(matcher).appendText(" but ");
                matcher.describeMismatch(v, description);
            } catch (Exception e) {
                description.appendText("error describing ");
                StringWriter trace = new StringWriter();
                e.printStackTrace(new PrintWriter(trace));
                description.appendValue(trace);
            }
            return;
        }
        description.appendValue(v);
    }
}
