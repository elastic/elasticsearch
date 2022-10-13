/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import static java.util.Collections.emptyList;
import static org.elasticsearch.test.MapMatcher.describeEntry;
import static org.elasticsearch.test.MapMatcher.describeEntryMissing;
import static org.elasticsearch.test.MapMatcher.describeEntryUnexepected;
import static org.elasticsearch.test.MapMatcher.describeEntryValue;
import static org.elasticsearch.test.MapMatcher.describeMatcher;
import static org.elasticsearch.test.MapMatcher.matcherFor;
import static org.elasticsearch.test.MapMatcher.maxKeyWidthForMatcher;
import static org.hamcrest.Matchers.nullValue;

/**
 * Matcher for {@link List Lists} that reports all errors at once.
 */
public class ListMatcher extends TypeSafeMatcher<List<?>> {
    /**
     * Create a {@linkplain ListMatcher} that matches empty lists.
     */
    public static ListMatcher matchesList() {
        return new ListMatcher(emptyList());
    }

    /**
     * Create a {@linkplain ListMatcher} that matches a list.
     */
    public static ListMatcher matchesList(List<?> list) {
        ListMatcher matcher = matchesList();
        for (Object item : list) {
            matcher = matcher.item(item);
        }
        return matcher;
    }

    private final List<Matcher<?>> matchers;

    private ListMatcher(List<Matcher<?>> matchers) {
        this.matchers = matchers;
    }

    /**
     * Expect a value.
     * <p>
     * Passing a {@link Matcher} to this method will function as though you
     * passed it directly to {@link #item(Matcher)}.
     *
     * @return a new {@link ListMatcher} that expects all items this matcher
     *         expected followed by the provided item
     */
    public ListMatcher item(Object value) {
        return item(matcherFor(value));
    }

    /**
     * Expect a {@link Matcher}.
     *
     * @return a new {@link ListMatcher} that expects all items this matcher
     *         expected followed by the provided item
     */
    public ListMatcher item(Matcher<?> valueMatcher) {
        if (valueMatcher == null) {
            valueMatcher = nullValue();
        }
        List<Matcher<?>> matchers = new ArrayList<>(this.matchers);
        matchers.add(valueMatcher);
        return new ListMatcher(matchers);
    }

    /**
     * {@inheritDoc}
     *
     * @hidden
     */
    @Override
    public void describeTo(Description description) {
        describeTo(keyWidth(emptyList()), description);
    }

    int keyWidth(List<?> item) {
        int max = Integer.toString(matchers.size()).length();
        Iterator<?> value = item.iterator();
        Iterator<Matcher<?>> matcher = matchers.iterator();
        while (matcher.hasNext()) {
            max = Math.max(max, maxKeyWidthForMatcher(value.hasNext() ? value.next() : null, matcher.next()));
        }
        return max;
    }

    void describeTo(int keyWidth, Description description) {
        description.appendText(matchers.isEmpty() ? "an empty list" : "a list containing");
        int index = 0;
        for (Matcher<?> matcher : matchers) {
            describeMatcher(keyWidth, index++, matcher, description);
        }
    }

    @Override
    protected boolean matchesSafely(List<?> item) {
        if (item.size() != matchers.size()) {
            return false;
        }
        Iterator<?> value = item.iterator();
        Iterator<Matcher<?>> matcher = matchers.iterator();
        while (matcher.hasNext()) {
            if (false == matcher.next().matches(value.next())) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected void describeMismatchSafely(List<?> item, Description description) {
        describePotentialMismatch(keyWidth(item), item, description);
    }

    void describePotentialMismatch(int keyWidth, List<?> item, Description description) {
        description.appendText(matchers.isEmpty() ? "an empty list" : "a list containing");
        int maxKeyWidth = Integer.toString(Math.max(item.size(), matchers.size())).length();
        String keyFormat = "%" + maxKeyWidth + "s";

        Iterator<?> value = item.iterator();
        Iterator<Matcher<?>> matcher = matchers.iterator();
        int index = 0;
        while (matcher.hasNext()) {
            describeEntry(keyWidth, String.format(Locale.ROOT, keyFormat, index++), description);
            if (false == value.hasNext()) {
                describeEntryMissing(matcher.next(), description);
                continue;
            }
            describeEntryValue(keyWidth, matcher.next(), value.next(), description);
        }
        while (value.hasNext()) {
            describeEntry(keyWidth, String.format(Locale.ROOT, keyFormat, index++), description);
            describeEntryUnexepected(value.next(), description);
        }
    }
}
