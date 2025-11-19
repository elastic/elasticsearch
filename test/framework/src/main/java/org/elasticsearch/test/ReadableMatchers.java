/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateUtils;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.time.Instant;

public class ReadableMatchers {
    private static final DateFormatter dateFormatter = DateFormatter.forPattern("strict_date_optional_time");

    /**
     * Test matcher for millis dates that expects longs, but describes the errors as dates, for better readability.
     * <p>
     *     See {@link #matchesDateNanos} for the nanos counterpart.
     * </p>
     */
    public static DateMillisMatcher matchesDateMillis(String date) {
        return new DateMillisMatcher(date);
    }

    /**
     * Test matcher for nanos dates that expects longs, but describes the errors as dates, for better readability.
     * <p>
     *     See {@link DateMillisMatcher} for the millis counterpart.
     * </p>
     */
    public static DateNanosMatcher matchesDateNanos(String date) {
        return new DateNanosMatcher(date);
    }

    /**
     * Test matcher for BytesRef that expects BytesRefs, but describes the errors as strings, for better readability.
     */
    public static StringBytesRefMatcher matchesBytesRef(String string) {
        return new StringBytesRefMatcher(string);
    }

    public static class DateMillisMatcher extends TypeSafeMatcher<Long> {
        private final long timeMillis;

        public DateMillisMatcher(String date) {
            this.timeMillis = Instant.parse(date).toEpochMilli();
        }

        @Override
        public boolean matchesSafely(Long item) {
            return timeMillis == item;
        }

        @Override
        public void describeMismatchSafely(Long item, Description description) {
            description.appendText("was ").appendValue(dateFormatter.formatMillis(item));
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(dateFormatter.formatMillis(timeMillis));
        }
    }

    public static class DateNanosMatcher extends TypeSafeMatcher<Long> {
        private final long timeNanos;

        public DateNanosMatcher(String date) {
            this.timeNanos = DateUtils.toLong(Instant.parse(date));
        }

        @Override
        public boolean matchesSafely(Long item) {
            return timeNanos == item;
        }

        @Override
        public void describeMismatchSafely(Long item, Description description) {
            description.appendText("was ").appendValue(dateFormatter.formatNanos(item));
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(dateFormatter.formatNanos(timeNanos));
        }
    }

    public static class StringBytesRefMatcher extends TypeSafeMatcher<BytesRef> {
        private final String string;
        private final BytesRef bytesRef;

        public StringBytesRefMatcher(String string) {
            this.string = string;
            this.bytesRef = new BytesRef(string);
        }

        @Override
        protected boolean matchesSafely(BytesRef item) {
            return item.equals(bytesRef);
        }

        @Override
        public void describeMismatchSafely(BytesRef item, Description description) {
            description.appendText("was ").appendValue(item.utf8ToString());
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(string);
        }
    }

}
