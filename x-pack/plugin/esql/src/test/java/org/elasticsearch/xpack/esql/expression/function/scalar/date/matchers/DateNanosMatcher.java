/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date.matchers;

import org.elasticsearch.common.time.DateUtils;
import org.hamcrest.BaseMatcher;

import java.time.Instant;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;

/**
 * Test matcher for ESQL date nanos that expects longs, but describes the errors as dates, for better readability.
 * <p>
 *     See {@link DateMillisMatcher} for the datetime (millis) counterpart.
 * </p>
 */
public class DateNanosMatcher extends BaseMatcher<Long> {
    private final long timeNanos;

    public DateNanosMatcher(String date) {
        this.timeNanos = DateUtils.toLong(Instant.parse(date));
    }

    @Override
    public boolean matches(Object item) {
        return item instanceof Long && timeNanos == (Long) item;
    }

    @Override
    public void describeMismatch(Object item, org.hamcrest.Description description) {
        description.appendText("was ");
        if (item instanceof Long l) {
            description.appendValue(DEFAULT_DATE_TIME_FORMATTER.formatNanos(l));
        } else {
            description.appendValue(item);
        }
    }

    @Override
    public void describeTo(org.hamcrest.Description description) {
        description.appendText(DEFAULT_DATE_TIME_FORMATTER.formatNanos(timeNanos));
    }
}
