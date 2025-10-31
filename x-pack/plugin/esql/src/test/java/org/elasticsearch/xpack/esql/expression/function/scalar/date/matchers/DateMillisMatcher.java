/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date.matchers;

import org.hamcrest.BaseMatcher;

import java.time.Instant;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;

/**
 * Test matcher for ESQL datetimes that expects longs, but describes the errors as dates, for better readability.
 * <p>
 *     See {@link DateNanosMatcher} for the date nanos counterpart.
 * </p>
 */
public class DateMillisMatcher extends BaseMatcher<Long> {
    private final long timeMillis;

    public DateMillisMatcher(String date) {
        this.timeMillis = Instant.parse(date).toEpochMilli();
    }

    @Override
    public boolean matches(Object item) {
        return item instanceof Long && timeMillis == (Long) item;
    }

    @Override
    public void describeMismatch(Object item, org.hamcrest.Description description) {
        description.appendText("was ");
        if (item instanceof Long l) {
            description.appendValue(DEFAULT_DATE_TIME_FORMATTER.formatMillis(l));
        } else {
            description.appendValue(item);
        }
    }

    @Override
    public void describeTo(org.hamcrest.Description description) {
        description.appendText(DEFAULT_DATE_TIME_FORMATTER.formatMillis(timeMillis));
    }
}
