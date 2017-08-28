/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.joda.time.DateTimeFieldType;
import org.joda.time.ReadableDateTime;

/**
 * Extracts portions of {@link ReadableDateTime}s. Note that the position in the enum is used for serialization.
 */
public enum DateTimeExtractor {
    DAY_OF_MONTH(DateTimeFieldType.dayOfMonth()),
    DAY_OF_WEEK(DateTimeFieldType.dayOfWeek()),
    DAY_OF_YEAR(DateTimeFieldType.dayOfYear()),
    HOUR_OF_DAY(DateTimeFieldType.hourOfDay()),
    MINUTE_OF_DAY(DateTimeFieldType.minuteOfDay()),
    MINUTE_OF_HOUR(DateTimeFieldType.minuteOfHour()),
    MONTH_OF_YEAR(DateTimeFieldType.monthOfYear()),
    SECOND_OF_MINUTE(DateTimeFieldType.secondOfMinute()),
    WEEK_OF_YEAR(DateTimeFieldType.weekOfWeekyear()),
    YEAR(DateTimeFieldType.year());

    private final DateTimeFieldType field;

    DateTimeExtractor(DateTimeFieldType field) {
        this.field = field;
    }

    public int extract(ReadableDateTime dt) {
        return dt.get(field);
    }
}
