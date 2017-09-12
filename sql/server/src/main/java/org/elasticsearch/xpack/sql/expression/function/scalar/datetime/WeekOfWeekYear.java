/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;
import org.elasticsearch.xpack.sql.tree.Location;
import org.joda.time.DateTimeZone;

import java.time.temporal.ChronoField;

public class WeekOfWeekYear extends DateTimeFunction {
    public WeekOfWeekYear(Location location, Expression field, DateTimeZone timeZone) {
        super(location, field, timeZone);
    }

    @Override
    public String dateTimeFormat() {
        return "w";
    }

    @Override
    public String interval() {
        return "week";
    }

    @Override
    protected ChronoField chronoField() {
        return ChronoField.ALIGNED_WEEK_OF_YEAR; // NOCOMMIT is this right?
    }

    @Override
    protected DateTimeExtractor extractor() {
        return DateTimeExtractor.WEEK_OF_YEAR;
    }
}
