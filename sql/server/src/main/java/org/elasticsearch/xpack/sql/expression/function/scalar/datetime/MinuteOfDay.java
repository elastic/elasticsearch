/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadableDateTime;

import java.time.temporal.ChronoField;

public class MinuteOfDay extends DateTimeFunction {

    public MinuteOfDay(Location location, Expression argument, DateTimeZone timeZone) {
        super(location, argument, timeZone);
    }

    @Override
    public String dateTimeFormat() {
        throw new UnsupportedOperationException("is there a format for it?");
    }

    @Override
    public String interval() {
        return "minute";
    }

    @Override
    protected int extract(ReadableDateTime dt) {
        return dt.getMinuteOfDay();
    }

    @Override
    protected ChronoField chronoField() {
        return ChronoField.MINUTE_OF_DAY;
    }
}
