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

public class Year extends DateTimeFunction {

    public Year(Location location, Expression argument, DateTimeZone timeZone) {
        super(location, argument, timeZone);
    }

    @Override
    public String dateTimeFormat() {
        return "year";
    }

    @Override
    public String interval() {
        return "year";
    }

    @Override
    protected int extract(ReadableDateTime dt) {
        return dt.getYear();
    }

    @Override
    public Expression orderBy() {
        return argument();
    }

    @Override
    protected ChronoField chronoField() {
        return ChronoField.YEAR;
    }
}
