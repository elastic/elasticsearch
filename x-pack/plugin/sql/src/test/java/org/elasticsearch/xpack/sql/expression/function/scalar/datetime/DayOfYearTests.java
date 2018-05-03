/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Locale;
import java.util.TimeZone;

public class DayOfYearTests extends DateTimeFunctionTestcase<DayOfYear> {

    public void testUTC() {
        processAndCheck(dateTime(0), UTC, 1);
    }

    public void testGMT_plus0100() {
        processAndCheck(dateTime(0), "GMT+01:00", 1);
    }

    public void testGMT_minus0100() {
        processAndCheck(dateTime(0), "GMT-01:00", 365);
    }

    @Override
    DayOfYear build(Object value, TimeZone timeZone) {
        return new DayOfYear(null, new Literal(null, value, DataType.DATE), timeZone);
    }

    Locale defaultLocale() {
        return null;
    }
}
