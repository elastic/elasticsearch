/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.type.DataType;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;

import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.TimeZone;

public class DayNameTests extends DateTimeFunctionTestcase<DayName> {

    public void test19700101_Thursday() {
        processAndCheck(dateTime(0), UTC, "Thursday");
    }

    public void test20180503_GMT_Plus100_Thursday() {
        processAndCheck(dateTime(2018,5,3), "GMT+01:00", "Thursday");
    }

    public void test20180502_GMTMinus100_Friday() {
        processAndCheck(dateTime(2018,5,4), "GMT-01:00", "Friday");
    }

    @Override
    DayName build(Object value, TimeZone timeZone) {
        return new DayName(null, new Literal(null, value, DataType.DATE), timeZone);
    }

    // test expectations assume English
    Locale defaultLocale() {
        return Locale.ENGLISH;
    }
}
