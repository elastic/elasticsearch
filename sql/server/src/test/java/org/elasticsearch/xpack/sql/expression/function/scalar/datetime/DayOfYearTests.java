/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.type.DateType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class DayOfYearTests extends ESTestCase {
    private static final DateTimeZone UTC = DateTimeZone.UTC;

    public void testAsColumnProcessor() {
        assertEquals(1, extract(dateTime(0), UTC));
        assertEquals(1, extract(dateTime(0), DateTimeZone.forID("GMT+1")));
        assertEquals(365, extract(dateTime(0), DateTimeZone.forID("GMT-1")));
    }

    private DateTime dateTime(long millisSinceEpoch) {
        return new DateTime(millisSinceEpoch, UTC);
    }

    private Object extract(Object value, DateTimeZone timeZone) {
        return build(value, timeZone).asProcessor().apply(value);
    }

    private DayOfYear build(Object value, DateTimeZone timeZone) {
        return new DayOfYear(null, new Literal(null, value, DateType.DEFAULT), timeZone);
    }
}
