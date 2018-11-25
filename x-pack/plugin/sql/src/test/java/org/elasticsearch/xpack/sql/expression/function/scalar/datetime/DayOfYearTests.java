/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.TimeZone;

import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.dateTime;

public class DayOfYearTests extends ESTestCase {
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    public void testAsColumnProcessor() {
        assertEquals(1, extract(dateTime(0), UTC));
        assertEquals(1, extract(dateTime(0), TimeZone.getTimeZone("GMT+01:00")));
        assertEquals(365, extract(dateTime(0), TimeZone.getTimeZone("GMT-01:00")));
    }

    private Object extract(Object value, TimeZone timeZone) {
        return build(value, timeZone).asPipe().asProcessor().process(value);
    }

    private DayOfYear build(Object value, TimeZone timeZone) {
        return new DayOfYear(null, new Literal(null, value, DataType.DATE), timeZone);
    }
}
