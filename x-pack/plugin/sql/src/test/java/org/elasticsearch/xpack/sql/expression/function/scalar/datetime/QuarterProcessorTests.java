/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.test.ESTestCase;

import java.time.ZoneId;

import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.dateTime;
import static org.elasticsearch.xpack.sql.util.DateUtils.UTC;

public class QuarterProcessorTests extends ESTestCase {

    public void testQuarterWithUTCTimezone() {
        QuarterProcessor proc = new QuarterProcessor(UTC);

        assertEquals(1, proc.process(dateTime(0L)));
        assertEquals(4, proc.process(dateTime(-5400, 12, 25, 10, 10)));
        assertEquals(1, proc.process(dateTime(30, 2, 1, 12, 13)));
        assertEquals(3, proc.process(dateTime(10902, 8, 22, 11, 11)));

        assertEquals(1, proc.process(dateTime(0L)));
        assertEquals(3, proc.process(dateTime(-64164233612338L)));
        assertEquals(2, proc.process(dateTime(64164233612338L)));
    }

    public void testValidDayNamesWithNonUTCTimeZone() {
        QuarterProcessor proc = new QuarterProcessor(ZoneId.of("GMT-10:00"));
        assertEquals(4, proc.process(dateTime(0L)));
        assertEquals(4, proc.process(dateTime(-5400, 1, 1, 5, 0)));
        assertEquals(1, proc.process(dateTime(30, 4, 1, 9, 59)));

        proc = new QuarterProcessor(ZoneId.of("GMT+10:00"));
        assertEquals(4, proc.process(dateTime(10902, 9, 30, 14, 1)));
        assertEquals(3, proc.process(dateTime(10902, 9, 30, 13, 59)));

        assertEquals(1, proc.process(dateTime(0L)));
        assertEquals(3, proc.process(dateTime(-64164233612338L)));
        assertEquals(2, proc.process(dateTime(64164233612338L)));
    }
}
