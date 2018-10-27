/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.TimeZone;

public class QuarterProcessorTests extends ESTestCase {

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    public void testQuarterWithUTCTimezone() {
        QuarterProcessor proc = new QuarterProcessor(UTC);
        
        assertEquals(1, proc.process(new DateTime(0L, DateTimeZone.UTC)));
        assertEquals(4, proc.process(new DateTime(-5400, 12, 25, 10, 10, DateTimeZone.UTC)));
        assertEquals(1, proc.process(new DateTime(30, 2, 1, 12, 13, DateTimeZone.UTC)));
        assertEquals(3, proc.process(new DateTime(10902, 8, 22, 11, 11, DateTimeZone.UTC)));
        
        assertEquals(1, proc.process("0"));
        assertEquals(3, proc.process("-64164233612338"));
        assertEquals(2, proc.process("64164233612338"));
    }
    
    public void testValidDayNamesWithNonUTCTimeZone() {
        QuarterProcessor proc = new QuarterProcessor(TimeZone.getTimeZone("GMT-10:00"));
        assertEquals(4, proc.process(new DateTime(0L, DateTimeZone.UTC)));
        assertEquals(4, proc.process(new DateTime(-5400, 1, 1, 5, 0, DateTimeZone.UTC)));
        assertEquals(1, proc.process(new DateTime(30, 4, 1, 9, 59, DateTimeZone.UTC)));
        
        proc = new QuarterProcessor(TimeZone.getTimeZone("GMT+10:00"));
        assertEquals(4, proc.process(new DateTime(10902, 9, 30, 14, 1, DateTimeZone.UTC)));
        assertEquals(3, proc.process(new DateTime(10902, 9, 30, 13, 59, DateTimeZone.UTC)));
        
        assertEquals(1, proc.process("0"));
        assertEquals(3, proc.process("-64164233612338"));
        assertEquals(2, proc.process("64164233612338"));
    }
}
