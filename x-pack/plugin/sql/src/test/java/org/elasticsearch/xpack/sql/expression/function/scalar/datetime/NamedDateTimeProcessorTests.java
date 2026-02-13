/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xpack.sql.AbstractSqlWireSerializingTestCase;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.NamedDateTimeProcessor.NameExtractor;

import java.time.ZoneId;

import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.dateTime;
import static org.elasticsearch.xpack.sql.util.DateUtils.UTC;

public class NamedDateTimeProcessorTests extends AbstractSqlWireSerializingTestCase<NamedDateTimeProcessor> {

    public static NamedDateTimeProcessor randomNamedDateTimeProcessor() {
        return new NamedDateTimeProcessor(randomFrom(NameExtractor.values()), UTC);
    }

    @Override
    protected NamedDateTimeProcessor createTestInstance() {
        return randomNamedDateTimeProcessor();
    }

    @Override
    protected Reader<NamedDateTimeProcessor> instanceReader() {
        return NamedDateTimeProcessor::new;
    }

    @Override
    protected NamedDateTimeProcessor mutateInstance(NamedDateTimeProcessor instance) {
        NameExtractor replaced = randomValueOtherThan(instance.extractor(), () -> randomFrom(NameExtractor.values()));
        return new NamedDateTimeProcessor(replaced, UTC);
    }

    @Override
    protected ZoneId instanceZoneId(NamedDateTimeProcessor instance) {
        return instance.zoneId();
    }

    public void testValidDayNamesInUTC() {
        NamedDateTimeProcessor proc = new NamedDateTimeProcessor(NameExtractor.DAY_NAME, UTC);
        assertEquals("Thursday", proc.process(dateTime(0L)));
        assertEquals("Saturday", proc.process(dateTime(-64164233612338L)));
        assertEquals("Monday", proc.process(dateTime(64164233612338L)));

        assertEquals("Thursday", proc.process(dateTime(0L)));
        assertEquals("Thursday", proc.process(dateTime(-5400, 12, 25, 2, 0)));
        assertEquals("Friday", proc.process(dateTime(30, 2, 1, 12, 13)));
        assertEquals("Tuesday", proc.process(dateTime(10902, 8, 22, 11, 11)));
    }

    public void testValidDayNamesWithNonUTCTimeZone() {
        NamedDateTimeProcessor proc = new NamedDateTimeProcessor(NameExtractor.DAY_NAME, ZoneId.of("GMT-10:00"));
        assertEquals("Wednesday", proc.process(dateTime(0)));
        assertEquals("Friday", proc.process(dateTime(-64164233612338L)));
        assertEquals("Monday", proc.process(dateTime(64164233612338L)));

        assertEquals("Wednesday", proc.process(dateTime(0L)));
        assertEquals("Wednesday", proc.process(dateTime(-5400, 12, 25, 2, 0)));
        assertEquals("Friday", proc.process(dateTime(30, 2, 1, 12, 13)));
        assertEquals("Tuesday", proc.process(dateTime(10902, 8, 22, 11, 11)));
        assertEquals("Monday", proc.process(dateTime(10902, 8, 22, 9, 59)));
    }

    public void testValidMonthNamesInUTC() {
        NamedDateTimeProcessor proc = new NamedDateTimeProcessor(NameExtractor.MONTH_NAME, UTC);
        assertEquals("January", proc.process(dateTime(0)));
        assertEquals("September", proc.process(dateTime(-64165813612338L)));
        assertEquals("April", proc.process(dateTime(64164233612338L)));

        assertEquals("January", proc.process(dateTime(0L)));
        assertEquals("December", proc.process(dateTime(-5400, 12, 25, 10, 10)));
        assertEquals("February", proc.process(dateTime(30, 2, 1, 12, 13)));
        assertEquals("August", proc.process(dateTime(10902, 8, 22, 11, 11)));
    }

    public void testValidMonthNamesWithNonUTCTimeZone() {
        NamedDateTimeProcessor proc = new NamedDateTimeProcessor(NameExtractor.MONTH_NAME, ZoneId.of("GMT-03:00"));
        assertEquals("December", proc.process(dateTime(0)));
        assertEquals("August", proc.process(dateTime(-64165813612338L))); // GMT: Tuesday, September 1, -0064 2:53:07.662 AM
        assertEquals("April", proc.process(dateTime(64164233612338L))); // GMT: Monday, April 14, 4003 2:13:32.338 PM

        assertEquals("December", proc.process(dateTime(0L)));
        assertEquals("November", proc.process(dateTime(-5400, 12, 1, 1, 1)));
        assertEquals("February", proc.process(dateTime(30, 2, 1, 12, 13)));
        assertEquals("July", proc.process(dateTime(10902, 8, 1, 2, 59)));
        assertEquals("August", proc.process(dateTime(10902, 8, 1, 3, 00)));
    }
}
