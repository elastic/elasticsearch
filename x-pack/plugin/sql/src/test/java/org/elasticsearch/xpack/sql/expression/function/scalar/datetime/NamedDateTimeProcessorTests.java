/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.NamedDateTimeProcessor.NameExtractor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.TimeZone;

public class NamedDateTimeProcessorTests extends AbstractWireSerializingTestCase<NamedDateTimeProcessor> {
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

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
    protected NamedDateTimeProcessor mutateInstance(NamedDateTimeProcessor instance) throws IOException {
        NameExtractor replaced = randomValueOtherThan(instance.extractor(), () -> randomFrom(NameExtractor.values()));
        return new NamedDateTimeProcessor(replaced, UTC);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/33621")
    public void testValidDayNamesInUTC() {
        NamedDateTimeProcessor proc = new NamedDateTimeProcessor(NameExtractor.DAY_NAME, UTC);
        assertEquals("Thursday", proc.process("0"));
        assertEquals("Saturday", proc.process("-64164233612338"));
        assertEquals("Monday", proc.process("64164233612338"));

        assertEquals("Thursday", proc.process(new DateTime(0L, DateTimeZone.UTC)));
        assertEquals("Thursday", proc.process(new DateTime(-5400, 12, 25, 2, 0, DateTimeZone.UTC)));
        assertEquals("Friday", proc.process(new DateTime(30, 2, 1, 12, 13, DateTimeZone.UTC)));
        assertEquals("Tuesday", proc.process(new DateTime(10902, 8, 22, 11, 11, DateTimeZone.UTC)));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/33621")
    public void testValidDayNamesWithNonUTCTimeZone() {
        NamedDateTimeProcessor proc = new NamedDateTimeProcessor(NameExtractor.DAY_NAME, TimeZone.getTimeZone("GMT-10:00"));
        assertEquals("Wednesday", proc.process("0"));
        assertEquals("Friday", proc.process("-64164233612338"));
        assertEquals("Monday", proc.process("64164233612338"));

        assertEquals("Wednesday", proc.process(new DateTime(0L, DateTimeZone.UTC)));
        assertEquals("Wednesday", proc.process(new DateTime(-5400, 12, 25, 2, 0, DateTimeZone.UTC)));
        assertEquals("Friday", proc.process(new DateTime(30, 2, 1, 12, 13, DateTimeZone.UTC)));
        assertEquals("Tuesday", proc.process(new DateTime(10902, 8, 22, 11, 11, DateTimeZone.UTC)));
        assertEquals("Monday", proc.process(new DateTime(10902, 8, 22, 9, 59, DateTimeZone.UTC)));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/33621")
    public void testValidMonthNamesInUTC() {
        NamedDateTimeProcessor proc = new NamedDateTimeProcessor(NameExtractor.MONTH_NAME, UTC);
        assertEquals("January", proc.process("0"));
        assertEquals("September", proc.process("-64164233612338"));
        assertEquals("April", proc.process("64164233612338"));

        assertEquals("January", proc.process(new DateTime(0L, DateTimeZone.UTC)));
        assertEquals("December", proc.process(new DateTime(-5400, 12, 25, 10, 10, DateTimeZone.UTC)));
        assertEquals("February", proc.process(new DateTime(30, 2, 1, 12, 13, DateTimeZone.UTC)));
        assertEquals("August", proc.process(new DateTime(10902, 8, 22, 11, 11, DateTimeZone.UTC)));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/33621")
    public void testValidMonthNamesWithNonUTCTimeZone() {
        NamedDateTimeProcessor proc = new NamedDateTimeProcessor(NameExtractor.MONTH_NAME, TimeZone.getTimeZone("GMT-3:00"));
        assertEquals("December", proc.process("0"));
        assertEquals("August", proc.process("-64165813612338")); // GMT: Tuesday, September 1, -0064 2:53:07.662 AM
        assertEquals("April", proc.process("64164233612338")); // GMT: Monday, April 14, 4003 2:13:32.338 PM

        assertEquals("December", proc.process(new DateTime(0L, DateTimeZone.UTC)));
        assertEquals("November", proc.process(new DateTime(-5400, 12, 1, 1, 1, DateTimeZone.UTC)));
        assertEquals("February", proc.process(new DateTime(30, 2, 1, 12, 13, DateTimeZone.UTC)));
        assertEquals("July", proc.process(new DateTime(10902, 8, 1, 2, 59, DateTimeZone.UTC)));
        assertEquals("August", proc.process(new DateTime(10902, 8, 1, 3, 00, DateTimeZone.UTC)));
    }
}
