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
        // because of https://github.com/elastic/elasticsearch/issues/33621#issuecomment-420617752
        // and the fact that -Djava.locale.providers=COMPAT option is not passed along when the jvm
        // is forked for a test, the tests below use startsWith instead of perfect equality.
        assertTrue(((String) proc.process("0")).startsWith("Thu"));
        assertTrue(((String) proc.process("-64164233612338")).startsWith("Sat"));
        assertTrue(((String) proc.process("64164233612338")).startsWith("Mon"));

        assertTrue(((String) proc.process(new DateTime(0L, DateTimeZone.UTC))).startsWith("Thu"));
        assertTrue(((String) proc.process(new DateTime(-5400, 12, 25, 2, 0, DateTimeZone.UTC))).startsWith("Thu"));
        assertTrue(((String) proc.process(new DateTime(30, 2, 1, 12, 13, DateTimeZone.UTC))).startsWith("Fri"));
        assertTrue(((String) proc.process(new DateTime(10902, 8, 22, 11, 11, DateTimeZone.UTC))).startsWith("Tue"));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/33621")
    public void testValidDayNamesWithNonUTCTimeZone() {
        NamedDateTimeProcessor proc = new NamedDateTimeProcessor(NameExtractor.DAY_NAME, TimeZone.getTimeZone("GMT-10:00"));
        assertTrue(((String) proc.process("0")).startsWith("Wed"));
        assertTrue(((String) proc.process("-64164233612338")).startsWith("Fri"));
        assertTrue(((String) proc.process("64164233612338")).startsWith("Mon"));

        assertTrue(((String) proc.process(new DateTime(0L, DateTimeZone.UTC))).startsWith("Wed"));
        assertTrue(((String) proc.process(new DateTime(-5400, 12, 25, 2, 0, DateTimeZone.UTC))).startsWith("Wed"));
        assertTrue(((String) proc.process(new DateTime(30, 2, 1, 12, 13, DateTimeZone.UTC))).startsWith("Fri"));
        assertTrue(((String) proc.process(new DateTime(10902, 8, 22, 11, 11, DateTimeZone.UTC))).startsWith("Tue"));
        assertTrue(((String) proc.process(new DateTime(10902, 8, 22, 9, 59, DateTimeZone.UTC))).startsWith("Mon"));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/33621")
    public void testValidMonthNamesInUTC() {
        NamedDateTimeProcessor proc  = new NamedDateTimeProcessor(NameExtractor.MONTH_NAME, UTC);
        assertTrue(((String) proc.process("0")).startsWith("Jan"));
        assertTrue(((String) proc.process("-64164233612338")).startsWith("Sep"));
        assertTrue(((String) proc.process("64164233612338")).startsWith("Apr"));

        assertTrue(((String) proc.process(new DateTime(0L, DateTimeZone.UTC))).startsWith("Jan"));
        assertTrue(((String) proc.process(new DateTime(-5400, 12, 25, 10, 10, DateTimeZone.UTC))).startsWith("Dec"));
        assertTrue(((String) proc.process(new DateTime(30, 2, 1, 12, 13, DateTimeZone.UTC))).startsWith("Feb"));
        assertTrue(((String) proc.process(new DateTime(10902, 8, 22, 11, 11, DateTimeZone.UTC))).startsWith("Aug"));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/33621")
    public void testValidMonthNamesWithNonUTCTimeZone() {
        NamedDateTimeProcessor proc = new NamedDateTimeProcessor(NameExtractor.MONTH_NAME, TimeZone.getTimeZone("GMT-3:00"));
        assertTrue(((String) proc.process("0")).startsWith("Dec"));
        assertTrue(((String) proc.process("-64165813612338")).startsWith("Aug")); // GMT: Tuesday, September 1, -0064 2:53:07.662 AM
        assertTrue(((String) proc.process("64164233612338")).startsWith("Apr")); // GMT: Monday, April 14, 4003 2:13:32.338 PM

        assertTrue(((String) proc.process(new DateTime(0L, DateTimeZone.UTC))).startsWith("Dec"));
        assertTrue(((String) proc.process(new DateTime(-5400, 12, 1, 1, 1, DateTimeZone.UTC))).startsWith("Nov"));
        assertTrue(((String) proc.process(new DateTime(30, 2, 1, 12, 13, DateTimeZone.UTC))).startsWith("Feb"));
        assertTrue(((String) proc.process(new DateTime(10902, 8, 1, 2, 59, DateTimeZone.UTC))).startsWith("Jul"));
        assertTrue(((String) proc.process(new DateTime(10902, 8, 1, 3, 00, DateTimeZone.UTC))).startsWith("Aug"));
    }
}
