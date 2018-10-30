/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.bootstrap.JavaVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.NamedDateTimeProcessor.NameExtractor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assume;

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

    public void testValidDayNamesInUTC() {
        assumeJava9PlusAndCompatLocaleProviderSetting();
        NamedDateTimeProcessor proc = new NamedDateTimeProcessor(NameExtractor.DAY_NAME, UTC);
        assertEquals("Thursday", proc.process("0"));
        assertEquals("Saturday", proc.process("-64164233612338"));
        assertEquals("Monday", proc.process("64164233612338"));
        
        assertEquals("Thursday", proc.process(new DateTime(0L, DateTimeZone.UTC)));
        assertEquals("Thursday", proc.process(new DateTime(-5400, 12, 25, 2, 0, DateTimeZone.UTC)));
        assertEquals("Friday", proc.process(new DateTime(30, 2, 1, 12, 13, DateTimeZone.UTC)));
        assertEquals("Tuesday", proc.process(new DateTime(10902, 8, 22, 11, 11, DateTimeZone.UTC)));
    }

    public void testValidDayNamesWithNonUTCTimeZone() {
        assumeJava9PlusAndCompatLocaleProviderSetting();
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

    public void testValidMonthNamesInUTC() {
        assumeJava9PlusAndCompatLocaleProviderSetting();
        NamedDateTimeProcessor proc  = new NamedDateTimeProcessor(NameExtractor.MONTH_NAME, UTC);
        assertEquals("January", proc.process("0"));
        assertEquals("September", proc.process("-64164233612338"));
        assertEquals("April", proc.process("64164233612338"));

        assertEquals("January", proc.process(new DateTime(0L, DateTimeZone.UTC)));
        assertEquals("December", proc.process(new DateTime(-5400, 12, 25, 10, 10, DateTimeZone.UTC)));
        assertEquals("February", proc.process(new DateTime(30, 2, 1, 12, 13, DateTimeZone.UTC)));
        assertEquals("August", proc.process(new DateTime(10902, 8, 22, 11, 11, DateTimeZone.UTC)));
    }

    public void testValidMonthNamesWithNonUTCTimeZone() {
        assumeJava9PlusAndCompatLocaleProviderSetting();
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
    
    /*
     * This method checks the existence of a jvm parameter that should exist in ES jvm.options for Java 9+. If the parameter is
     * missing, the tests will be skipped. Not doing this, the tests will fail because the day and month names will be in the narrow
     * format (Mon, Tue, Jan, Feb etc) instead of full format (Monday, Tuesday, January, February etc).
     * 
     * Related infra issue: https://github.com/elastic/elasticsearch/issues/33796
     */
    private void assumeJava9PlusAndCompatLocaleProviderSetting() {
        // at least Java 9
        if (JavaVersion.current().compareTo(JavaVersion.parse("9")) < 0) {
            return;
        }
        String beforeJava9CompatibleLocale = System.getProperty("java.locale.providers");
        // and COMPAT setting needs to be first on the list
        boolean isBeforeJava9Compatible = beforeJava9CompatibleLocale != null 
                && Strings.tokenizeToStringArray(beforeJava9CompatibleLocale, ",")[0].equals("COMPAT");
        Assume.assumeTrue(isBeforeJava9Compatible);
    }
}
