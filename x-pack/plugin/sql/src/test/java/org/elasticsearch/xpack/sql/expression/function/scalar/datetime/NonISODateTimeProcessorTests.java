/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.NonISODateTimeProcessor.NonISODateTimeExtractor;

import java.io.IOException;
import java.util.TimeZone;

import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.dateTime;

public class NonISODateTimeProcessorTests extends AbstractWireSerializingTestCase<NonISODateTimeProcessor> {
    
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    public static NonISODateTimeProcessor randomNonISODateTimeProcessor() {
        return new NonISODateTimeProcessor(randomFrom(NonISODateTimeExtractor.values()), UTC);
    }

    @Override
    protected NonISODateTimeProcessor createTestInstance() {
        return randomNonISODateTimeProcessor();
    }

    @Override
    protected Reader<NonISODateTimeProcessor> instanceReader() {
        return NonISODateTimeProcessor::new;
    }

    @Override
    protected NonISODateTimeProcessor mutateInstance(NonISODateTimeProcessor instance) throws IOException {
        NonISODateTimeExtractor replaced = randomValueOtherThan(instance.extractor(), () -> randomFrom(NonISODateTimeExtractor.values()));
        return new NonISODateTimeProcessor(replaced, UTC);
    }

    public void testNonISOWeekOfYearInUTC() {
        NonISODateTimeProcessor proc = new NonISODateTimeProcessor(NonISODateTimeExtractor.WEEK_OF_YEAR, UTC);
        assertEquals(2, proc.process(dateTime(568372930000L)));  //1988-01-05T09:22:10Z[UTC]
        assertEquals(6, proc.process(dateTime(981278530000L)));  //2001-02-04T09:22:10Z[UTC]
        assertEquals(7, proc.process(dateTime(224241730000L)));  //1977-02-08T09:22:10Z[UTC]
        
        assertEquals(12, proc.process(dateTime(132744130000L))); //1974-03-17T09:22:10Z[UTC]
        assertEquals(17, proc.process(dateTime(230376130000L))); //1977-04-20T09:22:10Z[UTC]
        assertEquals(17, proc.process(dateTime(766833730000L))); //1994-04-20T09:22:10Z[UTC]
        assertEquals(29, proc.process(dateTime(79780930000L)));  //1972-07-12T09:22:10Z[UTC]
        assertEquals(33, proc.process(dateTime(902913730000L))); //1998-08-12T09:22:10Z[UTC]
    }

    public void testNonISOWeekOfYearInNonUTCTimeZone() {
        NonISODateTimeProcessor proc = new NonISODateTimeProcessor(NonISODateTimeExtractor.WEEK_OF_YEAR, TimeZone.getTimeZone("GMT-10:00"));
        assertEquals(2, proc.process(dateTime(568372930000L)));
        assertEquals(5, proc.process(dateTime(981278530000L)));
        assertEquals(7, proc.process(dateTime(224241730000L)));
        
        assertEquals(11, proc.process(dateTime(132744130000L)));
        assertEquals(17, proc.process(dateTime(230376130000L)));
        assertEquals(17, proc.process(dateTime(766833730000L)));
        assertEquals(29, proc.process(dateTime(79780930000L)));
        assertEquals(33, proc.process(dateTime(902913730000L)));
    }
    
    public void testNonISODayOfWeekInUTC() {
        NonISODateTimeProcessor proc = new NonISODateTimeProcessor(NonISODateTimeExtractor.DAY_OF_WEEK, UTC);
        assertEquals(3, proc.process(dateTime(568372930000L))); //1988-01-05T09:22:10Z[UTC]
        assertEquals(1, proc.process(dateTime(981278530000L))); //2001-02-04T09:22:10Z[UTC]
        assertEquals(3, proc.process(dateTime(224241730000L))); //1977-02-08T09:22:10Z[UTC]
        
        assertEquals(1, proc.process(dateTime(132744130000L))); //1974-03-17T09:22:10Z[UTC]
        assertEquals(4, proc.process(dateTime(230376130000L))); //1977-04-20T09:22:10Z[UTC]
        assertEquals(4, proc.process(dateTime(766833730000L))); //1994-04-20T09:22:10Z[UTC]
        assertEquals(7, proc.process(dateTime(333451330000L))); //1980-07-26T09:22:10Z[UTC]
        assertEquals(6, proc.process(dateTime(874660930000L))); //1997-09-19T09:22:10Z[UTC]
    }

    public void testNonISODayOfWeekInNonUTCTimeZone() {
        NonISODateTimeProcessor proc = new NonISODateTimeProcessor(NonISODateTimeExtractor.DAY_OF_WEEK, TimeZone.getTimeZone("GMT-10:00"));
        assertEquals(2, proc.process(dateTime(568372930000L)));
        assertEquals(7, proc.process(dateTime(981278530000L)));
        assertEquals(2, proc.process(dateTime(224241730000L)));
        
        assertEquals(7, proc.process(dateTime(132744130000L)));
        assertEquals(3, proc.process(dateTime(230376130000L)));
        assertEquals(3, proc.process(dateTime(766833730000L)));
        assertEquals(6, proc.process(dateTime(333451330000L)));
        assertEquals(5, proc.process(dateTime(874660930000L)));
    }
}