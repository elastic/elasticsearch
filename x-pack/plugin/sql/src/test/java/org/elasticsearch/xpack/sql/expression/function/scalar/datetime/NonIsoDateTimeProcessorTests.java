/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xpack.sql.AbstractSqlWireSerializingTestCase;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.NonIsoDateTimeProcessor.NonIsoDateTimeExtractor;

import java.io.IOException;
import java.time.ZoneId;

import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.dateTime;
import static org.elasticsearch.xpack.sql.util.DateUtils.UTC;

public class NonIsoDateTimeProcessorTests extends AbstractSqlWireSerializingTestCase<NonIsoDateTimeProcessor> {
    

    public static NonIsoDateTimeProcessor randomNonISODateTimeProcessor() {
        return new NonIsoDateTimeProcessor(randomFrom(NonIsoDateTimeExtractor.values()), UTC);
    }

    @Override
    protected NonIsoDateTimeProcessor createTestInstance() {
        return randomNonISODateTimeProcessor();
    }

    @Override
    protected Reader<NonIsoDateTimeProcessor> instanceReader() {
        return NonIsoDateTimeProcessor::new;
    }

    @Override
    protected NonIsoDateTimeProcessor mutateInstance(NonIsoDateTimeProcessor instance) throws IOException {
        NonIsoDateTimeExtractor replaced = randomValueOtherThan(instance.extractor(), () -> randomFrom(NonIsoDateTimeExtractor.values()));
        return new NonIsoDateTimeProcessor(replaced, UTC);
    }

    @Override
    protected ZoneId instanceZoneId(NonIsoDateTimeProcessor instance) {
        return instance.zoneId();
    }

    public void testNonISOWeekOfYearInUTC() {
        NonIsoDateTimeProcessor proc = new NonIsoDateTimeProcessor(NonIsoDateTimeExtractor.WEEK_OF_YEAR, UTC);
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
        NonIsoDateTimeProcessor proc = new NonIsoDateTimeProcessor(NonIsoDateTimeExtractor.WEEK_OF_YEAR, ZoneId.of("GMT-10:00"));
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
        NonIsoDateTimeProcessor proc = new NonIsoDateTimeProcessor(NonIsoDateTimeExtractor.DAY_OF_WEEK, UTC);
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
        NonIsoDateTimeProcessor proc = new NonIsoDateTimeProcessor(NonIsoDateTimeExtractor.DAY_OF_WEEK, ZoneId.of("GMT-10:00"));
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