/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
        // 1 Jan 1988 is Friday - under Sunday,1 rule it is the first week of the year (under ISO rule it would be 53 of the previous year
        // hence the 5th Jan 1988 Tuesday is the second week of a year
        assertEquals(2, proc.process(dateTime(568372930000L)));  //1988-01-05T09:22:10Z[UTC]
        assertEquals(6, proc.process(dateTime(981278530000L)));  //2001-02-04T09:22:10Z[UTC]
        assertEquals(7, proc.process(dateTime(224241730000L)));  //1977-02-08T09:22:10Z[UTC]

        assertEquals(12, proc.process(dateTime(132744130000L))); //1974-03-17T09:22:10Z[UTC]
        assertEquals(17, proc.process(dateTime(230376130000L))); //1977-04-20T09:22:10Z[UTC]
        assertEquals(17, proc.process(dateTime(766833730000L))); //1994-04-20T09:22:10Z[UTC]
        assertEquals(29, proc.process(dateTime(79780930000L)));  //1972-07-12T09:22:10Z[UTC]
        assertEquals(33, proc.process(dateTime(902913730000L))); //1998-08-12T09:22:10Z[UTC]

        // Tested against MS-SQL Server and H2
        assertEquals(2, proc.process(dateTime(1988, 1, 5, 0, 0, 0, 0)));
        assertEquals(6, proc.process(dateTime(2001, 2, 4, 0, 0, 0, 0)));
        assertEquals(7, proc.process(dateTime(1977, 2, 8, 0, 0, 0, 0)));
        assertEquals(12, proc.process(dateTime(1974, 3, 17, 0, 0, 0, 0)));
        assertEquals(17, proc.process(dateTime(1977, 4, 20, 0, 0, 0, 0)));
        assertEquals(17, proc.process(dateTime(1994, 4, 20, 0, 0, 0, 0)));
        assertEquals(17, proc.process(dateTime(2002, 4, 27, 0, 0, 0, 0)));
        assertEquals(18, proc.process(dateTime(1974, 5, 3, 0, 0, 0, 0)));
        assertEquals(22, proc.process(dateTime(1997, 5, 30, 0, 0, 0, 0)));
        assertEquals(23, proc.process(dateTime(1995, 6, 4, 0, 0, 0, 0)));
        assertEquals(29, proc.process(dateTime(1972, 7, 12, 0, 0, 0, 0)));
        assertEquals(30, proc.process(dateTime(1980, 7, 26, 0, 0, 0, 0)));
        assertEquals(33, proc.process(dateTime(1998, 8, 12, 0, 0, 0, 0)));
        assertEquals(36, proc.process(dateTime(1995, 9, 3, 0, 0, 0, 0)));
        assertEquals(37, proc.process(dateTime(1976, 9, 9, 0, 0, 0, 0)));
        assertEquals(38, proc.process(dateTime(1997, 9, 19, 0, 0, 0, 0)));
        assertEquals(45, proc.process(dateTime(1980, 11, 7, 0, 0, 0, 0)));
        assertEquals(1, proc.process(dateTime(2005, 1, 1, 0, 0, 0, 0)));
        assertEquals(53, proc.process(dateTime(2007, 12, 31, 0, 0, 0, 0)));
        assertEquals(53, proc.process(dateTime(2019, 12, 31, 20, 22, 33, 987654321)));
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

        // Tested against MS-SQL Server and H2
        assertEquals(2, proc.process(dateTime(1988, 1, 5, 0, 0, 0, 0)));
        assertEquals(5, proc.process(dateTime(2001, 2, 4, 0, 0, 0, 0)));
        assertEquals(7, proc.process(dateTime(1977, 2, 8, 0, 0, 0, 0)));
        assertEquals(11, proc.process(dateTime(1974, 3, 17, 0, 0, 0, 0)));
        assertEquals(17, proc.process(dateTime(1977, 4, 20, 0, 0, 0, 0)));
        assertEquals(17, proc.process(dateTime(1994, 4, 20, 0, 0, 0, 0)));
        assertEquals(17, proc.process(dateTime(2002, 4, 27, 0, 0, 0, 0)));
        assertEquals(18, proc.process(dateTime(1974, 5, 3, 0, 0, 0, 0)));
        assertEquals(22, proc.process(dateTime(1997, 5, 30, 0, 0, 0, 0)));
        assertEquals(22, proc.process(dateTime(1995, 6, 4, 0, 0, 0, 0)));
        assertEquals(29, proc.process(dateTime(1972, 7, 12, 0, 0, 0, 0)));
        assertEquals(30, proc.process(dateTime(1980, 7, 26, 0, 0, 0, 0)));
        assertEquals(33, proc.process(dateTime(1998, 8, 12, 0, 0, 0, 0)));
        assertEquals(35, proc.process(dateTime(1995, 9, 3, 0, 0, 0, 0)));
        assertEquals(37, proc.process(dateTime(1976, 9, 9, 0, 0, 0, 0)));
        assertEquals(38, proc.process(dateTime(1997, 9, 19, 0, 0, 0, 0)));
        assertEquals(45, proc.process(dateTime(1980, 11, 7, 0, 0, 0, 0)));
        assertEquals(53, proc.process(dateTime(2005, 1, 1, 0, 0, 0, 0)));
        assertEquals(53, proc.process(dateTime(2007, 12, 31, 0, 0, 0, 0)));
        assertEquals(53, proc.process(dateTime(2019, 12, 31, 20, 22, 33, 987654321)));
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
