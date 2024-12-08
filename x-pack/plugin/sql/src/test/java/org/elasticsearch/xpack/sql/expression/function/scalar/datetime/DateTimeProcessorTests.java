/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xpack.sql.AbstractSqlWireSerializingTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;

import java.time.OffsetTime;
import java.time.ZoneId;

import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.dateTime;
import static org.elasticsearch.xpack.sql.util.DateUtils.UTC;
import static org.hamcrest.Matchers.startsWith;

public class DateTimeProcessorTests extends AbstractSqlWireSerializingTestCase<DateTimeProcessor> {

    public static DateTimeProcessor randomDateTimeProcessor() {
        return new DateTimeProcessor(randomFrom(DateTimeExtractor.values()), randomZone());
    }

    @Override
    protected DateTimeProcessor createTestInstance() {
        return randomDateTimeProcessor();
    }

    @Override
    protected Reader<DateTimeProcessor> instanceReader() {
        return DateTimeProcessor::new;
    }

    @Override
    protected ZoneId instanceZoneId(DateTimeProcessor instance) {
        return instance.zoneId();
    }

    @Override
    protected DateTimeProcessor mutateInstance(DateTimeProcessor instance) {
        DateTimeExtractor replaced = randomValueOtherThan(instance.extractor(), () -> randomFrom(DateTimeExtractor.values()));
        return new DateTimeProcessor(replaced, randomZone());
    }

    public void testApply_withTimezoneUTC() {
        DateTimeProcessor proc = new DateTimeProcessor(DateTimeExtractor.YEAR, UTC);
        assertEquals(1970, proc.process(dateTime(0L)));
        assertEquals(2017, proc.process(dateTime(2017, 01, 02, 10, 10)));

        proc = new DateTimeProcessor(DateTimeExtractor.DAY_OF_MONTH, UTC);
        assertEquals(1, proc.process(dateTime(0L)));
        assertEquals(2, proc.process(dateTime(2017, 01, 02, 10, 10)));
        assertEquals(31, proc.process(dateTime(2017, 01, 31, 10, 10)));

        // Tested against MS-SQL Server and H2
        proc = new DateTimeProcessor(DateTimeExtractor.ISO_WEEK_OF_YEAR, UTC);
        assertEquals(1, proc.process(dateTime(1988, 1, 5, 0, 0, 0, 0)));
        assertEquals(5, proc.process(dateTime(2001, 2, 4, 0, 0, 0, 0)));
        assertEquals(6, proc.process(dateTime(1977, 2, 8, 0, 0, 0, 0)));
        assertEquals(11, proc.process(dateTime(1974, 3, 17, 0, 0, 0, 0)));
        assertEquals(16, proc.process(dateTime(1977, 4, 20, 0, 0, 0, 0)));
        assertEquals(16, proc.process(dateTime(1994, 4, 20, 0, 0, 0, 0)));
        assertEquals(17, proc.process(dateTime(2002, 4, 27, 0, 0, 0, 0)));
        assertEquals(18, proc.process(dateTime(1974, 5, 3, 0, 0, 0, 0)));
        assertEquals(22, proc.process(dateTime(1997, 5, 30, 0, 0, 0, 0)));
        assertEquals(22, proc.process(dateTime(1995, 6, 4, 0, 0, 0, 0)));
        assertEquals(28, proc.process(dateTime(1972, 7, 12, 0, 0, 0, 0)));
        assertEquals(30, proc.process(dateTime(1980, 7, 26, 0, 0, 0, 0)));
        assertEquals(33, proc.process(dateTime(1998, 8, 12, 0, 0, 0, 0)));
        assertEquals(35, proc.process(dateTime(1995, 9, 3, 0, 0, 0, 0)));
        assertEquals(37, proc.process(dateTime(1976, 9, 9, 0, 0, 0, 0)));
        assertEquals(38, proc.process(dateTime(1997, 9, 19, 0, 0, 0, 0)));
        assertEquals(45, proc.process(dateTime(1980, 11, 7, 0, 0, 0, 0)));
        assertEquals(53, proc.process(dateTime(2005, 1, 1, 0, 0, 0, 0)));
        assertEquals(1, proc.process(dateTime(2007, 12, 31, 0, 0, 0, 0)));
        assertEquals(1, proc.process(dateTime(2019, 12, 31, 20, 22, 33, 987654321)));
    }

    public void testApply_withTimezoneOtherThanUTC() {
        ZoneId zoneId = ZoneId.of("Etc/GMT-10");
        DateTimeProcessor proc = new DateTimeProcessor(DateTimeExtractor.YEAR, zoneId);
        assertEquals(2018, proc.process(dateTime(2017, 12, 31, 18, 10)));

        proc = new DateTimeProcessor(DateTimeExtractor.DAY_OF_MONTH, zoneId);
        assertEquals(1, proc.process(dateTime(2017, 12, 31, 20, 30)));

        // Tested against MS-SQL Server and H2
        proc = new DateTimeProcessor(DateTimeExtractor.ISO_WEEK_OF_YEAR, UTC);
        assertEquals(1, proc.process(dateTime(1988, 1, 5, 0, 0, 0, 0)));
        assertEquals(5, proc.process(dateTime(2001, 2, 4, 0, 0, 0, 0)));
        assertEquals(6, proc.process(dateTime(1977, 2, 8, 0, 0, 0, 0)));
        assertEquals(11, proc.process(dateTime(1974, 3, 17, 0, 0, 0, 0)));
        assertEquals(16, proc.process(dateTime(1977, 4, 20, 0, 0, 0, 0)));
        assertEquals(16, proc.process(dateTime(1994, 4, 20, 0, 0, 0, 0)));
        assertEquals(17, proc.process(dateTime(2002, 4, 27, 0, 0, 0, 0)));
        assertEquals(18, proc.process(dateTime(1974, 5, 3, 0, 0, 0, 0)));
        assertEquals(22, proc.process(dateTime(1997, 5, 30, 0, 0, 0, 0)));
        assertEquals(22, proc.process(dateTime(1995, 6, 4, 0, 0, 0, 0)));
        assertEquals(28, proc.process(dateTime(1972, 7, 12, 0, 0, 0, 0)));
        assertEquals(30, proc.process(dateTime(1980, 7, 26, 0, 0, 0, 0)));
        assertEquals(33, proc.process(dateTime(1998, 8, 12, 0, 0, 0, 0)));
        assertEquals(35, proc.process(dateTime(1995, 9, 3, 0, 0, 0, 0)));
        assertEquals(37, proc.process(dateTime(1976, 9, 9, 0, 0, 0, 0)));
        assertEquals(38, proc.process(dateTime(1997, 9, 19, 0, 0, 0, 0)));
        assertEquals(45, proc.process(dateTime(1980, 11, 7, 0, 0, 0, 0)));
        assertEquals(53, proc.process(dateTime(2005, 1, 1, 0, 0, 0, 0)));
        assertEquals(1, proc.process(dateTime(2007, 12, 31, 0, 0, 0, 0)));
        assertEquals(1, proc.process(dateTime(2019, 12, 31, 20, 22, 33, 987654321)));
    }

    public void testFailOnTime() {
        DateTimeProcessor proc = new DateTimeProcessor(DateTimeExtractor.YEAR, UTC);
        SqlIllegalArgumentException e = expectThrows(SqlIllegalArgumentException.class, () -> { proc.process(OffsetTime.now(UTC)); });
        assertThat(e.getMessage(), startsWith("A [date], a [time] or a [datetime] is required; received "));
    }
}
