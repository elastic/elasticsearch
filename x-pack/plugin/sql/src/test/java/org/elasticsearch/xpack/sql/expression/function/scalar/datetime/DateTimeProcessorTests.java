/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
    }

    public void testApply_withTimezoneOtherThanUTC() {
        ZoneId zoneId = ZoneId.of("Etc/GMT-10");
        DateTimeProcessor proc = new DateTimeProcessor(DateTimeExtractor.YEAR, zoneId);
        assertEquals(2018, proc.process(dateTime(2017, 12, 31, 18, 10)));

        proc = new DateTimeProcessor(DateTimeExtractor.DAY_OF_MONTH, zoneId);
        assertEquals(1, proc.process(dateTime(2017, 12, 31, 20, 30)));
    }

    public void testFailOnTime() {
        DateTimeProcessor proc = new DateTimeProcessor(DateTimeExtractor.YEAR, UTC);
        SqlIllegalArgumentException e = expectThrows(SqlIllegalArgumentException.class, () -> {
           proc.process(OffsetTime.now(UTC));
        });
        assertThat(e.getMessage(), startsWith("A [date], a [time] or a [datetime] is required; received "));
    }
}
