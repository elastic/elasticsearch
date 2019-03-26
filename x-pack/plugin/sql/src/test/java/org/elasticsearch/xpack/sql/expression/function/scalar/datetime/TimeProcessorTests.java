/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;

import java.time.ZoneId;

import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.dateTime;
import static org.elasticsearch.xpack.sql.util.DateUtils.UTC;

public class TimeProcessorTests extends AbstractWireSerializingTestCase<TimeProcessor> {

    public static TimeProcessor randomTimeProcessor() {
        return new TimeProcessor(randomFrom(DateTimeExtractor.values()), randomZone());
    }

    @Override
    protected TimeProcessor createTestInstance() {
        return randomTimeProcessor();
    }

    @Override
    protected Reader<TimeProcessor> instanceReader() {
        return TimeProcessor::new;
    }

    @Override
    protected TimeProcessor mutateInstance(TimeProcessor instance) {
        DateTimeExtractor replaced = randomValueOtherThan(instance.extractor(), () -> randomFrom(DateTimeExtractor.values()));
        return new TimeProcessor(replaced, randomValueOtherThan(UTC, ESTestCase::randomZone));
    }

    public void testApply_withTimeZoneUTC() {
        DateTimeProcessor proc = new DateTimeProcessor(DateTimeExtractor.SECOND_OF_MINUTE, UTC);
        assertEquals(0, proc.process(dateTime(0L)));
        assertEquals(2, proc.process(dateTime(2345L)));

        proc = new DateTimeProcessor(DateTimeExtractor.MINUTE_OF_DAY, UTC);
        assertEquals(0, proc.process(dateTime(0L)));
        assertEquals(610, proc.process(dateTime(2017, 01, 02, 10, 10)));

        proc = new DateTimeProcessor(DateTimeExtractor.MINUTE_OF_HOUR, UTC);
        assertEquals(0, proc.process(dateTime(0L)));
        assertEquals(10, proc.process(dateTime(2017, 01, 02, 10, 10)));

        proc = new DateTimeProcessor(DateTimeExtractor.HOUR_OF_DAY, UTC);
        assertEquals(0, proc.process(dateTime(0L)));
        assertEquals(10, proc.process(dateTime(2017, 01, 02, 10, 10)));
        assertEquals(11, proc.process(dateTime(2017, 01, 31, 11, 10)));
    }

    public void testApply_withTimeZoneOtherThanUTC() {
        ZoneId zoneId = ZoneId.of("Etc/GMT-10");

        DateTimeProcessor proc = new DateTimeProcessor(DateTimeExtractor.SECOND_OF_MINUTE, zoneId);
        assertEquals(0, proc.process(dateTime(0L)));
        assertEquals(2, proc.process(dateTime(2345L)));

        proc = new DateTimeProcessor(DateTimeExtractor.MINUTE_OF_DAY, zoneId);
        assertEquals(600, proc.process(dateTime(0L)));
        assertEquals(1210, proc.process(dateTime(2017, 01, 02, 10, 10)));

        proc = new DateTimeProcessor(DateTimeExtractor.MINUTE_OF_HOUR, zoneId);
        assertEquals(0, proc.process(dateTime(0L)));
        assertEquals(10, proc.process(dateTime(2017, 01, 02, 10, 10)));

        proc = new DateTimeProcessor(DateTimeExtractor.HOUR_OF_DAY, zoneId);
        assertEquals(10, proc.process(dateTime(0L)));
        assertEquals(20, proc.process(dateTime(2017, 01, 02, 10, 10)));
        assertEquals(4, proc.process(dateTime(2017, 01, 31, 18, 10)));
    }
}
