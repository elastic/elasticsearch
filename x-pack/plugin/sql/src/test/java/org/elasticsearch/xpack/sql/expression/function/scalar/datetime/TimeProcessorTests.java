/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xpack.sql.AbstractSqlWireSerializingTestCase;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;

import java.time.ZoneId;

import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.time;
import static org.elasticsearch.xpack.sql.util.DateUtils.UTC;

public class TimeProcessorTests extends AbstractSqlWireSerializingTestCase<TimeProcessor> {

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
    protected ZoneId instanceZoneId(TimeProcessor instance) {
        return instance.zoneId();
    }

    @Override
    protected TimeProcessor mutateInstance(TimeProcessor instance) {
        DateTimeExtractor replaced = randomValueOtherThan(instance.extractor(), () -> randomFrom(DateTimeExtractor.values()));
        return new TimeProcessor(replaced, randomZone());
    }

    public void testApply_withTimeZoneUTC() {
        TimeProcessor proc = new TimeProcessor(DateTimeExtractor.SECOND_OF_MINUTE, UTC);
        assertEquals(0, proc.process(time(0L)));
        assertEquals(2, proc.process(time(2345L)));

        proc = new TimeProcessor(DateTimeExtractor.MINUTE_OF_DAY, UTC);
        assertEquals(0, proc.process(time(0L)));
        assertEquals(620, proc.process(time(10, 20, 30, 123456789)));

        proc = new TimeProcessor(DateTimeExtractor.MINUTE_OF_HOUR, UTC);
        assertEquals(0, proc.process(time(0L)));
        assertEquals(20, proc.process(time(10, 20, 30, 123456789)));

        proc = new TimeProcessor(DateTimeExtractor.HOUR_OF_DAY, UTC);
        assertEquals(0, proc.process(time(0L)));
        assertEquals(10, proc.process(time(10, 20, 30, 123456789)));
    }

    public void testApply_withTimeZoneOtherThanUTC() {
        ZoneId zoneId = ZoneId.of("Etc/GMT-10");

        TimeProcessor proc = new TimeProcessor(DateTimeExtractor.SECOND_OF_MINUTE, zoneId);
        assertEquals(0, proc.process(time(0L)));
        assertEquals(2, proc.process(time(2345L)));

        proc = new TimeProcessor(DateTimeExtractor.MINUTE_OF_DAY, zoneId);
        assertEquals(600, proc.process(time(0L)));
        assertEquals(1220, proc.process(time(10, 20, 30, 123456789)));

        proc = new TimeProcessor(DateTimeExtractor.MINUTE_OF_HOUR, zoneId);
        assertEquals(0, proc.process(time(0L)));
        assertEquals(20, proc.process(time(10, 20, 30, 123456789)));

        proc = new TimeProcessor(DateTimeExtractor.HOUR_OF_DAY, zoneId);
        assertEquals(10, proc.process(time(0L)));
        assertEquals(20, proc.process(time(10, 20, 30, 123456789)));;
        assertEquals(4, proc.process(time(18, 20, 30, 123456789)));
    }
}
