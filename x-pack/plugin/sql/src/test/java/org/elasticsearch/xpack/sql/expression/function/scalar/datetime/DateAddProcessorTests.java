/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.gen.processor.ConstantProcessor;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.AbstractSqlWireSerializingTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.elasticsearch.xpack.ql.expression.Literal.NULL;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomDatetimeLiteral;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomIntLiteral;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.dateTime;
import static org.elasticsearch.xpack.sql.proto.StringUtils.ISO_DATETIME_WITH_NANOS;

public class DateAddProcessorTests extends AbstractSqlWireSerializingTestCase<DateAddProcessor> {

    public static DateAddProcessor randomDateAddProcessor() {
        return new DateAddProcessor(
            new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(0, 128)),
            new ConstantProcessor(randomInt()),
            new ConstantProcessor(DateTimeTestUtils.nowWithMillisResolution()),
            randomZone());
    }

    @Override
    protected DateAddProcessor createTestInstance() {
        return randomDateAddProcessor();
    }

    @Override
    protected Reader<DateAddProcessor> instanceReader() {
        return DateAddProcessor::new;
    }

    @Override
    protected ZoneId instanceZoneId(DateAddProcessor instance) {
        return instance.zoneId();
    }

    @Override
    protected DateAddProcessor mutateInstance(DateAddProcessor instance) {
        return new DateAddProcessor(
            new ConstantProcessor(ESTestCase.randomRealisticUnicodeOfLength(128)),
            new ConstantProcessor(randomValueOtherThan((Integer) instance.second().process(null), ESTestCase::randomInt)),
            new ConstantProcessor(DateTimeTestUtils.nowWithMillisResolution()),
            randomValueOtherThan(instance.zoneId(), ESTestCase::randomZone));
    }

    public void testInvalidInputs() {
        SqlIllegalArgumentException siae = expectThrows(SqlIllegalArgumentException.class,
                () -> new DateAdd(Source.EMPTY,
                    l(5), l(10), randomDatetimeLiteral(), randomZone()).makePipe().asProcessor().process(null));
        assertEquals("A string is required; received [5]", siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DateAdd(Source.EMPTY,
                l("days"), l("foo"), randomDatetimeLiteral(), randomZone()).makePipe().asProcessor().process(null));
        assertEquals("A number is required; received [foo]", siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DateAdd(Source.EMPTY,
                l("days"), l(10), l("foo"), randomZone()).makePipe().asProcessor().process(null));
        assertEquals("A date/datetime is required; received [foo]", siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DateAdd(Source.EMPTY,
                l("invalid"), l(10), randomDatetimeLiteral(), randomZone()).makePipe().asProcessor().process(null));
        assertEquals("A value of [YEAR, QUARTER, MONTH, DAYOFYEAR, DAY, WEEK, WEEKDAY, HOUR, MINUTE, " +
            "SECOND, MILLISECOND, MICROSECOND, NANOSECOND] or their aliases is required; received [invalid]",
            siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DateAdd(Source.EMPTY,
                l("quertar"), l(10), randomDatetimeLiteral(), randomZone()).makePipe().asProcessor().process(null));
        assertEquals("Received value [quertar] is not valid date part to add; did you mean [quarter, quarters]?",
             siae.getMessage());
    }

    public void testWithNulls() {
        assertNull(new DateAdd(Source.EMPTY,
            NULL, randomIntLiteral(), randomDatetimeLiteral(), randomZone()).makePipe().asProcessor().process(null));
        assertNull(new DateAdd(Source.EMPTY,
            l("days"), NULL, randomDatetimeLiteral(), randomZone()).makePipe().asProcessor().process(null));
        assertNull(new DateAdd(Source.EMPTY,
            l("days"), randomIntLiteral(), NULL, randomZone()).makePipe().asProcessor().process(null));
        assertNull(new DateAdd(Source.EMPTY,
            NULL, NULL, NULL, randomZone()).makePipe().asProcessor().process(null));
    }

    public void testAddition() {
        ZoneId zoneId = ZoneId.of("Etc/GMT-10");
        Literal dateTime = l(dateTime(2019, 9, 3, 18, 10, 37, 123456789));

        assertEquals("2029-09-04T04:10:37.123456789+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("years"), l(10), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2009-09-04T04:10:37.123456789+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("years"), l(-10), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));

        assertEquals("2022-03-04T04:10:37.123456789+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("quarters"), l(10), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2017-03-04T04:10:37.123456789+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("quarters"), l(-10), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));

        assertEquals("2021-05-04T04:10:37.123456789+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("month"), l(20), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2018-01-04T04:10:37.123456789+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("month"), l(-20), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));

        assertEquals("2020-05-01T04:10:37.123456789+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("day"), l(240), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2019-05-07T04:10:37.123456789+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("day"), l(-120), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));

        assertEquals("2020-12-25T04:10:37.123456789+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("dayofyear"), l(478), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2018-05-14T04:10:37.123456789+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("dayofyear"), l(-478), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));

        assertEquals("2021-12-22T04:10:37.123456789+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("weeks"), l(120), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2017-05-17T04:10:37.123456789+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("weeks"), l(-120), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));

        assertEquals("2053-06-22T04:10:37.123456789+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("weekday"), l(12345), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("1985-11-16T04:10:37.123456789+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("weekday"), l(-12345), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));

        assertEquals("2020-07-05T05:10:37.123456789+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("hours"), l(7321), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2018-11-03T03:10:37.123456789+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("hours"), l(-7321), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));

        assertEquals("2021-07-21T01:04:37.123456789+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("minute"), l(987654), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2017-10-18T07:16:37.123456789+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("minute"), l(-987654), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));

        assertEquals("2020-02-01T11:51:31.123456789+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("seconds"), l(12987654), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2019-04-06T20:29:43.123456789+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("seconds"), l(-12987654), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));

        assertEquals("2019-09-19T04:56:42.555456789+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("ms"), l(1298765432), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2019-08-20T03:24:31.691456789+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("ms"), l(-1298765432), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));

        assertEquals("2019-09-04T04:12:41.111110789+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("mcs"), l(123987654), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2019-09-04T04:08:33.135802789+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("mcs"), l(-123987654), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));

        assertEquals("2019-09-04T04:10:37.935855554+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("nanoseconds"), l(812398765), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2019-09-04T04:10:36.311058024+10:00",
            toString((ZonedDateTime) new DateAdd(Source.EMPTY, l("nanoseconds"), l(-812398765), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
    }

    private String toString(ZonedDateTime dateTime) {
        return ISO_DATETIME_WITH_NANOS.format(dateTime);
    }
}
