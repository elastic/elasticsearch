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
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.AbstractSqlWireSerializingTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.literal.interval.IntervalDayTime;
import org.elasticsearch.xpack.sql.expression.literal.interval.IntervalYearMonth;
import org.elasticsearch.xpack.sql.proto.StringUtils;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.time.Duration;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;

import static org.elasticsearch.xpack.ql.expression.Literal.NULL;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomDatetimeLiteral;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.dateTime;
import static org.elasticsearch.xpack.sql.proto.StringUtils.ISO_DATETIME_WITH_NANOS;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_DAY_TO_SECOND;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_YEAR_TO_MONTH;

public class DateTruncProcessorTests extends AbstractSqlWireSerializingTestCase<DateTruncProcessor> {

    public static DateTruncProcessor randomDateTruncProcessor() {
        return new DateTruncProcessor(
            new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(0, 128)),
            new ConstantProcessor(DateTimeTestUtils.nowWithMillisResolution()),
            randomZone()
        );
    }

    @Override
    protected DateTruncProcessor createTestInstance() {
        return randomDateTruncProcessor();
    }

    @Override
    protected Reader<DateTruncProcessor> instanceReader() {
        return DateTruncProcessor::new;
    }

    @Override
    protected ZoneId instanceZoneId(DateTruncProcessor instance) {
        return instance.zoneId();
    }

    @Override
    protected DateTruncProcessor mutateInstance(DateTruncProcessor instance) {
        return new DateTruncProcessor(
            new ConstantProcessor(ESTestCase.randomRealisticUnicodeOfLength(128)),
            new ConstantProcessor(DateTimeTestUtils.nowWithMillisResolution()),
            randomValueOtherThan(instance.zoneId(), ESTestCase::randomZone)
        );
    }

    public void testInvalidInputs() {
        TemporalAmount period = Period.ofYears(2018).plusMonths(11);
        Literal yearToMonth = intervalLiteral(period, INTERVAL_YEAR_TO_MONTH);
        TemporalAmount duration = Duration.ofDays(42).plusHours(12).plusMinutes(23).plusSeconds(12).plusNanos(143000000);
        Literal dayToSecond = intervalLiteral(duration, INTERVAL_DAY_TO_SECOND);

        SqlIllegalArgumentException siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateTrunc(Source.EMPTY, l(5), randomDatetimeLiteral(), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals("A string is required; received [5]", siae.getMessage());

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateTrunc(Source.EMPTY, l("days"), l("foo"), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals("A date/datetime/interval is required; received [foo]", siae.getMessage());

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateTrunc(Source.EMPTY, l("invalid"), randomDatetimeLiteral(), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals(
            "A value of [MILLENNIUM, CENTURY, DECADE, YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, "
                + "SECOND, MILLISECOND, MICROSECOND, NANOSECOND] or their aliases is required; received [invalid]",
            siae.getMessage()
        );

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateTrunc(Source.EMPTY, l("dacede"), randomDatetimeLiteral(), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals("Received value [dacede] is not valid date part for truncation; did you mean [decade, decades]?", siae.getMessage());

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateTrunc(Source.EMPTY, l("weeks"), yearToMonth, null).makePipe().asProcessor().process(null)
        );
        assertEquals("Truncating intervals is not supported for weeks units", siae.getMessage());

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateTrunc(Source.EMPTY, l("week"), dayToSecond, null).makePipe().asProcessor().process(null)
        );
        assertEquals("Truncating intervals is not supported for week units", siae.getMessage());
    }

    public void testWithNulls() {
        assertNull(new DateTrunc(Source.EMPTY, NULL, randomDatetimeLiteral(), randomZone()).makePipe().asProcessor().process(null));
        assertNull(new DateTrunc(Source.EMPTY, l("days"), NULL, randomZone()).makePipe().asProcessor().process(null));
        assertNull(new DateTrunc(Source.EMPTY, NULL, NULL, randomZone()).makePipe().asProcessor().process(null));
    }

    public void testTruncation() {
        ZoneId zoneId = ZoneId.of("Etc/GMT-10");
        Literal dateTime = l(dateTime(2019, 9, 3, 18, 10, 37, 123456789));
        TemporalAmount period = Period.ofYears(2019).plusMonths(10);
        Literal yearToMonth = intervalLiteral(period, INTERVAL_YEAR_TO_MONTH);
        TemporalAmount duration = Duration.ofDays(105).plusHours(2).plusMinutes(45).plusSeconds(55).plusNanos(123456789);
        Literal dayToSecond = intervalLiteral(duration, INTERVAL_DAY_TO_SECOND);

        assertEquals(
            "2000-01-01T00:00:00.000+10:00",
            DateUtils.toString(
                (ZonedDateTime) new DateTrunc(Source.EMPTY, l("millennia"), dateTime, zoneId).makePipe().asProcessor().process(null)
            )
        );
        assertEquals(
            "2000-01-01T00:00:00.000+10:00",
            DateUtils.toString(
                (ZonedDateTime) new DateTrunc(Source.EMPTY, l("CENTURY"), dateTime, zoneId).makePipe().asProcessor().process(null)
            )
        );
        assertEquals(
            "2010-01-01T00:00:00.000+10:00",
            DateUtils.toString(
                (ZonedDateTime) new DateTrunc(Source.EMPTY, l("decades"), dateTime, zoneId).makePipe().asProcessor().process(null)
            )
        );
        assertEquals(
            "2019-01-01T00:00:00.000+10:00",
            DateUtils.toString(
                (ZonedDateTime) new DateTrunc(Source.EMPTY, l("years"), dateTime, zoneId).makePipe().asProcessor().process(null)
            )
        );
        assertEquals(
            "2019-07-01T00:00:00.000+10:00",
            toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("quarters"), dateTime, zoneId).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "2019-09-01T00:00:00.000+10:00",
            toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("month"), dateTime, zoneId).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "2019-09-02T00:00:00.000+10:00",
            toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("weeks"), dateTime, zoneId).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "2019-09-04T00:00:00.000+10:00",
            toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("days"), dateTime, zoneId).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "2019-09-04T04:00:00.000+10:00",
            toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("hh"), dateTime, zoneId).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "2019-09-04T04:10:00.000+10:00",
            toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("mi"), dateTime, zoneId).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "2019-09-04T04:10:37.000+10:00",
            toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("second"), dateTime, zoneId).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "2019-09-04T04:10:37.123+10:00",
            toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("ms"), dateTime, zoneId).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "2019-09-04T04:10:37.123456+10:00",
            toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("mcs"), dateTime, zoneId).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "2019-09-04T04:10:37.123456789+10:00",
            toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("nanoseconds"), dateTime, zoneId).makePipe().asProcessor().process(null))
        );

        assertEquals(
            "+2000-0",
            toString(
                (IntervalYearMonth) new DateTrunc(Source.EMPTY, l("millennia"), yearToMonth, null).makePipe().asProcessor().process(null)
            )
        );
        assertEquals(
            "+2000-0",
            toString(
                (IntervalYearMonth) new DateTrunc(Source.EMPTY, l("CENTURY"), yearToMonth, null).makePipe().asProcessor().process(null)
            )
        );
        assertEquals(
            "+2010-0",
            toString(
                (IntervalYearMonth) new DateTrunc(Source.EMPTY, l("decades"), yearToMonth, null).makePipe().asProcessor().process(null)
            )
        );
        assertEquals(
            "+2019-0",
            toString((IntervalYearMonth) new DateTrunc(Source.EMPTY, l("years"), yearToMonth, null).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "+2019-9",
            toString(
                (IntervalYearMonth) new DateTrunc(Source.EMPTY, l("quarters"), yearToMonth, null).makePipe().asProcessor().process(null)
            )
        );
        assertEquals(
            "+2019-10",
            toString((IntervalYearMonth) new DateTrunc(Source.EMPTY, l("month"), yearToMonth, null).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "+2019-10",
            toString((IntervalYearMonth) new DateTrunc(Source.EMPTY, l("days"), yearToMonth, null).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "+2019-10",
            toString((IntervalYearMonth) new DateTrunc(Source.EMPTY, l("hh"), yearToMonth, null).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "+2019-10",
            toString((IntervalYearMonth) new DateTrunc(Source.EMPTY, l("mi"), yearToMonth, null).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "+2019-10",
            toString((IntervalYearMonth) new DateTrunc(Source.EMPTY, l("second"), yearToMonth, null).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "+2019-10",
            toString((IntervalYearMonth) new DateTrunc(Source.EMPTY, l("ms"), yearToMonth, null).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "+2019-10",
            toString((IntervalYearMonth) new DateTrunc(Source.EMPTY, l("mcs"), yearToMonth, null).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "+2019-10",
            toString(
                (IntervalYearMonth) new DateTrunc(Source.EMPTY, l("nanoseconds"), yearToMonth, null).makePipe().asProcessor().process(null)
            )
        );

        assertEquals(
            "+0 00:00:00",
            toString(
                (IntervalDayTime) new DateTrunc(Source.EMPTY, l("millennia"), dayToSecond, null).makePipe().asProcessor().process(null)
            )
        );
        assertEquals(
            "+0 00:00:00",
            toString((IntervalDayTime) new DateTrunc(Source.EMPTY, l("CENTURY"), dayToSecond, null).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "+0 00:00:00",
            toString((IntervalDayTime) new DateTrunc(Source.EMPTY, l("decades"), dayToSecond, null).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "+0 00:00:00",
            toString((IntervalDayTime) new DateTrunc(Source.EMPTY, l("years"), dayToSecond, null).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "+0 00:00:00",
            toString((IntervalDayTime) new DateTrunc(Source.EMPTY, l("quarters"), dayToSecond, null).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "+0 00:00:00",
            toString((IntervalDayTime) new DateTrunc(Source.EMPTY, l("month"), dayToSecond, null).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "+105 00:00:00",
            toString((IntervalDayTime) new DateTrunc(Source.EMPTY, l("days"), dayToSecond, null).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "+105 02:00:00",
            toString((IntervalDayTime) new DateTrunc(Source.EMPTY, l("hh"), dayToSecond, null).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "+105 02:45:00",
            toString((IntervalDayTime) new DateTrunc(Source.EMPTY, l("mi"), dayToSecond, null).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "+105 02:45:55",
            toString((IntervalDayTime) new DateTrunc(Source.EMPTY, l("second"), dayToSecond, null).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "+105 02:45:55.123",
            toString((IntervalDayTime) new DateTrunc(Source.EMPTY, l("ms"), dayToSecond, null).makePipe().asProcessor().process(null))
        );
        assertEquals(
            "+105 02:45:55.123",
            toString(
                (IntervalDayTime) new DateTrunc(Source.EMPTY, l("microseconds"), dayToSecond, null).makePipe().asProcessor().process(null)
            )
        );
        assertEquals(
            "+105 02:45:55.123",
            toString(
                (IntervalDayTime) new DateTrunc(Source.EMPTY, l("nanoseconds"), dayToSecond, null).makePipe().asProcessor().process(null)
            )
        );
    }

    public void testTruncationEdgeCases() {
        ZoneId zoneId = ZoneId.of("Etc/GMT-10");
        Literal dateTime = l(dateTime(-11412, 9, 3, 18, 10, 37, 123456789));
        assertEquals(
            "-11000-01-01T00:00:00.000+10:00",
            DateUtils.toString(
                (ZonedDateTime) new DateTrunc(Source.EMPTY, l("millennia"), dateTime, zoneId).makePipe().asProcessor().process(null)
            )
        );

        dateTime = l(dateTime(-12999, 9, 3, 18, 10, 37, 123456789));
        assertEquals(
            "-12900-01-01T00:00:00.000+10:00",
            DateUtils.toString(
                (ZonedDateTime) new DateTrunc(Source.EMPTY, l("centuries"), dateTime, zoneId).makePipe().asProcessor().process(null)
            )
        );

        dateTime = l(dateTime(-32999, 9, 3, 18, 10, 37, 123456789));
        assertEquals(
            "-32990-01-01T00:00:00.000+10:00",
            DateUtils.toString(
                (ZonedDateTime) new DateTrunc(Source.EMPTY, l("decades"), dateTime, zoneId).makePipe().asProcessor().process(null)
            )
        );

        dateTime = l(dateTime(-1234, 9, 3, 18, 10, 37, 123456789));
        assertEquals(
            "-1234-08-29T00:00:00.000+10:00",
            DateUtils.toString(
                (ZonedDateTime) new DateTrunc(Source.EMPTY, l("week"), dateTime, zoneId).makePipe().asProcessor().process(null)
            )
        );

        Literal yearToMonth = intervalLiteral(Period.ofYears(-12523).minusMonths(10), INTERVAL_YEAR_TO_MONTH);
        assertEquals(
            "-12000-0",
            toString(
                (IntervalYearMonth) new DateTrunc(Source.EMPTY, l("millennia"), yearToMonth, null).makePipe().asProcessor().process(null)
            )
        );

        yearToMonth = intervalLiteral(Period.ofYears(-32543).minusMonths(10), INTERVAL_YEAR_TO_MONTH);
        assertEquals(
            "-32500-0",
            toString(
                (IntervalYearMonth) new DateTrunc(Source.EMPTY, l("centuries"), yearToMonth, null).makePipe().asProcessor().process(null)
            )
        );

        yearToMonth = intervalLiteral(Period.ofYears(-24321).minusMonths(10), INTERVAL_YEAR_TO_MONTH);
        assertEquals(
            "-24320-0",
            toString(
                (IntervalYearMonth) new DateTrunc(Source.EMPTY, l("decades"), yearToMonth, null).makePipe().asProcessor().process(null)
            )
        );

        Literal dayToSecond = intervalLiteral(
            Duration.ofDays(-435).minusHours(23).minusMinutes(45).minusSeconds(55).minusNanos(123000000),
            INTERVAL_DAY_TO_SECOND
        );
        assertEquals(
            "-435 00:00:00",
            toString((IntervalDayTime) new DateTrunc(Source.EMPTY, l("days"), dayToSecond, null).makePipe().asProcessor().process(null))
        );

        dayToSecond = intervalLiteral(
            Duration.ofDays(-4231).minusHours(23).minusMinutes(45).minusSeconds(55).minusNanos(234000000),
            INTERVAL_DAY_TO_SECOND
        );
        assertEquals(
            "-4231 23:00:00",
            toString((IntervalDayTime) new DateTrunc(Source.EMPTY, l("hh"), dayToSecond, null).makePipe().asProcessor().process(null))
        );

        dayToSecond = intervalLiteral(
            Duration.ofDays(-124).minusHours(0).minusMinutes(59).minusSeconds(11).minusNanos(564000000),
            INTERVAL_DAY_TO_SECOND
        );
        assertEquals(
            "-124 00:59:00",
            toString((IntervalDayTime) new DateTrunc(Source.EMPTY, l("mi"), dayToSecond, null).makePipe().asProcessor().process(null))
        );

        dayToSecond = intervalLiteral(
            Duration.ofDays(-534).minusHours(23).minusMinutes(59).minusSeconds(59).minusNanos(245000000),
            INTERVAL_DAY_TO_SECOND
        );
        assertEquals(
            "-534 23:59:59",
            toString((IntervalDayTime) new DateTrunc(Source.EMPTY, l("seconds"), dayToSecond, null).makePipe().asProcessor().process(null))
        );

        dayToSecond = intervalLiteral(
            Duration.ofDays(-127).minusHours(17).minusMinutes(59).minusSeconds(59).minusNanos(987654321),
            INTERVAL_DAY_TO_SECOND
        );
        assertEquals(
            "-127 17:59:59.987",
            toString((IntervalDayTime) new DateTrunc(Source.EMPTY, l("ms"), dayToSecond, null).makePipe().asProcessor().process(null))
        );
    }

    private String toString(IntervalYearMonth intervalYearMonth) {
        return StringUtils.toString(intervalYearMonth);
    }

    private String toString(IntervalDayTime intervalDayTime) {
        return StringUtils.toString(intervalDayTime);
    }

    private String toString(ZonedDateTime dateTime) {
        return ISO_DATETIME_WITH_NANOS.format(dateTime);
    }

    private static Literal intervalLiteral(TemporalAmount value, DataType intervalType) {
        Object interval = value instanceof Period
            ? new IntervalYearMonth((Period) value, intervalType)
            : new IntervalDayTime((Duration) value, intervalType);
        return new Literal(EMPTY, interval, SqlDataTypes.fromJava(interval));
    }
}
