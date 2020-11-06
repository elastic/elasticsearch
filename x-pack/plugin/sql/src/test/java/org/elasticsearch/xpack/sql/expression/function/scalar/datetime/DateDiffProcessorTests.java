/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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

import static org.elasticsearch.xpack.ql.expression.Literal.NULL;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomDatetimeLiteral;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.dateTime;
import static org.elasticsearch.xpack.sql.util.DateUtils.UTC;

public class DateDiffProcessorTests extends AbstractSqlWireSerializingTestCase<DateDiffProcessor> {

    public static DateDiffProcessor randomDateDiffProcessor() {
        return new DateDiffProcessor(
            new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(0, 128)),
            new ConstantProcessor(DateTimeTestUtils.nowWithMillisResolution()),
            new ConstantProcessor(DateTimeTestUtils.nowWithMillisResolution()),
            randomZone());
    }

    @Override
    protected DateDiffProcessor createTestInstance() {
        return randomDateDiffProcessor();
    }

    @Override
    protected Reader<DateDiffProcessor> instanceReader() {
        return DateDiffProcessor::new;
    }

    @Override
    protected ZoneId instanceZoneId(DateDiffProcessor instance) {
        return instance.zoneId();
    }

    @Override
    protected DateDiffProcessor mutateInstance(DateDiffProcessor instance) {
        return new DateDiffProcessor(
            new ConstantProcessor(ESTestCase.randomRealisticUnicodeOfLength(128)),
            new ConstantProcessor(DateTimeTestUtils.nowWithMillisResolution()),
            new ConstantProcessor(DateTimeTestUtils.nowWithMillisResolution()),
            randomValueOtherThan(instance.zoneId(), ESTestCase::randomZone));
    }

    public void testInvalidInputs() {
        SqlIllegalArgumentException siae = expectThrows(SqlIllegalArgumentException.class,
                () -> new DateDiff(Source.EMPTY, l(5),
                    randomDatetimeLiteral(), randomDatetimeLiteral(), randomZone()).makePipe().asProcessor().process(null));
        assertEquals("A string is required; received [5]", siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DateDiff(Source.EMPTY,
                l("days"), l("foo"), randomDatetimeLiteral(), randomZone()).makePipe().asProcessor().process(null));
        assertEquals("A date/datetime is required; received [foo]", siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DateDiff(Source.EMPTY,
                l("days"), randomDatetimeLiteral(), l("foo"), randomZone()).makePipe().asProcessor().process(null));
        assertEquals("A date/datetime is required; received [foo]", siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DateDiff(Source.EMPTY, l("invalid"),
                randomDatetimeLiteral(), randomDatetimeLiteral(), randomZone()).makePipe().asProcessor().process(null));
        assertEquals("A value of [YEAR, QUARTER, MONTH, DAYOFYEAR, DAY, WEEK, WEEKDAY, HOUR, MINUTE, " +
            "SECOND, MILLISECOND, MICROSECOND, NANOSECOND] or their aliases is required; received [invalid]",
            siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DateDiff(Source.EMPTY, l("quertar"),
                randomDatetimeLiteral(), randomDatetimeLiteral(), randomZone()).makePipe().asProcessor().process(null));
        assertEquals("Received value [quertar] is not valid date part to add; did you mean [quarter, quarters]?",
             siae.getMessage());
    }

    public void testWithNulls() {
        assertNull(new DateDiff(Source.EMPTY,
            NULL, randomDatetimeLiteral(), randomDatetimeLiteral(), randomZone()).makePipe().asProcessor().process(null));
        assertNull(new DateDiff(Source.EMPTY,
            l("days"), NULL, randomDatetimeLiteral(), randomZone()).makePipe().asProcessor().process(null));
        assertNull(new DateDiff(Source.EMPTY,
            l("days"), randomDatetimeLiteral(), NULL, randomZone()).makePipe().asProcessor().process(null));
        assertNull(new DateDiff(Source.EMPTY,
            NULL, NULL, NULL, randomZone()).makePipe().asProcessor().process(null));
    }

    public void testDiff() {
        ZoneId zoneId = ZoneId.of("Etc/GMT-10");

        Literal dt1 = l(dateTime(2019, 12, 31, 20, 22, 33, 987654321, ZoneId.of("Etc/GMT+5")));
        Literal dt2 = l(dateTime(2022, 1, 1, 4, 33, 22, 123456789, ZoneId.of("Etc/GMT-5")));

        assertEquals(1, new DateDiff(Source.EMPTY, l("years"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-1, new DateDiff(Source.EMPTY, l("year"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(2, new DateDiff(Source.EMPTY, l("yyyy"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(-2, new DateDiff(Source.EMPTY, l("yy"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        assertEquals(7, new DateDiff(Source.EMPTY, l("quarter"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-7, new DateDiff(Source.EMPTY, l("qq"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(8, new DateDiff(Source.EMPTY, l("quarter"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(-8, new DateDiff(Source.EMPTY, l("qq"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        assertEquals(23, new DateDiff(Source.EMPTY, l("month"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-23, new DateDiff(Source.EMPTY, l("months"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(24, new DateDiff(Source.EMPTY, l("mm"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(-24, new DateDiff(Source.EMPTY, l("m"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        assertEquals(730, new DateDiff(Source.EMPTY, l("dayofyear"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-730, new DateDiff(Source.EMPTY, l("dy"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(730, new DateDiff(Source.EMPTY, l("y"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(-730, new DateDiff(Source.EMPTY, l("y"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        assertEquals(730, new DateDiff(Source.EMPTY, l("day"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-730, new DateDiff(Source.EMPTY, l("days"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(730, new DateDiff(Source.EMPTY, l("dd"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(-730, new DateDiff(Source.EMPTY, l("dd"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        assertEquals(104, new DateDiff(Source.EMPTY, l("week"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-104, new DateDiff(Source.EMPTY, l("weeks"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(104, new DateDiff(Source.EMPTY, l("wk"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(-104, new DateDiff(Source.EMPTY, l("ww"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        assertEquals(730, new DateDiff(Source.EMPTY, l("weekday"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-730, new DateDiff(Source.EMPTY, l("weekdays"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(730, new DateDiff(Source.EMPTY, l("dw"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(-730, new DateDiff(Source.EMPTY, l("dw"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        assertEquals(17542, new DateDiff(Source.EMPTY, l("hour"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-17542, new DateDiff(Source.EMPTY, l("hours"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(17542, new DateDiff(Source.EMPTY, l("hh"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(-17542, new DateDiff(Source.EMPTY, l("hh"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        assertEquals(1052531, new DateDiff(Source.EMPTY, l("minute"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-1052531, new DateDiff(Source.EMPTY, l("minutes"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(1052531, new DateDiff(Source.EMPTY, l("mi"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(-1052531, new DateDiff(Source.EMPTY, l("n"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        dt1 = l(dateTime(2020, 12, 31, 20, 22, 33, 123456789, ZoneId.of("Etc/GMT+5")));
        dt2 = l(dateTime(2021, 1, 1, 10, 33, 22, 987654321, ZoneId.of("Etc/GMT-5")));

        assertEquals(15049, new DateDiff(Source.EMPTY, l("second"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-15049, new DateDiff(Source.EMPTY, l("seconds"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(15049, new DateDiff(Source.EMPTY, l("ss"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(-15049, new DateDiff(Source.EMPTY, l("s"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        assertEquals(15049864, new DateDiff(Source.EMPTY, l("millisecond"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-15049864, new DateDiff(Source.EMPTY, l("milliseconds"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(15049864, new DateDiff(Source.EMPTY, l("ms"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(-15049864, new DateDiff(Source.EMPTY, l("ms"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        dt1 = l(dateTime(2020, 12, 31, 20, 22, 33, 123456789, ZoneId.of("Etc/GMT+5")));
        dt2 = l(dateTime(2021, 1, 1, 6, 33, 22, 987654321, ZoneId.of("Etc/GMT-5")));

        assertEquals(649864198, new DateDiff(Source.EMPTY, l("microsecond"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-649864198, new DateDiff(Source.EMPTY, l("microseconds"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(649864198, new DateDiff(Source.EMPTY, l("mcs"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(-649864198, new DateDiff(Source.EMPTY, l("mcs"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        dt1 = l(dateTime(2020, 12, 31, 20, 33, 22, 123456789, ZoneId.of("Etc/GMT+5")));
        dt2 = l(dateTime(2021, 1, 1, 6, 33, 23, 987654321, ZoneId.of("Etc/GMT-5")));

        assertEquals(1864197532, new DateDiff(Source.EMPTY, l("nanosecond"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-1864197532, new DateDiff(Source.EMPTY, l("nanoseconds"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(1864197532, new DateDiff(Source.EMPTY, l("ns"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(-1864197532, new DateDiff(Source.EMPTY, l("ns"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));
    }

    public void testDiffEdgeCases() {
        ZoneId zoneId = ZoneId.of("Etc/GMT-10");

        Literal dt1 = l(dateTime(2010, 12, 31, 18, 0, 0, 0));
        Literal dt2 = l(dateTime(2019, 1, 1, 18, 0, 0, 0));

        assertEquals(9, new DateDiff(Source.EMPTY, l("years"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-9, new DateDiff(Source.EMPTY, l("year"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(8, new DateDiff(Source.EMPTY, l("yyyy"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(-8, new DateDiff(Source.EMPTY, l("yy"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        assertEquals(33, new DateDiff(Source.EMPTY, l("quarter"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-33, new DateDiff(Source.EMPTY, l("qq"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(32, new DateDiff(Source.EMPTY, l("quarter"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(-32, new DateDiff(Source.EMPTY, l("qq"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        assertEquals(97, new DateDiff(Source.EMPTY, l("month"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-97, new DateDiff(Source.EMPTY, l("months"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(96, new DateDiff(Source.EMPTY, l("mm"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(-96, new DateDiff(Source.EMPTY, l("m"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        dt1 = l(dateTime(1976, 9, 9, 0, 0, 0, 0));
        dt2 = l(dateTime(1983, 5, 22, 0, 0, 0, 0));
        assertEquals(350, new DateDiff(Source.EMPTY, l("week"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-350, new DateDiff(Source.EMPTY, l("weeks"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(350, new DateDiff(Source.EMPTY, l("wk"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(-350, new DateDiff(Source.EMPTY, l("ww"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        dt1 = l(dateTime(1988, 1, 2, 0, 0, 0, 0));
        dt2 = l(dateTime(1987, 12, 29, 0, 0, 0, 0));
        assertEquals(0, new DateDiff(Source.EMPTY, l("week"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(0, new DateDiff(Source.EMPTY, l("weeks"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(0, new DateDiff(Source.EMPTY, l("wk"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(0, new DateDiff(Source.EMPTY, l("ww"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        dt1 = l(dateTime(1988, 1, 5, 0, 0, 0, 0));
        dt2 = l(dateTime(1996, 5, 13, 0, 0, 0, 0));
        assertEquals(436, new DateDiff(Source.EMPTY, l("week"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-436, new DateDiff(Source.EMPTY, l("weeks"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(436, new DateDiff(Source.EMPTY, l("wk"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(-436, new DateDiff(Source.EMPTY, l("ww"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        dt1 = l(dateTime(1999, 8, 20, 0, 0, 0, 0));
        dt2 = l(dateTime(1974, 3, 17, 0, 0, 0, 0));
        assertEquals(-1326, new DateDiff(Source.EMPTY, l("week"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(1326, new DateDiff(Source.EMPTY, l("weeks"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-1326, new DateDiff(Source.EMPTY, l("wk"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(1326, new DateDiff(Source.EMPTY, l("ww"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        dt1 = l(dateTime(1997, 2, 2, 0, 0, 0, 0));
        dt2 = l(dateTime(1997, 9, 19, 0, 0, 0, 0));
        assertEquals(32, new DateDiff(Source.EMPTY, l("week"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-32, new DateDiff(Source.EMPTY, l("weeks"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(32, new DateDiff(Source.EMPTY, l("wk"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(-32, new DateDiff(Source.EMPTY, l("ww"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        dt1 = l(dateTime(1980, 11, 7, 0, 0, 0, 0));
        dt2 = l(dateTime(1979, 4, 1, 0, 0, 0, 0));
        assertEquals(-83, new DateDiff(Source.EMPTY, l("week"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(83, new DateDiff(Source.EMPTY, l("weeks"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-83, new DateDiff(Source.EMPTY, l("wk"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(83, new DateDiff(Source.EMPTY, l("ww"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        dt1 = l(dateTime(1997, 9, 19, 0, 0, 0, 0));
        dt2 = l(dateTime(2004, 8, 2, 7, 59, 23, 0));
        assertEquals(60223, new DateDiff(Source.EMPTY, l("hour"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-60223, new DateDiff(Source.EMPTY, l("hours"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(60223, new DateDiff(Source.EMPTY, l("hh"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(-60223, new DateDiff(Source.EMPTY, l("hh"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        dt1 = l(dateTime(1997, 9, 19, 0, 0, 0, 0));
        dt2 = l(dateTime(2004, 8, 2, 7, 59, 59, 999999999));
        assertEquals(60223, new DateDiff(Source.EMPTY, l("hour"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-60223, new DateDiff(Source.EMPTY, l("hours"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(60223, new DateDiff(Source.EMPTY, l("hh"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(-60223, new DateDiff(Source.EMPTY, l("hh"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        dt1 = l(dateTime(2002, 4, 27, 0, 0, 0, 0));
        dt2 = l(dateTime(2004, 7, 28, 12, 34, 28, 0));
        assertEquals(1185874, new DateDiff(Source.EMPTY, l("minute"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-1185874, new DateDiff(Source.EMPTY, l("minutes"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(1185874, new DateDiff(Source.EMPTY, l("mi"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(-1185874, new DateDiff(Source.EMPTY, l("n"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        dt1 = l(dateTime(1995, 9, 3, 0, 0, 0, 0));
        dt2 = l(dateTime(2004, 7, 26, 12, 30, 34, 0));
        assertEquals(4679310, new DateDiff(Source.EMPTY, l("minute"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-4679310, new DateDiff(Source.EMPTY, l("minutes"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(4679310, new DateDiff(Source.EMPTY, l("mi"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(-4679310, new DateDiff(Source.EMPTY, l("n"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));

        dt1 = l(dateTime(1997, 5, 30, 0, 0, 0, 0));
        dt2 = l(dateTime(2004, 7, 28, 23, 30, 59, 999999999));
        assertEquals(3768450, new DateDiff(Source.EMPTY, l("minute"), dt1, dt2, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(-3768450, new DateDiff(Source.EMPTY, l("minutes"), dt2, dt1, UTC)
            .makePipe().asProcessor().process(null));
        assertEquals(3768450, new DateDiff(Source.EMPTY, l("mi"), dt1, dt2, zoneId)
            .makePipe().asProcessor().process(null));
        assertEquals(-3768450, new DateDiff(Source.EMPTY, l("n"), dt2, dt1, zoneId)
            .makePipe().asProcessor().process(null));
    }

    public void testOverflow() {
        ZoneId zoneId = ZoneId.of("Etc/GMT-10");
        Literal dt1 = l(dateTime(-99992022, 12, 31, 20, 22, 33, 123456789, ZoneId.of("Etc/GMT-5")));
        Literal dt2 = l(dateTime(99992022, 4, 18, 8, 33, 22, 987654321, ZoneId.of("Etc/GMT+5")));

        SqlIllegalArgumentException siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DateDiff(Source.EMPTY, l("month"), dt1, dt2, zoneId).makePipe().asProcessor().process(null));
        assertEquals("The DATE_DIFF function resulted in an overflow; the number of units separating two date/datetime " +
                "instances is too large. Try to use DATE_DIFF with a less precise unit.",
            siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DateDiff(Source.EMPTY, l("dayofyear"), dt1, dt2, zoneId).makePipe().asProcessor().process(null));
        assertEquals("The DATE_DIFF function resulted in an overflow; the number of units separating two date/datetime " +
                "instances is too large. Try to use DATE_DIFF with a less precise unit.",
            siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DateDiff(Source.EMPTY, l("day"), dt1, dt2, zoneId).makePipe().asProcessor().process(null));
        assertEquals("The DATE_DIFF function resulted in an overflow; the number of units separating two date/datetime " +
                "instances is too large. Try to use DATE_DIFF with a less precise unit.",
            siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DateDiff(Source.EMPTY, l("week"), dt1, dt2, zoneId).makePipe().asProcessor().process(null));
        assertEquals("The DATE_DIFF function resulted in an overflow; the number of units separating two date/datetime " +
                "instances is too large. Try to use DATE_DIFF with a less precise unit.",
            siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DateDiff(Source.EMPTY, l("weekday"), dt1, dt2, zoneId).makePipe().asProcessor().process(null));
        assertEquals("The DATE_DIFF function resulted in an overflow; the number of units separating two date/datetime " +
                "instances is too large. Try to use DATE_DIFF with a less precise unit.",
            siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DateDiff(Source.EMPTY, l("hours"), dt1, dt2, zoneId).makePipe().asProcessor().process(null));
        assertEquals("The DATE_DIFF function resulted in an overflow; the number of units separating two date/datetime " +
                "instances is too large. Try to use DATE_DIFF with a less precise unit.",
            siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DateDiff(Source.EMPTY, l("minute"), dt1, dt2, zoneId).makePipe().asProcessor().process(null));
        assertEquals("The DATE_DIFF function resulted in an overflow; the number of units separating two date/datetime " +
                "instances is too large. Try to use DATE_DIFF with a less precise unit.",
            siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DateDiff(Source.EMPTY, l("second"), dt1, dt2, zoneId).makePipe().asProcessor().process(null));
        assertEquals("The DATE_DIFF function resulted in an overflow; the number of units separating two date/datetime " +
                "instances is too large. Try to use DATE_DIFF with a less precise unit.",
            siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DateDiff(Source.EMPTY, l("milliseconds"), dt2, dt1, zoneId).makePipe().asProcessor().process(null));
        assertEquals("The DATE_DIFF function resulted in an overflow; the number of units separating two date/datetime " +
                "instances is too large. Try to use DATE_DIFF with a less precise unit.",
            siae.getMessage());

         siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DateDiff(Source.EMPTY, l("mcs"), dt1, dt2, zoneId).makePipe().asProcessor().process(null));
        assertEquals("The DATE_DIFF function resulted in an overflow; the number of units separating two date/datetime " +
                "instances is too large. Try to use DATE_DIFF with a less precise unit.",
            siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DateDiff(Source.EMPTY, l("nanoseconds"), dt2, dt1, zoneId).makePipe().asProcessor().process(null));
        assertEquals("The DATE_DIFF function resulted in an overflow; the number of units separating two date/datetime " +
                "instances is too large. Try to use DATE_DIFF with a less precise unit.",
            siae.getMessage());
    }
}
