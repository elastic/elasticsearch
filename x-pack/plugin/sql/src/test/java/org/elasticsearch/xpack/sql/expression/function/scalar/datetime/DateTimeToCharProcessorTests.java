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
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeFormatProcessor.Formatter;

import java.time.Instant;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.elasticsearch.xpack.ql.expression.Literal.NULL;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.*;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.*;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.TIME;
import static org.elasticsearch.xpack.sql.util.DateUtils.UTC;

public class DateTimeToCharProcessorTests extends AbstractSqlWireSerializingTestCase<DateTimeFormatProcessor> {

    public static DateTimeFormatProcessor randomDateTimeFormatProcessor() {
        return new DateTimeFormatProcessor(
            new ConstantProcessor(DateTimeTestUtils.nowWithMillisResolution()),
            new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(0, 128)),
            randomZone(),
            randomFrom(Formatter.values())
        );
    }

    public static Literal randomTimeLiteral() {
        return l(OffsetTime.ofInstant(Instant.ofEpochMilli(ESTestCase.randomLong()), ESTestCase.randomZone()), TIME);
    }

    @Override
    protected DateTimeFormatProcessor createTestInstance() {
        return randomDateTimeFormatProcessor();
    }

    @Override
    protected Reader<DateTimeFormatProcessor> instanceReader() {
        return DateTimeFormatProcessor::new;
    }

    @Override
    protected ZoneId instanceZoneId(DateTimeFormatProcessor instance) {
        return instance.zoneId();
    }

    @Override
    protected DateTimeFormatProcessor mutateInstance(DateTimeFormatProcessor instance) {
        return new DateTimeFormatProcessor(
            new ConstantProcessor(DateTimeTestUtils.nowWithMillisResolution()),
            new ConstantProcessor(ESTestCase.randomRealisticUnicodeOfLength(128)),
            randomZone(),
            randomValueOtherThan(instance.formatter(), () -> randomFrom(Formatter.values()))
        );
    }

    public void testSeconds() {
        assertThat(dateTime(2019, 9, 3, 18, 10, 37, 1))
            .withFormat("US")
            .withZoneId("Etc/GMT-10")
            .isFormattedTo("000001");
        assertThat(dateTime(2019, 9, 3, 18, 10, 37, 12))
            .withFormat("US")
            .withZoneId("Etc/GMT-10")
            .isFormattedTo("000012");
        assertThat(dateTime(2019, 9, 3, 18, 10, 37, 123))
            .withFormat("US")
            .withZoneId("Etc/GMT-10")
            .isFormattedTo("000123");
        assertThat(dateTime(2019, 9, 3, 18, 10, 37, 1234))
            .withFormat("US")
            .withZoneId("Etc/GMT-10")
            .isFormattedTo("001234");
        assertThat(dateTime(2019, 9, 3, 18, 10, 37, 12345))
            .withFormat("US")
            .withZoneId("Etc/GMT-10")
            .isFormattedTo("012345");
        assertThat(dateTime(2019, 9, 3, 18, 10, 37, 123456789))
            .withFormat("US")
            .withZoneId("Etc/GMT-10")
            .isFormattedTo("123456");
        assertThat(dateTime(2019, 9, 3, 18, 10, 37, 123456789))
            .withFormat("ss.MS")
            .withZoneId("Etc/GMT-10")
            .isFormattedTo("37.123");
        assertThat(dateTime(2019, 9, 3, 18, 10, 1, 0))
            .withFormat("ss.MS")
            .isFormattedTo("01.000");
        assertThat(dateTime(2019, 9, 3, 18, 10, 37, 123456789))
            .withFormat("ss.n")
            .withZoneId("Etc/GMT-10")
            .isFormattedTo("37.123456789");
        assertThat(time(21, 11, 2, 123456789))
            .withFormat("s-ss")
            .isFormattedTo("2-02");
    }

    public void testSecondOfDay() {
        assertThat(dateTime(2020, 10, 13, 13, 26, 41, 0))
            .withFormat("SSSS")
            .isFormattedTo("48401");
    }

    public void testMinutes() {
        assertThat(dateTime(2019, 9, 3, 18, 0, 37, 123456789))
            .withFormat("mm MI")
            .isFormattedTo("00 00");

        assertThat(dateTime(2019, 9, 3, 18, 10, 37, 123456789))
            .withFormat("mm MI")
            .isFormattedTo("10 10");

        assertThat(dateTime(2019, 9, 3, 18, 59, 37, 123456789))
            .withFormat("mm MI")
            .isFormattedTo("59 59");
    }

    public void testHours() {
        var dateTime = dateTime(2019, 9, 3, 16, 10, 37, 123456789);
        assertThat(dateTime)
            .withFormat("HH")
            .isFormattedTo("04");
        assertThat(dateTime)
            .withFormat("HH12")
            .isFormattedTo("04");
        assertThat(dateTime)
            .withFormat("HH24")
            .isFormattedTo("16");
        assertThat(dateTime)
            .withFormat("HH24")
            .withZoneId("Etc/GMT-10")
            .isFormattedTo("02");
    }

    public void testMonths() {
        assertThat(date(2019, 9, 3, UTC))
            .withFormat("M-MM-MMM-MMMM")
            .isFormattedTo("9-09-Sep-September");
        assertThat(date(2019, 9, 3, UTC))
            .withFormat("Month MONTH month")
            .isFormattedTo("September SEPTEMBER september");
        assertThat(date(2019, 9, 3, UTC))
            .withFormat("Mon MON mon")
            .isFormattedTo("Sep SEP sep");
        assertThat(date(2019, 9, 3, UTC))
            .withFormat("%M-\"MM-\\MMM-MMMM")
            .isFormattedTo("%9-\"09-\\Sep-September");
    }

    public void testRomeMonths() {
        assertThat(date(2001, 1, 3, UTC))
            .withFormat("RM rm")
            .isFormattedTo("I i");
        assertThat(date(2001, 2, 3, UTC))
            .withFormat("RM rm")
            .isFormattedTo("II ii");
        assertThat(date(2001, 3, 3, UTC))
            .withFormat("RM rm")
            .isFormattedTo("III iii");
        assertThat(date(2001, 4, 3, UTC))
            .withFormat("RM rm")
            .isFormattedTo("IV iv");
        assertThat(date(2001, 5, 3, UTC))
            .withFormat("RM rm")
            .isFormattedTo("V v");
        assertThat(date(2001, 6, 3, UTC))
            .withFormat("RM rm")
            .isFormattedTo("VI vi");
        assertThat(date(2001, 7, 3, UTC))
            .withFormat("RM rm")
            .isFormattedTo("VII vii");
        assertThat(date(2001, 8, 3, UTC))
            .withFormat("RM rm")
            .isFormattedTo("VIII viii");
        assertThat(date(2001, 9, 3, UTC))
            .withFormat("RM rm")
            .isFormattedTo("IX ix");
        assertThat(date(2001, 10, 3, UTC))
            .withFormat("RM rm")
            .isFormattedTo("X x");
        assertThat(date(2001, 11, 3, UTC))
            .withFormat("RM rm")
            .isFormattedTo("XI xi");
        assertThat(date(2001, 12, 3, UTC))
            .withFormat("RM rm")
            .isFormattedTo("XII xii");
    }

    public void testYearOfEra() {
        assertThat(date(2001, 9, 3, UTC))
            .withFormat("y-yy-yyyy-yyyyy")
            .isFormattedTo("2001-01-2001-02001");

        assertThat(dateTime(45, 9, 3, 18, 10, 37, 123456789))
            .withFormat("y-yyyy")
            .isFormattedTo("45-0045");

        assertThat(dateTime(2017, 1, 1, 1, 1, 1, 0))
            .withFormat("Y,YYY Y YY YYY YYYY")
            .isFormattedTo("2,017 7 17 017 2017");
    }

    public void testWeekBasedYear() {
        assertThat(date(2017, 1, 1, UTC))
            .withFormat("IYYY IYY IY I")
            .isFormattedTo("2016 016 16 6");
    }

    public void testDayOfWeek() {
        var zoneId = ZoneId.of("Etc/GMT-10");
        var dateTime = dateTime(2019, 9, 3, 18, 10, 37, 123456789);
        assertThat(dateTime)
            .withFormat("Dy DY dy")
            .withZoneId(zoneId)
            .isFormattedTo("Wed WED wed");

        assertThat(dateTime)
            .withFormat("Day DAY day")
            .withZoneId(zoneId)
            .isFormattedTo("Wednesday WEDNESDAY wednesday");

        assertThat(date(2019, 9, 3, UTC))
            .withFormat("ID D Day")
            .isFormattedTo("2 3 Tuesday");

        assertThat(date(2019, 9, 8, UTC))
            .withFormat("ID D")
            .isFormattedTo("7 1");

        assertThat(date(2019, 9, 9, UTC))
            .withFormat("ID D")
            .isFormattedTo("1 2");
    }

    public void testDayOfMonth() {
        assertThat(date(2019, 9, 23, UTC))
            .withFormat("d")
            .isFormattedTo("23");

        assertThat(date(2019, 9, 3, UTC))
            .withFormat("d")
            .isFormattedTo("3");
    }

    public void testDayOfYear() {
        assertThat(date(2018, 9, 23, UTC))
            .withFormat("DDD")
            .isFormattedTo("266");

        assertThat(date(2018, 1, 1, UTC))
            .withFormat("DDD")
            .isFormattedTo("001");

        assertThat(date(2019, 9, 23, UTC))
            .withFormat("DDD")
            .isFormattedTo("266");

        assertThat(date(2019, 12, 31, UTC))
            .withFormat("DDD")
            .isFormattedTo("365");

        assertThat(date(2020, 12, 31, UTC))
            .withFormat("DDD")
            .isFormattedTo("366");
    }

    public void testWeekNumber() {
        assertThat(date(2020, 1, 1, UTC))
            .withFormat("W IW")
            .isFormattedTo("1 01");

//        assertThat(dateTime(2018, 6, 8, 12, 0, 0, 0))
//            .withFormat("W IW")
//            .isFormattedTo("2 23");
    }

    public void testEra() {
        assertThat(date(2019, 9, 3, UTC))
            .withFormat("G AD A.D. a.d. ad")
            .isFormattedTo("AD AD A.D. a.d. ad");

        assertThat(date(-2019, 9, 3, UTC))
            .withFormat("G BC A.D. a.d. ad")
            .isFormattedTo("BC BC B.C. b.c. bc");

        assertThat(date(-2019, 9, 3, UTC))
            .withFormat("G BC B.C. b.c. bc")
            .isFormattedTo("BC BC B.C. b.c. bc");

        assertThat(date(2019, 9, 3, UTC))
            .withFormat("G AD A.D. a.d. ad")
            .isFormattedTo("AD AD A.D. a.d. ad");
    }

    public void testQuarterOfYear() {
        for (int month = 0; month < 12; month++) {
            assertThat(date(2019, month + 1, 3, UTC))
                .withFormat("Q")
                .isFormattedTo(String.valueOf((month / 3) + 1));
        }
    }

    public void testJulianDays() {
        assertThat(dateTime(2017, 1, 1, 1, 1, 1, 0))
            .withFormat("J")
            .isFormattedTo("2457755");

        assertThat(dateTime(500, 4, 11, 23, 59, 0, 0))
            .withFormat("J")
            .isFormattedTo("1903782");
    }

    public void testCentury() {
        assertThat(date(2001, 1, 1, UTC))
            .withFormat("CC")
            .isFormattedTo("21");

        assertThat(date(2000, 1, 1, UTC))
            .withFormat("CC")
            .isFormattedTo("20");

        assertThat(date(1, 1, 1, UTC))
            .withFormat("CC")
            .isFormattedTo("01");

        assertThat(date(0, 1, 1, UTC))
            .withFormat("CC")
            .isFormattedTo("-01");

        assertThat(date(-1, 1, 1, UTC))
            .withFormat("CC")
            .isFormattedTo("-01");

        assertThat(date(-101, 1, 1, UTC))
            .withFormat("CC")
            .isFormattedTo("-02");

        assertThat(date(-2000, 1, 1, UTC))
            .withFormat("CC")
            .isFormattedTo("-21");

        assertThat(date(-2001, 1, 1, UTC))
            .withFormat("CC")
            .isFormattedTo("-21");
    }

    public void testAmPmOfDay() {
        var zoneId = ZoneId.of("America/Sao_Paulo");
        var dateTime = dateTime(2019, 9, 3, 18, 10, 37, 123456789);

        assertThat(dateTime(2034, 9, 3, 9, 10, 37, 123456789))
            .withFormat("am a.m. AM A.M.")
            .withZoneId(zoneId)
            .isFormattedTo("am a.m. AM A.M.");
        assertThat(dateTime(2034, 9, 3, 18, 10, 37, 123456789))
            .withFormat("am a.m. AM A.M.")
            .withZoneId(zoneId)
            .isFormattedTo("pm p.m. PM P.M.");

        assertThat(dateTime)
            .withFormat("'arr:' h:m am")
            .withZoneId(zoneId)
            .isFormattedTo("arr: 3:10 pm");
    }

    public void testDate() {
        assertThat(dateTime(2019, 9, 3, 18, 10, 37, 123456789))
            .withFormat("YYYY-MM-dd")
            .withZoneId("Etc/GMT-10")
            .isFormattedTo("2019-09-04");
    }

    public void testWithNulls() {
        assertThat(randomDatetimeLiteral())
            .withFormat(NULL)
            .withZoneId(randomZone())
            .isNull();
        assertThat(randomDatetimeLiteral())
            .withFormat(l(""))
            .withZoneId(randomZone())
            .isNull();
        assertThat(NULL)
            .withFormat(randomStringLiteral())
            .withZoneId(randomZone())
            .isNull();
    }

    public void testFormatInvalidInputs() {
        assertThat(l("foo"))
            .withFormat(randomStringLiteral())
            .withZoneId(randomZone())
            .throwsWithMessage(SqlIllegalArgumentException.class, "A date/datetime/time is required; received [foo]");
        assertThat(randomDatetimeLiteral())
            .withFormat(l(5))
            .withZoneId(randomZone())
            .throwsWithMessage(SqlIllegalArgumentException.class, "A string is required; received [5]");
        assertThat(dateTime(2019, 9, 3, 18, 10, 37, 0))
            .withFormat("invalid")
            .withZoneId(randomZone())
            .throwsWithMessage(SqlIllegalArgumentException.class, "Invalid pattern [invalid] is received for formatting date/time [2019-09-03T18:10:37Z]; Unknown pattern letter: i");
        assertThat(time(18, 10, 37, 123000000))
            .withFormat("MM/dd")
            .withZoneId(randomZone())
            .throwsWithMessage(SqlIllegalArgumentException.class, "Invalid pattern [MM/dd] is received for formatting date/time [18:10:37.123Z]; Unsupported field: MonthOfYear");
    }

    private ToCharAssert assertThat(ZonedDateTime zonedDateTime) {
        return assertThat(l(zonedDateTime));
    }

    private ToCharAssert assertThat(OffsetTime offsetTime) {
        return assertThat(l(offsetTime, TIME));
    }

    private ToCharAssert assertThat(Literal literal) {
        return new ToCharAssert(literal);
    }

    static class ToCharAssert {

        private final Literal time;
        private Literal format;
        private ZoneId zoneId = UTC;

        public ToCharAssert(Literal time) {
            this.time = time;
        }

        ToCharAssert withFormat(Literal format) {
            this.format = format;
            return this;
        }

        ToCharAssert withFormat(String format) {
            this.format = l(format);
            return this;
        }

        ToCharAssert withZoneId(ZoneId zoneId) {
            this.zoneId = zoneId;
            return this;
        }

        ToCharAssert withZoneId(String zoneId) {
            this.zoneId = ZoneId.of(zoneId);
            return this;
        }

        void isFormattedTo(String expectedFormat) {
            assertEquals(expectedFormat, execute());
        }

        void isNull() {
            assertNull(execute());
        }

        void throwsWithMessage(Class<? extends Throwable> exceptionClass, String expectedMessage) {
            var ex = expectThrows(exceptionClass, this::execute);
            assertEquals(expectedMessage, ex.getMessage());
        }

        private Object execute() {
            return new ToChar(Source.EMPTY, time, format, zoneId)
                .makePipe()
                .asProcessor()
                .process(null);
        }
    }
}
