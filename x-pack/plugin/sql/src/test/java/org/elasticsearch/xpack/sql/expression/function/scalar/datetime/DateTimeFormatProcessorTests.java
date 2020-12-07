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
import java.time.ZoneOffset;

import static org.elasticsearch.xpack.ql.expression.Literal.NULL;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomDatetimeLiteral;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.date;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.dateTime;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.time;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.TIME;

public class DateTimeFormatProcessorTests extends AbstractSqlWireSerializingTestCase<DateTimeFormatProcessor> {

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

    public void testDateTimeFormatInvalidInputs() {
        SqlIllegalArgumentException siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateTimeFormat(Source.EMPTY, l("foo"), randomStringLiteral(), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals("A date/datetime/time is required; received [foo]", siae.getMessage());

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateTimeFormat(Source.EMPTY, randomDatetimeLiteral(), l(5), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals("A string is required; received [5]", siae.getMessage());

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateTimeFormat(Source.EMPTY, l(dateTime(2019, 9, 3, 18, 10, 37, 0)), l("invalid"), randomZone()).makePipe()
                .asProcessor()
                .process(null)
        );
        assertEquals(
            "Invalid pattern [invalid] is received for formatting date/time [2019-09-03T18:10:37Z]; Unknown pattern letter: i",
            siae.getMessage()
        );

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateTimeFormat(Source.EMPTY, l(time(18, 10, 37, 123000000)), l("MM/dd"), randomZone()).makePipe()
                .asProcessor()
                .process(null)
        );
        assertEquals(
            "Invalid pattern [MM/dd] is received for formatting date/time [18:10:37.123Z]; Unsupported field: MonthOfYear",
            siae.getMessage()
        );
    }

    public void testFormatInvalidInputs() {
        SqlIllegalArgumentException siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new Format(Source.EMPTY, l("foo"), randomStringLiteral(), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals("A date/datetime/time is required; received [foo]", siae.getMessage());

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new Format(Source.EMPTY, randomDatetimeLiteral(), l(5), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals("A string is required; received [5]", siae.getMessage());

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new Format(Source.EMPTY, l(dateTime(2019, 9, 3, 18, 10, 37, 0)), l("invalid"), randomZone()).makePipe()
                .asProcessor()
                .process(null)
        );
        assertEquals(
            "Invalid pattern [invalid] is received for formatting date/time [2019-09-03T18:10:37Z]; Unknown pattern letter: i",
            siae.getMessage()
        );

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new Format(Source.EMPTY, l(time(18, 10, 37, 123000000)), l("MM/dd"), randomZone()).makePipe()
                .asProcessor()
                .process(null)
        );
        assertEquals(
            "Invalid pattern [MM/dd] is received for formatting date/time [18:10:37.123Z]; Unsupported field: MonthOfYear",
            siae.getMessage()
        );
    }

    public void testDateFormatInvalidInputs() {
        SqlIllegalArgumentException siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateFormat(Source.EMPTY, l("foo"), randomStringLiteral(), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals("A date/datetime/time is required; received [foo]", siae.getMessage());

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateFormat(Source.EMPTY, randomDatetimeLiteral(), l(5), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals("A string is required; received [5]", siae.getMessage());

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateFormat(Source.EMPTY, l(dateTime(2020, 12, 6, 23, 44, 37, 0)), l("invalid"), randomZone())
                .makePipe()
                .asProcessor()
                .process(null)
        );
        assertEquals("Invalid pattern [invalid] is received for formatting date/time [2020-12-06T23:44:37Z]; Unknown pattern letter: i",
            siae.getMessage()
        );

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateFormat(Source.EMPTY, l(time(23, 44, 37, 123000000)), l("MM/dd"), randomZone()).makePipe()
                .asProcessor()
                .process(null)
        );
        assertEquals(
            "Invalid pattern [MM/dd] is received for formatting date/time [23:44:37.123Z]; Unsupported field: MonthOfYear",
            siae.getMessage()
        );
    }

    public void testWithNulls() {
        assertNull(new DateTimeFormat(Source.EMPTY, randomDatetimeLiteral(), NULL, randomZone()).makePipe().asProcessor().process(null));
        assertNull(new DateTimeFormat(Source.EMPTY, randomDatetimeLiteral(), l(""), randomZone()).makePipe().asProcessor().process(null));
        assertNull(new DateTimeFormat(Source.EMPTY, NULL, randomStringLiteral(), randomZone()).makePipe().asProcessor().process(null));

        assertNull(new Format(Source.EMPTY, randomDatetimeLiteral(), NULL, randomZone()).makePipe().asProcessor().process(null));
        assertNull(new Format(Source.EMPTY, randomDatetimeLiteral(), l(""), randomZone()).makePipe().asProcessor().process(null));
        assertNull(new Format(Source.EMPTY, NULL, randomStringLiteral(), randomZone()).makePipe().asProcessor().process(null));

        assertNull(new DateFormat(Source.EMPTY, randomDatetimeLiteral(), NULL, randomZone()).makePipe().asProcessor().process(null));
        assertNull(new DateFormat(Source.EMPTY, randomDatetimeLiteral(), l(""), randomZone()).makePipe().asProcessor().process(null));
        assertNull(new DateFormat(Source.EMPTY, NULL, randomStringLiteral(), randomZone()).makePipe().asProcessor().process(null));
    }

    public void testFormatting() {
        ZoneId zoneId = ZoneId.of("Etc/GMT-10");
        Literal dateTime = l(dateTime(2019, 9, 3, 18, 10, 37, 123456789));

        assertEquals("AD : 3", new DateTimeFormat(Source.EMPTY, dateTime, l("G : Q"), zoneId).makePipe().asProcessor().process(null));
        assertEquals(
            "2019-09-04",
            new DateTimeFormat(Source.EMPTY, dateTime, l("YYYY-MM-dd"), zoneId).makePipe().asProcessor().process(null)
        );
        assertEquals(
            "04:10:37.123456",
            new DateTimeFormat(Source.EMPTY, dateTime, l("HH:mm:ss.SSSSSS"), zoneId).makePipe().asProcessor().process(null)
        );
        assertEquals(
            "2019-09-04 04:10:37.12345678",
            new DateTimeFormat(Source.EMPTY, dateTime, l("YYYY-MM-dd HH:mm:ss.SSSSSSSS"), zoneId).makePipe().asProcessor().process(null)
        );
        assertEquals("+1000", new DateTimeFormat(Source.EMPTY, dateTime, l("Z"), zoneId).makePipe().asProcessor().process(null));
        assertEquals("Etc/GMT-10", new DateTimeFormat(Source.EMPTY, dateTime, l("z"), zoneId).makePipe().asProcessor().process(null));
        assertEquals("Etc/GMT-10", new DateTimeFormat(Source.EMPTY, dateTime, l("VV"), zoneId).makePipe().asProcessor().process(null));

        zoneId = ZoneId.of("America/Sao_Paulo");
        assertEquals("-0300", new DateTimeFormat(Source.EMPTY, dateTime, l("Z"), zoneId).makePipe().asProcessor().process(null));
        assertEquals("BRT", new DateTimeFormat(Source.EMPTY, dateTime, l("z"), zoneId).makePipe().asProcessor().process(null));
        assertEquals(
            "America/Sao_Paulo",
            new DateTimeFormat(Source.EMPTY, dateTime, l("VV"), zoneId).makePipe().asProcessor().process(null)
        );

        assertEquals(
            "07:11:22.1234",
            new DateTimeFormat(Source.EMPTY, l(time(10, 11, 22, 123456789), TIME), l("HH:mm:ss.SSSS"), zoneId).makePipe()
                .asProcessor()
                .process(null)
        );


        zoneId = ZoneId.of("Etc/GMT-10");
        dateTime = l(dateTime(2019, 9, 3, 18, 10, 37, 123456789));

        assertEquals("AD : 3", new Format(Source.EMPTY, dateTime, l("G : Q"), zoneId).makePipe().asProcessor().process(null));
        assertEquals("AD", new Format(Source.EMPTY, dateTime, l("g"), zoneId).makePipe().asProcessor().process(null));
        assertEquals(
            "2019-09-04",
            new Format(Source.EMPTY, dateTime, l("YYYY-MM-dd"), zoneId).makePipe().asProcessor().process(null)
        );
        assertEquals(
            "2019-09-04 Wed",
            new Format(Source.EMPTY, dateTime, l("YYYY-MM-dd ddd"), zoneId).makePipe().asProcessor().process(null)
        );
        assertEquals(
            "2019-09-04 Wednesday",
            new Format(Source.EMPTY, dateTime, l("YYYY-MM-dd dddd"), zoneId).makePipe().asProcessor().process(null)
        );
        assertEquals(
            "04:10:37.123456",
            new Format(Source.EMPTY, dateTime, l("HH:mm:ss.ffffff"), zoneId).makePipe().asProcessor().process(null)
        );
        assertEquals(
            "2019-09-04 04:10:37.12345678",
            new Format(Source.EMPTY, dateTime, l("YYYY-MM-dd HH:mm:ss.ffffffff"), zoneId).makePipe().asProcessor().process(null)
        );
        assertEquals(
            "2019-09-04 04:10:37.12345678 AM",
            new Format(Source.EMPTY, dateTime, l("YYYY-MM-dd HH:mm:ss.ffffffff tt"), zoneId).makePipe().asProcessor().process(null)
        );
        assertEquals(
            "2019-09-04 04:10:37.12345678 AM",
            new Format(Source.EMPTY, dateTime, l("YYYY-MM-dd HH:mm:ss.ffffffff t"), zoneId).makePipe().asProcessor().process(null)
        );
        assertEquals("+1000", new Format(Source.EMPTY, dateTime, l("Z"), zoneId).makePipe().asProcessor().process(null));
        assertEquals("+10", new Format(Source.EMPTY, dateTime, l("z"), zoneId).makePipe().asProcessor().process(null));
        assertEquals("Etc/GMT-10", new Format(Source.EMPTY, dateTime, l("VV"), zoneId).makePipe().asProcessor().process(null));
        assertEquals("Etc/GMT-10", new Format(Source.EMPTY, dateTime, l("K"), zoneId).makePipe().asProcessor().process(null));

        assertEquals("1", new Format(Source.EMPTY, dateTime, l("F"), zoneId).makePipe().asProcessor().process(null));
        assertEquals("12", new Format(Source.EMPTY, dateTime, l("FF"), zoneId).makePipe().asProcessor().process(null));

        zoneId = ZoneId.of("America/Sao_Paulo");
        assertEquals("-0300", new Format(Source.EMPTY, dateTime, l("Z"), zoneId).makePipe().asProcessor().process(null));
        assertEquals("-03", new Format(Source.EMPTY, dateTime, l("z"), zoneId).makePipe().asProcessor().process(null));
        assertEquals(
            "America/Sao_Paulo",
            new Format(Source.EMPTY, dateTime, l("VV"), zoneId).makePipe().asProcessor().process(null)
        );

        assertEquals(
            "07:11:22.1234",
            new Format(Source.EMPTY, l(time(10, 11, 22, 123456789), TIME), l("HH:mm:ss.ffff"), zoneId).makePipe()
                .asProcessor()
                .process(null)
        );

        assertEquals(
            "10:11",
            new Format(Source.EMPTY, l(time(10, 11, 22, 123456789), TIME), l("H:m"), ZoneOffset.UTC).makePipe()
                .asProcessor()
                .process(null)
        );

        assertEquals(
            "21:9",
            new Format(Source.EMPTY, l(time(21, 11, 22, 123456789), TIME), l("H:h"), ZoneOffset.UTC).makePipe()
                .asProcessor()
                .process(null)
        );
        assertEquals(
            "2-02",
            new Format(Source.EMPTY, l(time(21, 11, 2, 123456789), TIME), l("s-ss"), ZoneOffset.UTC).makePipe()
                .asProcessor()
                .process(null)
        );

        assertEquals("9-09-Sep-September",
            new Format(Source.EMPTY, dateTime, l("M-MM-MMM-MMMM"), zoneId).makePipe()
                .asProcessor()
                .process(null));

        assertEquals("arr: 3:10 PM",
            new Format(Source.EMPTY, dateTime, l("'arr:' h:m t"), zoneId).makePipe()
                .asProcessor()
                .process(null))
        ;
        assertEquals("-03/-0300/-03:00",
            new Format(Source.EMPTY, dateTime, l("z/zz/zzz"), zoneId).makePipe()
                .asProcessor()
                .process(null));
        assertEquals("3", new Format(Source.EMPTY, dateTime, l("d"), zoneId).makePipe().asProcessor().process(null));
        assertEquals("2001-01-2001-02001",
            new Format(Source.EMPTY, l(dateTime(2001, 9, 3, 18, 10, 37, 123456789)),
                l("y-yy-yyyy-yyyyy"), zoneId).makePipe().asProcessor().process(null));

        assertEquals("%9-\"09-\\Sep-September",
            new Format(Source.EMPTY, dateTime, l("%M-\"MM-\\MMM-MMMM"), zoneId).makePipe()
                .asProcessor()
                .process(null));

        assertEquals("45-0045",
            new Format(Source.EMPTY, l(dateTime(45, 9, 3, 18, 10, 37, 123456789)), l("y-yyyy"), zoneId).makePipe()
                .asProcessor()
                .process(null));

        zoneId = ZoneId.of("Etc/GMT-10");
        dateTime = l(dateTime(2019, 9, 3, 18, 10, 37, 123456789));
        assertEquals(
            "2019-09-04",
            new DateFormat(Source.EMPTY, dateTime, l("%Y-%m-%d"), zoneId).makePipe().asProcessor().process(null)
        );

        assertEquals(
            "04:10:37",
            new DateFormat(Source.EMPTY, dateTime, l("%H:%i:%s"), zoneId).makePipe().asProcessor().process(null)
        );

        assertEquals(
            "4th 19 Wed 04 09 Sep 247", // I dont know how to format day of month with ordinal indicator
            new DateFormat(Source.EMPTY, dateTime, l("%D %y %a %d %m %b %j"), zoneId).makePipe().asProcessor().process(null)
        );

        assertEquals(
            "04 4 04 04:10:37 AM 04:10:37 37 3",
            new DateFormat(Source.EMPTY, dateTime, l("%H %k %I %r %T %S %w"), zoneId).makePipe().asProcessor().process(null)
        );

        assertEquals(
            "2019 36",
            new DateFormat(Source.EMPTY, dateTime, l("%X %V"), zoneId).makePipe().asProcessor().process(null)
        );

        assertEquals(
            "04",
            new DateFormat(Source.EMPTY, dateTime, l("%d"), zoneId).makePipe().asProcessor().process(null)
        );

        zoneId = ZoneId.of("Etc/GMT");
        dateTime = l(dateTime(2009, 10, 4, 22, 23, 0, 123456789));
        assertEquals(
            "Sunday October 2009",
            new DateFormat(Source.EMPTY, dateTime, l("%W %M %Y"), zoneId).makePipe().asProcessor().process(null)
        );

        dateTime = l(dateTime(2007, 10, 4, 22, 23, 0, 123456789));
        assertEquals(
            "22:23:00",
            new DateFormat(Source.EMPTY, dateTime, l("%H:%i:%s"), zoneId).makePipe().asProcessor().process(null)
        );

        dateTime = l(dateTime(1900, 10, 4, 22, 23, 0, 123456789));
        assertEquals(
            "4th 00 Thu 04 10 Oct 277",
            new DateFormat(Source.EMPTY, dateTime, l("%D %y %a %d %m %b %j"), zoneId).makePipe().asProcessor().process(null)
        );

        dateTime = l(dateTime(1997, 10, 4, 22, 23, 0, 0));
        assertEquals(
            "22 22 10 10:23:00 PM 22:23:00 00 6",
            new DateFormat(Source.EMPTY, dateTime, l("%H %k %I %r %T %S %w"), zoneId).makePipe().asProcessor().process(null)
        );

        // based on example at https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format
        // the expected value is: 1998 52
        dateTime = l(date(1999, 1, 1, zoneId));
        assertEquals(
            "1998 52", // Why the actual value always 1998 53
            new DateFormat(Source.EMPTY, dateTime, l("%X %V"), zoneId).makePipe().asProcessor().process(null)
        );

/*     // day 0 is not supported in LocalTime
        dateTime = l(date(2006, 6, 0, zoneId));
        assertEquals(
            "00",
            new DateFormat(Source.EMPTY, dateTime, l("%d"), zoneId).makePipe().asProcessor().process(null)
        );
*/
    }
}
