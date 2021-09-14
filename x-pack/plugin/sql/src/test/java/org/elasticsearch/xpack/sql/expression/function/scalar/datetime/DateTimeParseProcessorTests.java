/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.gen.processor.ConstantProcessor;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.AbstractSqlWireSerializingTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeParseProcessor.Parser;

import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.elasticsearch.xpack.ql.expression.Literal.NULL;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.dateTime;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.time;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.date;

public class DateTimeParseProcessorTests extends AbstractSqlWireSerializingTestCase<DateTimeParseProcessor> {

    public static DateTimeParseProcessor randomDateTimeParseProcessor() {
        return new DateTimeParseProcessor(
            new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(0, 128)),
            new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(0, 128)),
            randomZone(),
            randomFrom(Parser.values())
        );
    }

    @Override
    protected DateTimeParseProcessor createTestInstance() {
        return randomDateTimeParseProcessor();
    }

    @Override
    protected Reader<DateTimeParseProcessor> instanceReader() {
        return DateTimeParseProcessor::new;
    }

    @Override
    protected ZoneId instanceZoneId(DateTimeParseProcessor instance) {
        return instance.zoneId();
    }

    @Override
    protected DateTimeParseProcessor mutateInstance(DateTimeParseProcessor instance) {
        Parser replaced = randomValueOtherThan(instance.parser(), () -> randomFrom(Parser.values()));
        return new DateTimeParseProcessor(
            new ConstantProcessor(ESTestCase.randomRealisticUnicodeOfLength(128)),
            new ConstantProcessor(ESTestCase.randomRealisticUnicodeOfLength(128)),
            randomZone(),
            replaced
        );
    }

    public void testDateTimeInvalidInputs() {
        SqlIllegalArgumentException siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateTimeParse(Source.EMPTY, l(10), randomStringLiteral(), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals("A string is required; received [10]", siae.getMessage());

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateTimeParse(Source.EMPTY, randomStringLiteral(), l(20), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals("A string is required; received [20]", siae.getMessage());

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateTimeParse(Source.EMPTY, l("2020-04-07"), l("invalid"), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals(
            "Invalid datetime string [2020-04-07] or pattern [invalid] is received; Unknown pattern letter: i",
            siae.getMessage()
        );

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateTimeParse(Source.EMPTY, l("2020-04-07"), l("MM/dd"), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals(
            "Invalid datetime string [2020-04-07] or pattern [MM/dd] is received; Text '2020-04-07' could not be parsed at index 2",
            siae.getMessage()
        );

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateTimeParse(Source.EMPTY, l("07/05/2020"), l("dd/MM/uuuu"), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals(
            "Invalid datetime string [07/05/2020] or pattern [dd/MM/uuuu] is received; Unable to convert parsed text into [datetime]",
            siae.getMessage()
        );

        siae = expectThrows(
            SqlIllegalArgumentException.class, () -> new DateTimeParse(
                Source.EMPTY, l("10:20:30.123456789"), l("HH:mm:ss.SSSSSSSSS"), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals(
            "Invalid datetime string [10:20:30.123456789] or pattern [HH:mm:ss.SSSSSSSSS] is received; "
                + "Unable to convert parsed text into [datetime]",
            siae.getMessage()
        );
    }

    public void testTimeInvalidInputs() {
        SqlIllegalArgumentException siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new TimeParse(Source.EMPTY, l(10), randomStringLiteral(), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals("A string is required; received [10]", siae.getMessage());

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new TimeParse(Source.EMPTY, randomStringLiteral(), l(20), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals("A string is required; received [20]", siae.getMessage());

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new TimeParse(Source.EMPTY, l("11:04:07"), l("invalid"), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals(
            "Invalid time string [11:04:07] or pattern [invalid] is received; Unknown pattern letter: i",
            siae.getMessage()
        );

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new TimeParse(Source.EMPTY, l("11:04:07"), l("HH:mm"), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals(
            "Invalid time string [11:04:07] or pattern [HH:mm] is received; " +
                "Text '11:04:07' could not be parsed, unparsed text found at index 5",
            siae.getMessage()
        );

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new TimeParse(Source.EMPTY, l("07/05/2020"), l("dd/MM/uuuu"), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals(
            "Invalid time string [07/05/2020] or pattern [dd/MM/uuuu] is received; Unable to convert parsed text into [time]",
            siae.getMessage()
        );
    }

    public void testDateInvalidInputs() {
        SqlIllegalArgumentException siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateParse(Source.EMPTY, l(10), randomStringLiteral(), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals("A string is required; received [10]", siae.getMessage());

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateParse(Source.EMPTY, randomStringLiteral(), l(20), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals("A string is required; received [20]", siae.getMessage());

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateParse(Source.EMPTY, l("07/05/2020"), l("invalid"), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals(
            "Invalid date string [07/05/2020] or pattern [invalid] is received; Unknown pattern letter: i",
                siae.getMessage()
        );

        siae = expectThrows(
             SqlIllegalArgumentException.class,
             () -> new DateParse(Source.EMPTY, l("07/05/2020"), l("dd/MM"), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals(
           "Invalid date string [07/05/2020] or pattern [dd/MM] is received; " +
                "Text '07/05/2020' could not be parsed, unparsed text found at index 5",
           siae.getMessage()
        );

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateParse(Source.EMPTY, l("11:04:07"), l("HH:mm:ss"), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals(
            "Invalid date string [11:04:07] or pattern [HH:mm:ss] is received; Unable to convert parsed text into [date]",
            siae.getMessage()
        );

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateParse(Source.EMPTY, l("05/2020 11:04:07"), l("MM/uuuu HH:mm:ss"), randomZone())
                .makePipe()
                .asProcessor()
                .process(null)
        );
        assertEquals(
            "Invalid date string [05/2020 11:04:07] or pattern [MM/uuuu HH:mm:ss] is received; Unable to convert parsed text into [date]",
            siae.getMessage()
        );
    }

    public void testWithNulls() {
        // DateTimeParse
        assertNull(new DateTimeParse(Source.EMPTY, randomStringLiteral(), NULL, randomZone()).makePipe().asProcessor().process(null));
        assertNull(new DateTimeParse(Source.EMPTY, randomStringLiteral(), l(""), randomZone()).makePipe().asProcessor().process(null));
        assertNull(new DateTimeParse(Source.EMPTY, NULL, randomStringLiteral(), randomZone()).makePipe().asProcessor().process(null));
        assertNull(new DateTimeParse(Source.EMPTY, l(""), randomStringLiteral(), randomZone()).makePipe().asProcessor().process(null));
        // TimeParse
        assertNull(new TimeParse(Source.EMPTY, randomStringLiteral(), NULL, randomZone()).makePipe().asProcessor().process(null));
        assertNull(new TimeParse(Source.EMPTY, randomStringLiteral(), l(""), randomZone()).makePipe().asProcessor().process(null));
        assertNull(new TimeParse(Source.EMPTY, NULL, randomStringLiteral(), randomZone()).makePipe().asProcessor().process(null));
        assertNull(new TimeParse(Source.EMPTY, l(""), randomStringLiteral(), randomZone()).makePipe().asProcessor().process(null));
        // DateParse
        assertNull(new DateParse(Source.EMPTY, randomStringLiteral(), NULL, randomZone()).makePipe().asProcessor().process(null));
        assertNull(new DateParse(Source.EMPTY, randomStringLiteral(), l(""), randomZone()).makePipe().asProcessor().process(null));
        assertNull(new DateParse(Source.EMPTY, NULL, randomStringLiteral(), randomZone()).makePipe().asProcessor().process(null));
        assertNull(new DateParse(Source.EMPTY, l(""), randomStringLiteral(), randomZone()).makePipe().asProcessor().process(null));
    }

    public void testParsing() {
        // DateTimeParse
        ZoneId zoneId = ZoneId.of("America/Sao_Paulo");
        assertEquals(
            dateTime(2020, 4, 7, 10, 20, 30, 123000000, zoneId),
            new DateTimeParse(Source.EMPTY, l("07/04/2020 10:20:30.123"), l("dd/MM/uuuu HH:mm:ss.SSS"), zoneId)
                .makePipe()
                .asProcessor()
                .process(null)
        );
        assertEquals(
            dateTime(2020, 4, 7, 5, 20, 30, 123456789, zoneId),
            new DateTimeParse(Source.EMPTY, l("07/04/2020 10:20:30.123456789 Europe/Berlin"), l("dd/MM/uuuu HH:mm:ss.SSSSSSSSS VV"), zoneId)
                .makePipe()
                .asProcessor()
                .process(null)
        );
        assertEquals(
            dateTime(2020, 4, 7, 1, 50, 30, 123456789, zoneId),
            new DateTimeParse(Source.EMPTY, l("07/04/2020 10:20:30.123456789 +05:30"), l("dd/MM/uuuu HH:mm:ss.SSSSSSSSS zz"), zoneId)
                .makePipe()
                .asProcessor()
                .process(null)
        );
        // TimeParse
        assertEquals(
            time(10, 20, 30, 123000000, zoneId),
            new TimeParse(Source.EMPTY, l("10:20:30.123"), l("HH:mm:ss.SSS"), zoneId).makePipe()
                .asProcessor()
                .process(null)
        );
        assertEquals(
            time(10, 20, 30, 123456789, ZoneOffset.of("+05:30"), zoneId),
            new TimeParse(Source.EMPTY, l("10:20:30.123456789 +05:30"), l("HH:mm:ss.SSSSSSSSS zz"), zoneId).makePipe()
                .asProcessor()
                .process(null)
        );
        // DateParse
        assertEquals(
            date(2020, 4, 7, zoneId),
            new DateParse(Source.EMPTY, l("07/04/2020"), l("dd/MM/uuuu"), zoneId).makePipe()
                .asProcessor()
                .process(null)
        );
        assertEquals(
                date(2020, 4, 7, zoneId),
                new DateParse(Source.EMPTY, l("07/04/2020 12:12:00"), l("dd/MM/uuuu HH:mm:ss"), zoneId).makePipe()
                        .asProcessor()
                        .process(null)
        );
        assertEquals(
            time(10, 20, 30, 123456789, ZoneOffset.of("+05:30"), zoneId),
            new TimeParse(Source.EMPTY, l("16/06/2020 10:20:30.123456789 +05:30"), l("dd/MM/uuuu HH:mm:ss.SSSSSSSSS zz"), zoneId).makePipe()
                .asProcessor()
                .process(null)
        );
    }
}
