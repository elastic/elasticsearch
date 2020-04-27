/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.gen.processor.ConstantProcessor;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.AbstractSqlWireSerializingTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.time.ZoneId;

import static org.elasticsearch.xpack.ql.expression.Literal.NULL;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.dateTime;

public class DateTimeParseProcessorTests extends AbstractSqlWireSerializingTestCase<DateTimeParseProcessor> {

    public static DateTimeParseProcessor randomDateTimeParseProcessor() {
        return new DateTimeParseProcessor(
            new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(0, 128)),
            new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(0, 128))
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
    protected DateTimeParseProcessor mutateInstance(DateTimeParseProcessor instance) {
        return new DateTimeParseProcessor(
            new ConstantProcessor(ESTestCase.randomRealisticUnicodeOfLength(128)),
            new ConstantProcessor(ESTestCase.randomRealisticUnicodeOfLength(128))
        );
    }

    public void testInvalidInputs() {
        SqlIllegalArgumentException siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateTimeParse(Source.EMPTY, l(10), randomStringLiteral()).makePipe().asProcessor().process(null)
        );
        assertEquals("A string is required; received [10]", siae.getMessage());

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateTimeParse(Source.EMPTY, randomStringLiteral(), l(20)).makePipe().asProcessor().process(null)
        );
        assertEquals("A string is required; received [20]", siae.getMessage());

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateTimeParse(Source.EMPTY, l("2020-04-07"), l("invalid")).makePipe().asProcessor().process(null)
        );
        assertEquals(
            "Invalid date/time string [2020-04-07] or pattern [invalid] is received; Unknown pattern letter: i",
            siae.getMessage()
        );

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateTimeParse(Source.EMPTY, l("2020-04-07"), l("MM/dd")).makePipe().asProcessor().process(null)
        );
        assertEquals(
            "Invalid date/time string [2020-04-07] or pattern [MM/dd] is received; Text '2020-04-07' could not be parsed at index 2",
            siae.getMessage()
        );

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateTimeParse(Source.EMPTY, l("07/05/2020"), l("dd/MM/uuuu")).makePipe().asProcessor().process(null)
        );
        assertEquals(
            "Invalid date/time string [07/05/2020] or pattern [dd/MM/uuuu] is received; Unable to convert parsed text into [datetime]",
            siae.getMessage()
        );

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new DateTimeParse(Source.EMPTY, l("10:20:30.123456789"), l("HH:mm:ss.SSSSSSSSS")).makePipe().asProcessor().process(null)
        );
        assertEquals(
            "Invalid date/time string [10:20:30.123456789] or pattern [HH:mm:ss.SSSSSSSSS] is received; "
                + "Unable to convert parsed text into [datetime]",
            siae.getMessage()
        );
    }

    public void testWithNulls() {
        assertNull(new DateTimeParse(Source.EMPTY, randomStringLiteral(), NULL).makePipe().asProcessor().process(null));
        assertNull(new DateTimeParse(Source.EMPTY, randomStringLiteral(), l("")).makePipe().asProcessor().process(null));
        assertNull(new DateTimeParse(Source.EMPTY, NULL, randomStringLiteral()).makePipe().asProcessor().process(null));
        assertNull(new DateTimeParse(Source.EMPTY, l(""), randomStringLiteral()).makePipe().asProcessor().process(null));
    }

    public void testParsing() {
        ZoneId zoneId = ZoneId.of("America/Sao_Paulo");
        assertEquals(
            dateTime(2020, 4, 7, 10, 20, 30, 123000000),
            new DateTimeParse(Source.EMPTY, l("07/04/2020 10:20:30.123"), l("dd/MM/uuuu HH:mm:ss.SSS")).makePipe()
                .asProcessor()
                .process(null)
        );
        assertEquals(
            dateTime(2020, 4, 7, 10, 20, 30, 123456789, zoneId),
            new DateTimeParse(Source.EMPTY, l("07/04/2020 10:20:30.123456789 America/Sao_Paulo"), l("dd/MM/uuuu HH:mm:ss.SSSSSSSSS VV"))
                .makePipe()
                .asProcessor()
                .process(null)
        );
        assertEquals(
            dateTime(2020, 4, 7, 10, 20, 30, 123456789, ZoneId.of("+05:30")),
            new DateTimeParse(Source.EMPTY, l("07/04/2020 10:20:30.123456789 +05:30"), l("dd/MM/uuuu HH:mm:ss.SSSSSSSSS zz")).makePipe()
                .asProcessor()
                .process(null)
        );
    }
}
