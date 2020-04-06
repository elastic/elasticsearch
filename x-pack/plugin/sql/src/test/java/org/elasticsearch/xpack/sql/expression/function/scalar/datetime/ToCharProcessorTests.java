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

import java.time.Instant;
import java.time.OffsetTime;
import java.time.ZoneId;

import static org.elasticsearch.xpack.ql.expression.Literal.NULL;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomDatetimeLiteral;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.dateTime;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.time;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.TIME;

public class ToCharProcessorTests extends AbstractSqlWireSerializingTestCase<ToCharProcessor> {

    public static ToCharProcessor randomToCharProcessor() {
        return new ToCharProcessor(
            new ConstantProcessor(DateTimeTestUtils.nowWithMillisResolution()),
            new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(0, 128)),
            randomZone()
        );
    }

    @Override
    protected ToCharProcessor createTestInstance() {
        return randomToCharProcessor();
    }

    @Override
    protected Reader<ToCharProcessor> instanceReader() {
        return ToCharProcessor::new;
    }

    @Override
    protected ZoneId instanceZoneId(ToCharProcessor instance) {
        return instance.zoneId();
    }

    @Override
    protected ToCharProcessor mutateInstance(ToCharProcessor instance) {
        return new ToCharProcessor(
            new ConstantProcessor(DateTimeTestUtils.nowWithMillisResolution()),
            new ConstantProcessor(ESTestCase.randomRealisticUnicodeOfLength(128)),
            randomValueOtherThan(instance.zoneId(), ESTestCase::randomZone)
        );
    }

    public void testInvalidInputs() {
        SqlIllegalArgumentException siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new ToChar(Source.EMPTY, l("foo"), l("bar"), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals("A date/datetime/time is required; received [foo]", siae.getMessage());

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new ToChar(Source.EMPTY, randomDatetimeLiteral(), l(5), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals("A string is required; received [5]", siae.getMessage());

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new ToChar(Source.EMPTY, randomDatetimeLiteral(), l("invalid"), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals("Invalid date/time pattern is received [invalid]; Unknown pattern letter: i", siae.getMessage());

        siae = expectThrows(
                SqlIllegalArgumentException.class,
                () -> new ToChar(Source.EMPTY, randomTimeLiteral(), l("MM/dd"), randomZone()).makePipe().asProcessor().process(null)
        );
        assertEquals("Invalid date/time pattern is received [MM/dd]; Unsupported field: MonthOfYear", siae.getMessage());
    }

    public void testWithNulls() {
        assertNull(new ToChar(Source.EMPTY, randomDatetimeLiteral(), NULL, randomZone()).makePipe().asProcessor().process(null));
        assertNull(new ToChar(Source.EMPTY, randomDatetimeLiteral(), l(""), randomZone()).makePipe().asProcessor().process(null));
        assertNull(new ToChar(Source.EMPTY, NULL, l("some_pattern"), randomZone()).makePipe().asProcessor().process(null));
    }

    public void testFormatting() {
        ZoneId zoneId = ZoneId.of("Etc/GMT-10");
        Literal dateTime = l(dateTime(2019, 9, 3, 18, 10, 37, 123456789));

        assertEquals("AD : 3", new ToChar(Source.EMPTY, dateTime, l("G : Q"), zoneId).makePipe().asProcessor().process(null));
        assertEquals("2019-09-04", new ToChar(Source.EMPTY, dateTime, l("YYYY-MM-dd"), zoneId).makePipe().asProcessor().process(null));
        assertEquals(
            "04:10:37.123456",
            new ToChar(Source.EMPTY, dateTime, l("HH:mm:ss.SSSSSS"), zoneId).makePipe().asProcessor().process(null)
        );
        assertEquals(
            "2019-09-04 04:10:37.12345678",
            new ToChar(Source.EMPTY, dateTime, l("YYYY-MM-dd HH:mm:ss.SSSSSSSS"), zoneId).makePipe().asProcessor().process(null)
        );
        assertEquals(
                "+1000",
                new ToChar(Source.EMPTY, dateTime, l("Z"), zoneId).makePipe().asProcessor().process(null)
        );
        assertEquals(
                "Etc/GMT-10",
                new ToChar(Source.EMPTY, dateTime, l("z"), zoneId).makePipe().asProcessor().process(null)
        );
        assertEquals(
                "Etc/GMT-10",
                new ToChar(Source.EMPTY, dateTime, l("VV"), zoneId).makePipe().asProcessor().process(null)
        );

        zoneId = ZoneId.of("America/Sao_Paulo");
        assertEquals(
                "-0300",
                new ToChar(Source.EMPTY, dateTime, l("Z"), zoneId).makePipe().asProcessor().process(null)
        );
        assertEquals(
                "BRT",
                new ToChar(Source.EMPTY, dateTime, l("z"), zoneId).makePipe().asProcessor().process(null)
        );
        assertEquals(
                "America/Sao_Paulo",
                new ToChar(Source.EMPTY, dateTime, l("VV"), zoneId).makePipe().asProcessor().process(null)
        );

        assertEquals(
                "07:11:22.1234",
                new ToChar(Source.EMPTY, l(time(10, 11, 22, 123456789), TIME), l("HH:mm:ss.SSSS"), zoneId)
                        .makePipe().asProcessor().process(null)
        );
    }

    public static Literal randomTimeLiteral() {
        return l(OffsetTime.ofInstant(Instant.ofEpochMilli(ESTestCase.randomLong()), ESTestCase.randomZone()), TIME);
    }
}
