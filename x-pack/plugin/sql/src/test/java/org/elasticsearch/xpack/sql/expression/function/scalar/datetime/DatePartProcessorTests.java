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

public class DatePartProcessorTests extends AbstractSqlWireSerializingTestCase<DatePartProcessor> {

    public static DatePartProcessor randomDatePartProcessor() {
        return new DatePartProcessor(
            new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(0, 128)),
            new ConstantProcessor(DateTimeTestUtils.nowWithMillisResolution()),
            randomZone());
    }

    @Override
    protected DatePartProcessor createTestInstance() {
        return randomDatePartProcessor();
    }

    @Override
    protected Reader<DatePartProcessor> instanceReader() {
        return DatePartProcessor::new;
    }

    @Override
    protected ZoneId instanceZoneId(DatePartProcessor instance) {
        return instance.zoneId();
    }

    @Override
    protected DatePartProcessor mutateInstance(DatePartProcessor instance) {
        return new DatePartProcessor(
            new ConstantProcessor(ESTestCase.randomRealisticUnicodeOfLength(128)),
            new ConstantProcessor(DateTimeTestUtils.nowWithMillisResolution()),
            randomValueOtherThan(instance.zoneId(), ESTestCase::randomZone));
    }

    public void testInvalidInputs() {
        SqlIllegalArgumentException siae = expectThrows(SqlIllegalArgumentException.class,
                () -> new DatePart(Source.EMPTY, l(5), randomDatetimeLiteral(), randomZone()).makePipe().asProcessor().process(null));
        assertEquals("A string is required; received [5]", siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DatePart(Source.EMPTY, l("days"), l("foo"), randomZone()).makePipe().asProcessor().process(null));
        assertEquals("A date/datetime is required; received [foo]", siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DatePart(Source.EMPTY, l("invalid"), randomDatetimeLiteral(), randomZone()).makePipe().asProcessor().process(null));
        assertEquals("A value of [YEAR, QUARTER, MONTH, DAYOFYEAR, DAY, WEEK, WEEKDAY, HOUR, MINUTE, SECOND, MILLISECOND, " +
                "MICROSECOND, NANOSECOND, TZOFFSET] or their aliases is required; received [invalid]",
            siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DatePart(Source.EMPTY, l("dayfyear"), randomDatetimeLiteral(), randomZone()).makePipe().asProcessor().process(null));
        assertEquals("Received value [dayfyear] is not valid date part for extraction; did you mean [dayofyear, year]?",
            siae.getMessage());
    }

    public void testWithNulls() {
        assertNull(new DatePart(Source.EMPTY, NULL, randomDatetimeLiteral(), randomZone()).makePipe().asProcessor().process(null));
        assertNull(new DatePart(Source.EMPTY, l("days"), NULL, randomZone()).makePipe().asProcessor().process(null));
        assertNull(new DatePart(Source.EMPTY, NULL, NULL, randomZone()).makePipe().asProcessor().process(null));
    }

    public void testTruncation() {
        ZoneId zoneId = ZoneId.of("+05:10");
        Literal dateTime = l(dateTime(2007, 10, 30, 12, 15, 32, 123456789));

        assertEquals(2007, new DatePart(Source.EMPTY, l("years"), dateTime, zoneId).makePipe().asProcessor().process(null));
        assertEquals(4, new DatePart(Source.EMPTY, l("quarters"), dateTime, zoneId).makePipe().asProcessor().process(null));
        assertEquals(10, new DatePart(Source.EMPTY, l("month"), dateTime, zoneId).makePipe().asProcessor().process(null));
        assertEquals(303, new DatePart(Source.EMPTY, l("dayofyear"), dateTime, zoneId).makePipe().asProcessor().process(null));
        assertEquals(30, new DatePart(Source.EMPTY, l("day"), dateTime, zoneId).makePipe().asProcessor().process(null));
        assertEquals(44, new DatePart(Source.EMPTY, l("week"), dateTime, zoneId).makePipe().asProcessor().process(null));
        assertEquals(3, new DatePart(Source.EMPTY, l("weekday"), dateTime, zoneId).makePipe().asProcessor().process(null));
        assertEquals(17, new DatePart(Source.EMPTY, l("hour"), dateTime, zoneId).makePipe().asProcessor().process(null));
        assertEquals(25, new DatePart(Source.EMPTY, l("minutes"), dateTime, zoneId).makePipe().asProcessor().process(null));
        assertEquals(32, new DatePart(Source.EMPTY, l("ss"), dateTime, zoneId).makePipe().asProcessor().process(null));
        assertEquals(123, new DatePart(Source.EMPTY, l("ms"), dateTime, zoneId).makePipe().asProcessor().process(null));
        assertEquals(123456, new DatePart(Source.EMPTY, l("microsecond"), dateTime, zoneId).makePipe().asProcessor().process(null));
        assertEquals(123456789, new DatePart(Source.EMPTY, l("ns"), dateTime, zoneId).makePipe().asProcessor().process(null));
        assertEquals(310, new DatePart(Source.EMPTY, l("tzoffset"), dateTime, zoneId).makePipe().asProcessor().process(null));
    }
}
