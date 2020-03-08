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
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.elasticsearch.xpack.ql.expression.Literal.NULL;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomDatetimeLiteral;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.dateTime;
import static org.elasticsearch.xpack.sql.proto.StringUtils.ISO_DATE_WITH_NANOS;

public class DateTruncProcessorTests extends AbstractSqlWireSerializingTestCase<DateTruncProcessor> {

    public static DateTruncProcessor randomDateTruncProcessor() {
        return new DateTruncProcessor(
            new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(0, 128)),
            new ConstantProcessor(DateTimeTestUtils.nowWithMillisResolution()),
            randomZone());
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
            randomValueOtherThan(instance.zoneId(), ESTestCase::randomZone));
    }

    public void testInvalidInputs() {
        SqlIllegalArgumentException siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DateTrunc(Source.EMPTY, l(5), randomDatetimeLiteral(), randomZone()).makePipe().asProcessor().process(null));
        assertEquals("A string is required; received [5]", siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DateTrunc(Source.EMPTY, l("days"), l("foo"), randomZone()).makePipe().asProcessor().process(null));
        assertEquals("A date/datetime is required; received [foo]", siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DateTrunc(Source.EMPTY, l("invalid"), randomDatetimeLiteral(), randomZone()).makePipe().asProcessor().process(null));
        assertEquals("A value of [MILLENNIUM, CENTURY, DECADE, YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, " +
                "SECOND, MILLISECOND, MICROSECOND, NANOSECOND] or their aliases is required; received [invalid]",
            siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new DateTrunc(Source.EMPTY, l("dacede"), randomDatetimeLiteral(), randomZone()).makePipe().asProcessor().process(null));
        assertEquals("Received value [dacede] is not valid date part for truncation; did you mean [decade, decades]?",
            siae.getMessage());
    }

    public void testWithNulls() {
        assertNull(new DateTrunc(Source.EMPTY, NULL, randomDatetimeLiteral(), randomZone()).makePipe().asProcessor().process(null));
        assertNull(new DateTrunc(Source.EMPTY, l("days"), NULL, randomZone()).makePipe().asProcessor().process(null));
        assertNull(new DateTrunc(Source.EMPTY, NULL, NULL, randomZone()).makePipe().asProcessor().process(null));
    }

    public void testTruncation() {
        ZoneId zoneId = ZoneId.of("Etc/GMT-10");
        Literal dateTime = l(dateTime(2019, 9, 3, 18, 10, 37, 123456789));

        assertEquals("2000-01-01T00:00:00.000+10:00",
            DateUtils.toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("millennia"), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2000-01-01T00:00:00.000+10:00",
            DateUtils.toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("CENTURY"), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2010-01-01T00:00:00.000+10:00",
            DateUtils.toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("decades"), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2019-01-01T00:00:00.000+10:00",
            DateUtils.toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("years"), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2019-07-01T00:00:00.000+10:00",
            toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("quarters"), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2019-09-01T00:00:00.000+10:00",
            toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("month"), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2019-09-02T00:00:00.000+10:00",
            toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("weeks"), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2019-09-04T00:00:00.000+10:00",
            toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("days"), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2019-09-04T04:00:00.000+10:00",
            toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("hh"), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2019-09-04T04:10:00.000+10:00",
            toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("mi"), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2019-09-04T04:10:37.000+10:00",
            toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("second"), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2019-09-04T04:10:37.123+10:00",
            toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("ms"), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2019-09-04T04:10:37.123456+10:00",
            toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("mcs"), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
        assertEquals("2019-09-04T04:10:37.123456789+10:00",
            toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("nanoseconds"), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
    }

    public void testTruncationEdgeCases() {
        ZoneId zoneId = ZoneId.of("Etc/GMT-10");
        Literal dateTime = l(dateTime(-11412, 9, 3, 18, 10, 37, 123456789));
        assertEquals("-11000-01-01T00:00:00.000+10:00",
            DateUtils.toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("millennia"), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));

        dateTime = l(dateTime(-12999, 9, 3, 18, 10, 37, 123456789));
        assertEquals("-12900-01-01T00:00:00.000+10:00",
            DateUtils.toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("centuries"), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));

        dateTime = l(dateTime(-32999, 9, 3, 18, 10, 37, 123456789));
        assertEquals("-32990-01-01T00:00:00.000+10:00",
            DateUtils.toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("decades"), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));

        dateTime = l(dateTime(-1234, 9, 3, 18, 10, 37, 123456789));
        assertEquals("-1234-08-29T00:00:00.000+10:00",
            DateUtils.toString((ZonedDateTime) new DateTrunc(Source.EMPTY, l("week"), dateTime, zoneId)
                .makePipe().asProcessor().process(null)));
    }

    private String toString(ZonedDateTime dateTime) {
        return ISO_DATE_WITH_NANOS.format(dateTime);
    }
}
