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

    import java.time.ZonedDateTime;

    import static java.time.ZoneOffset.UTC;
    import static org.elasticsearch.xpack.ql.expression.Literal.NULL;
    import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
    import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomDatetimeLiteral;
    import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.randomStringLiteral;
    import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.dateTime;
    import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.time;

public class DateFormatProcessorTests extends AbstractSqlWireSerializingTestCase<DateFormatProcessor> {

    public static DateFormatProcessor randomDateFormatProcessor() {
        return new DateFormatProcessor(
            new ConstantProcessor(DateTimeTestUtils.nowWithMillisResolution()),
            new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(0, 128)),
            randomZone()
        );
    }

    /*public static Literal randomTimeLiteral() {
        return l(OffsetTime.ofInstant(Instant.ofEpochMilli(ESTestCase.randomLong()), ESTestCase.randomZone()), TIME);
    }*/

    @Override
    protected DateFormatProcessor createTestInstance() {
        return randomDateFormatProcessor();
    }

    @Override
    protected Reader<DateFormatProcessor> instanceReader() {
        return DateFormatProcessor::new;
    }

    @Override
    protected DateFormatProcessor mutateInstance(DateFormatProcessor instance) {
        return new DateFormatProcessor(
            new ConstantProcessor(DateTimeTestUtils.nowWithMillisResolution()),
            new ConstantProcessor(ESTestCase.randomRealisticUnicodeOfLength(128)),
            randomValueOtherThan(instance.zoneId(), ESTestCase::randomZone)
        );
    }

    public void testInvalidInputs() {
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

    public void testWithNulls() {
        assertNull(new DateTimeFormat(Source.EMPTY, randomDatetimeLiteral(), NULL, UTC).makePipe().asProcessor().process(null));
        assertNull(new DateTimeFormat(Source.EMPTY, randomDatetimeLiteral(), l(""), UTC).makePipe().asProcessor().process(null));
        assertNull(new DateTimeFormat(Source.EMPTY, NULL, randomStringLiteral(), UTC).makePipe().asProcessor().process(null));
    }

    public void testFormatting() {
        ZonedDateTime dateTime = dateTime(2019, 9, 3, 18, 10, 37, 123456789);
        assertEquals("2019 09 03", DateFormatProcessor.process(dateTime, "%Y %m %e"));
        assertEquals("September", DateFormatProcessor.process(dateTime, "%M"));
        assertEquals("Tue", DateFormatProcessor.process(dateTime, "%a"));
        assertEquals("Sep", DateFormatProcessor.process(dateTime, "%b"));
        assertEquals("09", DateFormatProcessor.process(dateTime, "%c"));
        assertEquals("03", DateFormatProcessor.process(dateTime, "%D"));
        assertEquals("03", DateFormatProcessor.process(dateTime, "%d"));
        assertEquals("03", DateFormatProcessor.process(dateTime, "%e"));
        assertEquals("18", DateFormatProcessor.process(dateTime, "%H"));
        assertEquals("06", DateFormatProcessor.process(dateTime, "%h"));
        assertEquals("06", DateFormatProcessor.process(dateTime, "%I"));
        assertEquals("10", DateFormatProcessor.process(dateTime, "%i"));
        assertEquals("246", DateFormatProcessor.process(dateTime, "%j"));
        assertEquals("18", DateFormatProcessor.process(dateTime, "%k"));
        assertEquals("06", DateFormatProcessor.process(dateTime, "%l"));
        assertEquals("September", DateFormatProcessor.process(dateTime, "%M"));
        assertEquals("09", DateFormatProcessor.process(dateTime, "%m"));
        assertEquals("PM", DateFormatProcessor.process(dateTime, "%p"));
        assertEquals("06:10:37 PM", DateFormatProcessor.process(dateTime, "%r"));
        assertEquals("37", DateFormatProcessor.process(dateTime, "%S"));
        assertEquals("37", DateFormatProcessor.process(dateTime, "%s"));
        assertEquals("18:10:37 PM", DateFormatProcessor.process(dateTime, "%T"));
        assertEquals("36", DateFormatProcessor.process(dateTime, "%U"));
        assertEquals("36", DateFormatProcessor.process(dateTime, "%u"));
        assertEquals("36", DateFormatProcessor.process(dateTime, "%V"));
        assertEquals("36", DateFormatProcessor.process(dateTime, "%v"));
        assertEquals("Tuesday", DateFormatProcessor.process(dateTime, "%W"));
        assertEquals("3", DateFormatProcessor.process(dateTime, "%w"));
        assertEquals("36", DateFormatProcessor.process(dateTime, "%X"));
        assertEquals("2019", DateFormatProcessor.process(dateTime, "%Y"));
        assertEquals("19", DateFormatProcessor.process(dateTime, "%y"));


    }
}
