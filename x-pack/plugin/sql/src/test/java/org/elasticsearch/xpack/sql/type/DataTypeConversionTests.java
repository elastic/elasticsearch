/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataTypeConversion.Conversion;

import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.date;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.dateTime;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.time;
import static org.elasticsearch.xpack.sql.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.sql.type.DataType.BYTE;
import static org.elasticsearch.xpack.sql.type.DataType.DATE;
import static org.elasticsearch.xpack.sql.type.DataType.DATETIME;
import static org.elasticsearch.xpack.sql.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.sql.type.DataType.FLOAT;
import static org.elasticsearch.xpack.sql.type.DataType.INTEGER;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_HOUR_TO_MINUTE;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_HOUR_TO_SECOND;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_MONTH;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_SECOND;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_YEAR;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_YEAR_TO_MONTH;
import static org.elasticsearch.xpack.sql.type.DataType.IP;
import static org.elasticsearch.xpack.sql.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.sql.type.DataType.LONG;
import static org.elasticsearch.xpack.sql.type.DataType.NULL;
import static org.elasticsearch.xpack.sql.type.DataType.SHORT;
import static org.elasticsearch.xpack.sql.type.DataType.TEXT;
import static org.elasticsearch.xpack.sql.type.DataType.TIME;
import static org.elasticsearch.xpack.sql.type.DataType.UNSUPPORTED;
import static org.elasticsearch.xpack.sql.type.DataType.fromTypeName;
import static org.elasticsearch.xpack.sql.type.DataType.values;
import static org.elasticsearch.xpack.sql.type.DataTypeConversion.commonType;
import static org.elasticsearch.xpack.sql.type.DataTypeConversion.conversionFor;
import static org.elasticsearch.xpack.sql.util.DateUtils.asDateOnly;
import static org.elasticsearch.xpack.sql.util.DateUtils.asDateTime;
import static org.elasticsearch.xpack.sql.util.DateUtils.asTimeOnly;


public class DataTypeConversionTests extends ESTestCase {

    public void testConversionToString() {
        DataType to = KEYWORD;
        {
            Conversion conversion = conversionFor(DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals("10.0", conversion.convert(10.0));
        }
        {
            Conversion conversion = conversionFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals("1973-11-29", conversion.convert(asDateOnly(123456789101L)));
            assertEquals("1966-02-02", conversion.convert(asDateOnly(-123456789101L)));
        }
        {
            Conversion conversion = conversionFor(TIME, to);
            assertNull(conversion.convert(null));
            assertEquals("00:02:03.456Z", conversion.convert(asTimeOnly(123456L)));
            assertEquals("21:33:09.101Z", conversion.convert(asTimeOnly(123456789101L)));
            assertEquals("23:57:56.544Z", conversion.convert(asTimeOnly(-123456L)));
            assertEquals("02:26:50.899Z", conversion.convert(asTimeOnly(-123456789101L)));
        }
        {
            Conversion conversion = conversionFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals("1973-11-29T21:33:09.101Z", conversion.convert(asDateTime(123456789101L)));
            assertEquals("1966-02-02T02:26:50.899Z", conversion.convert(asDateTime(-123456789101L)));
        }
    }

    /**
     * Test conversion to long.
     */
    public void testConversionToLong() {
        DataType to = LONG;
        {
            Conversion conversion = conversionFor(DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals(10L, conversion.convert(10.0));
            assertEquals(10L, conversion.convert(10.1));
            assertEquals(11L, conversion.convert(10.6));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert(Double.MAX_VALUE));
            assertEquals("[" + Double.MAX_VALUE + "] out of [long] range", e.getMessage());
        }
        {
            Conversion conversion = conversionFor(INTEGER, to);
            assertNull(conversion.convert(null));
            assertEquals(10L, conversion.convert(10));
            assertEquals(-134L, conversion.convert(-134));
        }
        {
            Conversion conversion = conversionFor(BOOLEAN, to);
            assertNull(conversion.convert(null));
            assertEquals(1L, conversion.convert(true));
            assertEquals(0L, conversion.convert(false));
        }
        {
            Conversion conversion = conversionFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals(123379200000L, conversion.convert(asDateOnly(123456789101L)));
            assertEquals(-123465600000L, conversion.convert(asDateOnly(-123456789101L)));
        }
        {
            Conversion conversion = conversionFor(TIME, to);
            assertNull(conversion.convert(null));
            assertEquals(123456L, conversion.convert(asTimeOnly(123456L)));
            assertEquals(77589101L, conversion.convert(asTimeOnly(123456789101L)));
            assertEquals(86276544L, conversion.convert(asTimeOnly(-123456L)));
            assertEquals(8810899L, conversion.convert(asTimeOnly(-123456789101L)));
        }
        {
            Conversion conversion = conversionFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals(123456789101L, conversion.convert(asDateTime(123456789101L)));
            assertEquals(-123456789101L, conversion.convert(asDateTime(-123456789101L)));
        }
        {
            Conversion conversion = conversionFor(KEYWORD, to);
            assertNull(conversion.convert(null));
            assertEquals(1L, conversion.convert("1"));
            assertEquals(0L, conversion.convert("-0"));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("0xff"));
            assertEquals("cannot cast [0xff] to [long]", e.getMessage());
        }
    }

    public void testConversionToDate() {
        DataType to = DATE;
        {
            Conversion conversion = conversionFor(DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals(date(10L), conversion.convert(10.0));
            assertEquals(date(10L), conversion.convert(10.1));
            assertEquals(date(11L), conversion.convert(10.6));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert(Double.MAX_VALUE));
            assertEquals("[" + Double.MAX_VALUE + "] out of [long] range", e.getMessage());
        }
        {
            Conversion conversion = conversionFor(INTEGER, to);
            assertNull(conversion.convert(null));
            assertEquals(date(10L), conversion.convert(10));
            assertEquals(date(-134L), conversion.convert(-134));
        }
        {
            Conversion conversion = conversionFor(BOOLEAN, to);
            assertNull(conversion.convert(null));
            assertEquals(date(1), conversion.convert(true));
            assertEquals(date(0), conversion.convert(false));
        }
        {
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversionFor(TIME, to));
            assertEquals("cannot convert from [time] to [date]", e.getMessage());
        }
        {
            Conversion conversion = conversionFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals(date(123456780000L), conversion.convert(asDateTime(123456789101L)));
            assertEquals(date(-123456789101L), conversion.convert(asDateTime(-123456789101L)));
        }
        {
            Conversion conversion = conversionFor(KEYWORD, to);
            assertNull(conversion.convert(null));

            assertEquals(date(0L), conversion.convert("1970-01-01"));
            assertEquals(date(1483228800000L), conversion.convert("2017-01-01"));
            assertEquals(date(-1672531200000L), conversion.convert("1917-01-01"));
            assertEquals(date(18000000L), conversion.convert("1970-01-01"));

            // double check back and forth conversion

            ZonedDateTime zdt = org.elasticsearch.common.time.DateUtils.nowWithMillisResolution();
            Conversion forward = conversionFor(DATE, KEYWORD);
            Conversion back = conversionFor(KEYWORD, DATE);
            assertEquals(asDateOnly(zdt), back.convert(forward.convert(zdt)));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("0xff"));
            assertEquals("cannot cast [0xff] to [date]: Text '0xff' could not be parsed at index 0", e.getMessage());
        }
    }

    public void testConversionToTime() {
        DataType to = TIME;
        {
            Conversion conversion = conversionFor(DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals(time(10L), conversion.convert(10.0));
            assertEquals(time(10L), conversion.convert(10.1));
            assertEquals(time(11L), conversion.convert(10.6));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert(Double.MAX_VALUE));
            assertEquals("[" + Double.MAX_VALUE + "] out of [long] range", e.getMessage());
        }
        {
            Conversion conversion = conversionFor(INTEGER, to);
            assertNull(conversion.convert(null));
            assertEquals(time(10L), conversion.convert(10));
            assertEquals(time(-134L), conversion.convert(-134));
        }
        {
            Conversion conversion = conversionFor(BOOLEAN, to);
            assertNull(conversion.convert(null));
            assertEquals(time(1), conversion.convert(true));
            assertEquals(time(0), conversion.convert(false));
        }
        {
            Conversion conversion = conversionFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals(time(123379200000L), conversion.convert(asDateOnly(123456789101L)));
            assertEquals(time(-123465600000L), conversion.convert(asDateOnly(-123456789101L)));
        }
        {
            Conversion conversion = conversionFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals(time(77589101L), conversion.convert(asDateTime(123456789101L)));
            assertEquals(time(8810899L), conversion.convert(asDateTime(-123456789101L)));
        }
        {
            Conversion conversion = conversionFor(KEYWORD, to);
            assertNull(conversion.convert(null));

            assertEquals(time(0L), conversion.convert("00:00:00Z"));
            assertEquals(time(1000L), conversion.convert("00:00:01Z"));
            assertEquals(time(1234L), conversion.convert("00:00:01.234Z"));
            assertEquals(time(63296789L).withOffsetSameInstant(ZoneOffset.ofHours(-5)), conversion.convert("12:34:56.789-05:00"));

            // double check back and forth conversion
            OffsetTime ot = org.elasticsearch.common.time.DateUtils.nowWithMillisResolution().toOffsetDateTime().toOffsetTime();
            Conversion forward = conversionFor(TIME, KEYWORD);
            Conversion back = conversionFor(KEYWORD, TIME);
            assertEquals(ot, back.convert(forward.convert(ot)));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("0xff"));
            assertEquals("cannot cast [0xff] to [time]: Text '0xff' could not be parsed at index 0",
                e.getMessage());
        }
    }

    public void testConversionToDateTime() {
        DataType to = DATETIME;
        {
            Conversion conversion = conversionFor(DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals(dateTime(10L), conversion.convert(10.0));
            assertEquals(dateTime(10L), conversion.convert(10.1));
            assertEquals(dateTime(11L), conversion.convert(10.6));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert(Double.MAX_VALUE));
            assertEquals("[" + Double.MAX_VALUE + "] out of [long] range", e.getMessage());
        }
        {
            Conversion conversion = conversionFor(INTEGER, to);
            assertNull(conversion.convert(null));
            assertEquals(dateTime(10L), conversion.convert(10));
            assertEquals(dateTime(-134L), conversion.convert(-134));
        }
        {
            Conversion conversion = conversionFor(BOOLEAN, to);
            assertNull(conversion.convert(null));
            assertEquals(dateTime(1), conversion.convert(true));
            assertEquals(dateTime(0), conversion.convert(false));
        }
        {
            Conversion conversion = conversionFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals(dateTime(123379200000L), conversion.convert(asDateOnly(123456789101L)));
            assertEquals(dateTime(-123465600000L), conversion.convert(asDateOnly(-123456789101L)));
        }
        {
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversionFor(TIME, to));
            assertEquals("cannot convert from [time] to [datetime]", e.getMessage());
        }
        {
            Conversion conversion = conversionFor(KEYWORD, to);
            assertNull(conversion.convert(null));

            assertEquals(dateTime(0L), conversion.convert("1970-01-01"));
            assertEquals(dateTime(1000L), conversion.convert("1970-01-01T00:00:01Z"));
            assertEquals(dateTime(1483228800000L), conversion.convert("2017-01-01T00:00:00Z"));
            assertEquals(dateTime(1483228800000L), conversion.convert("2017-01-01T00:00:00Z"));
            assertEquals(dateTime(18000000L), conversion.convert("1970-01-01T00:00:00-05:00"));

            // double check back and forth conversion

            ZonedDateTime dt = org.elasticsearch.common.time.DateUtils.nowWithMillisResolution();
            Conversion forward = conversionFor(DATETIME, KEYWORD);
            Conversion back = conversionFor(KEYWORD, DATETIME);
            assertEquals(dt, back.convert(forward.convert(dt)));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("0xff"));
            assertEquals("cannot cast [0xff] to [datetime]: failed to parse date field [0xff] with format [date_optional_time]",
                e.getMessage());
        }
    }

    public void testConversionToFloat() {
        DataType to = FLOAT;
        {
            Conversion conversion = conversionFor(DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals(10.0f, (float) conversion.convert(10.0d), 0.00001);
            assertEquals(10.1f, (float) conversion.convert(10.1d), 0.00001);
            assertEquals(10.6f, (float) conversion.convert(10.6d), 0.00001);
        }
        {
            Conversion conversion = conversionFor(INTEGER, to);
            assertNull(conversion.convert(null));
            assertEquals(10.0f, (float) conversion.convert(10), 0.00001);
            assertEquals(-134.0f, (float) conversion.convert(-134), 0.00001);
        }
        {
            Conversion conversion = conversionFor(BOOLEAN, to);
            assertNull(conversion.convert(null));
            assertEquals(1.0f, (float) conversion.convert(true), 0);
            assertEquals(0.0f, (float) conversion.convert(false), 0);
        }
        {
            Conversion conversion = conversionFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals(1.233792E11f, (float) conversion.convert(asDateOnly(123456789101L)), 0);
            assertEquals(-1.234656E11f, (float) conversion.convert(asDateOnly(-123456789101L)), 0);
        }
        {
            Conversion conversion = conversionFor(TIME, to);
            assertNull(conversion.convert(null));
            assertEquals(123456.0f, (float) conversion.convert(asTimeOnly(123456L)), 0);
            assertEquals(7.7589104E7f, (float) conversion.convert(asTimeOnly(123456789101L)), 0);
            assertEquals(8.6276544E7f, (float) conversion.convert(asTimeOnly(-123456L)), 0);
            assertEquals(8810899.0f, (float) conversion.convert(asTimeOnly(-123456789101L)), 0);
        }
        {
            Conversion conversion = conversionFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals(1.23456789101E11f, (float) conversion.convert(asDateTime(123456789101L)), 0);
            assertEquals(-1.23456789101E11f, (float) conversion.convert(asDateTime(-123456789101L)), 0);
        }
        {
            Conversion conversion = conversionFor(KEYWORD, to);
            assertNull(conversion.convert(null));
            assertEquals(1.0f, (float) conversion.convert("1"), 0);
            assertEquals(0.0f, (float) conversion.convert("-0"), 0);
            assertEquals(12.776f, (float) conversion.convert("12.776"), 0.00001);
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("0xff"));
            assertEquals("cannot cast [0xff] to [float]", e.getMessage());
        }
    }

    public void testConversionToDouble() {
        DataType to = DOUBLE;
        {
            Conversion conversion = conversionFor(FLOAT, to);
            assertNull(conversion.convert(null));
            assertEquals(10.0, (double) conversion.convert(10.0f), 0.00001);
            assertEquals(10.1, (double) conversion.convert(10.1f), 0.00001);
            assertEquals(10.6, (double) conversion.convert(10.6f), 0.00001);
        }
        {
            Conversion conversion = conversionFor(INTEGER, to);
            assertNull(conversion.convert(null));
            assertEquals(10.0, (double) conversion.convert(10), 0.00001);
            assertEquals(-134.0, (double) conversion.convert(-134), 0.00001);
        }
        {
            Conversion conversion = conversionFor(BOOLEAN, to);
            assertNull(conversion.convert(null));
            assertEquals(1.0, (double) conversion.convert(true), 0);
            assertEquals(0.0, (double) conversion.convert(false), 0);
        }
        {
            Conversion conversion = conversionFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals(1.233792E11, (double) conversion.convert(asDateOnly(123456789101L)), 0);
            assertEquals(-1.234656E11, (double) conversion.convert(asDateOnly(-123456789101L)), 0);
        }
        {
            Conversion conversion = conversionFor(TIME, to);
            assertNull(conversion.convert(null));
            assertEquals(123456.0, (double) conversion.convert(asTimeOnly(123456L)), 0);
            assertEquals(7.7589101E7, (double) conversion.convert(asTimeOnly(123456789101L)), 0);
            assertEquals(8.6276544E7, (double) conversion.convert(asTimeOnly(-123456L)), 0);
            assertEquals(8810899.0, (double) conversion.convert(asTimeOnly(-123456789101L)), 0);
        }
        {
            Conversion conversion = conversionFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals(1.23456789101E11, (double) conversion.convert(asDateTime(123456789101L)), 0);
            assertEquals(-1.23456789101E11, (double) conversion.convert(asDateTime(-123456789101L)), 0);
        }
        {
            Conversion conversion = conversionFor(KEYWORD, to);
            assertNull(conversion.convert(null));
            assertEquals(1.0, (double) conversion.convert("1"), 0);
            assertEquals(0.0, (double) conversion.convert("-0"), 0);
            assertEquals(12.776, (double) conversion.convert("12.776"), 0.00001);
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("0xff"));
            assertEquals("cannot cast [0xff] to [double]", e.getMessage());
        }
    }

    public void testConversionToBoolean() {
        DataType to = BOOLEAN;
        {
            Conversion conversion = conversionFor(FLOAT, to);
            assertNull(conversion.convert(null));
            assertEquals(true, conversion.convert(10.0f));
            assertEquals(true, conversion.convert(-10.0f));
            assertEquals(false, conversion.convert(0.0f));
        }
        {
            Conversion conversion = conversionFor(INTEGER, to);
            assertNull(conversion.convert(null));
            assertEquals(true, conversion.convert(10));
            assertEquals(true, conversion.convert(-10));
            assertEquals(false, conversion.convert(0));
        }
        {
            Conversion conversion = conversionFor(LONG, to);
            assertNull(conversion.convert(null));
            assertEquals(true, conversion.convert(10L));
            assertEquals(true, conversion.convert(-10L));
            assertEquals(false, conversion.convert(0L));
        }
        {
            Conversion conversion = conversionFor(DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals(true, conversion.convert(10.0d));
            assertEquals(true, conversion.convert(-10.0d));
            assertEquals(false, conversion.convert(0.0d));
        }
        {
            Conversion conversion = conversionFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals(true, conversion.convert(asDateOnly(123456789101L)));
            assertEquals(true, conversion.convert(asDateOnly(-123456789101L)));
            assertEquals(false, conversion.convert(asDateOnly(0L)));
        }
        {
            Conversion conversion = conversionFor(TIME, to);
            assertNull(conversion.convert(null));
            assertEquals(true, conversion.convert(asTimeOnly(123456789101L)));
            assertEquals(true, conversion.convert(asTimeOnly(-123456789101L)));
            assertEquals(false, conversion.convert(asTimeOnly(0L)));
        }
        {
            Conversion conversion = conversionFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals(true, conversion.convert(asDateTime(123456789101L)));
            assertEquals(true, conversion.convert(asDateTime(-123456789101L)));
            assertEquals(false, conversion.convert(asDateTime(0L)));
        }
        {
            Conversion conversion = conversionFor(KEYWORD, to);
            assertNull(conversion.convert(null));
            // We only handled upper and lower case true and false
            assertEquals(true, conversion.convert("true"));
            assertEquals(false, conversion.convert("false"));
            assertEquals(true, conversion.convert("True"));
            assertEquals(false, conversion.convert("fAlSe"));
            // Everything else should fail
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("10"));
            assertEquals("cannot cast [10] to [boolean]", e.getMessage());
            e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("-1"));
            assertEquals("cannot cast [-1] to [boolean]", e.getMessage());
            e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("0"));
            assertEquals("cannot cast [0] to [boolean]", e.getMessage());
            e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("blah"));
            assertEquals("cannot cast [blah] to [boolean]", e.getMessage());
            e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("Yes"));
            assertEquals("cannot cast [Yes] to [boolean]", e.getMessage());
            e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("nO"));
            assertEquals("cannot cast [nO] to [boolean]", e.getMessage());
        }
    }

    public void testConversionToInt() {
        DataType to = INTEGER;
        {
            Conversion conversion = conversionFor(DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals(10, conversion.convert(10.0));
            assertEquals(10, conversion.convert(10.1));
            assertEquals(11, conversion.convert(10.6));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert(Long.MAX_VALUE));
            assertEquals("[" + Long.MAX_VALUE + "] out of [integer] range", e.getMessage());
        }
        {
            Conversion conversion = conversionFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals(0, conversion.convert(asDateOnly(12345678L)));
            assertEquals(86400000, conversion.convert(asDateOnly(123456789L)));
            assertEquals(172800000, conversion.convert(asDateOnly(223456789L)));
            assertEquals(-172800000, conversion.convert(asDateOnly(-123456789L)));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert(asDateOnly(Long.MAX_VALUE)));
            assertEquals("[9223372036828800000] out of [integer] range", e.getMessage());
        }
        {
            Conversion conversion = conversionFor(TIME, to);
            assertNull(conversion.convert(null));
            assertEquals(123456, conversion.convert(asTimeOnly(123456L)));
            assertEquals(77589101, conversion.convert(asTimeOnly(123456789101L)));
            assertEquals(86276544, conversion.convert(asTimeOnly(-123456L)));
            assertEquals(8810899, conversion.convert(asTimeOnly(-123456789101L)));
            assertEquals(25975807, conversion.convert(asTimeOnly(Long.MAX_VALUE)));
        }
        {
            Conversion conversion = conversionFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals(12345678, conversion.convert(asDateTime(12345678L)));
            assertEquals(223456789, conversion.convert(asDateTime(223456789L)));
            assertEquals(-123456789, conversion.convert(asDateTime(-123456789L)));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert(asDateTime(Long.MAX_VALUE)));
            assertEquals("[" + Long.MAX_VALUE + "] out of [integer] range", e.getMessage());
        }
    }

    public void testConversionToShort() {
        DataType to = SHORT;
        {
            Conversion conversion = conversionFor(DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals((short) 10, conversion.convert(10.0));
            assertEquals((short) 10, conversion.convert(10.1));
            assertEquals((short) 11, conversion.convert(10.6));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert(Integer.MAX_VALUE));
            assertEquals("[" + Integer.MAX_VALUE + "] out of [short] range", e.getMessage());
        }
        {
            Conversion conversion = conversionFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals((short) 0, conversion.convert(asDateOnly(12345678L)));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert(asDateOnly(123456789L)));
            assertEquals("[86400000] out of [short] range", e.getMessage());
        }
        {
            Conversion conversion = conversionFor(TIME, to);
            assertNull(conversion.convert(null));
            assertEquals((short) 12345, conversion.convert(asTimeOnly(12345L)));
            Exception e1 = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert(asTimeOnly(-123456789L)));
            assertEquals("[49343211] out of [short] range", e1.getMessage());
            Exception e2 = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert(asTimeOnly(123456789L)));
            assertEquals("[37056789] out of [short] range", e2.getMessage());
        }
        {
            Conversion conversion = conversionFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals((short) 12345, conversion.convert(asDateTime(12345L)));
            assertEquals((short) -12345, conversion.convert(asDateTime(-12345L)));
            Exception e = expectThrows(SqlIllegalArgumentException.class,
                () -> conversion.convert(asDateTime(Integer.MAX_VALUE)));
            assertEquals("[" + Integer.MAX_VALUE + "] out of [short] range", e.getMessage());
        }
    }

    public void testConversionToByte() {
        DataType to = BYTE;
        {
            Conversion conversion = conversionFor(DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals((byte) 10, conversion.convert(10.0));
            assertEquals((byte) 10, conversion.convert(10.1));
            assertEquals((byte) 11, conversion.convert(10.6));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert(Short.MAX_VALUE));
            assertEquals("[" + Short.MAX_VALUE + "] out of [byte] range", e.getMessage());
        }
        {
            Conversion conversion = conversionFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals((byte) 0, conversion.convert(asDateOnly(12345678L)));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert(asDateOnly(123456789L)));
            assertEquals("[86400000] out of [byte] range", e.getMessage());
        }
        {
            Conversion conversion = conversionFor(TIME, to);
            assertNull(conversion.convert(null));
            assertEquals((byte) 123, conversion.convert(asTimeOnly(123L)));
            Exception e1 = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert(asTimeOnly(-123L)));
            assertEquals("[86399877] out of [byte] range", e1.getMessage());
            Exception e2 = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert(asTimeOnly(123456789L)));
            assertEquals("[37056789] out of [byte] range", e2.getMessage());
        }
        {
            Conversion conversion = conversionFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals((byte) 123, conversion.convert(asDateTime(123L)));
            assertEquals((byte) -123, conversion.convert(asDateTime(-123L)));
            Exception e = expectThrows(SqlIllegalArgumentException.class,
                () -> conversion.convert(asDateTime(Integer.MAX_VALUE)));
            assertEquals("[" + Integer.MAX_VALUE + "] out of [byte] range", e.getMessage());
        }
    }

    public void testConversionToNull() {
        Conversion conversion = conversionFor(DOUBLE, NULL);
        assertNull(conversion.convert(null));
        assertNull(conversion.convert(10.0));
    }

    public void testConversionFromNull() {
        Conversion conversion = conversionFor(NULL, INTEGER);
        assertNull(conversion.convert(null));
        assertNull(conversion.convert(10));
    }

    public void testConversionToIdentity() {
        Conversion conversion = conversionFor(INTEGER, INTEGER);
        assertNull(conversion.convert(null));
        assertEquals(10, conversion.convert(10));
    }

    public void testCommonType() {
        assertEquals(BOOLEAN, commonType(BOOLEAN, NULL));
        assertEquals(BOOLEAN, commonType(NULL, BOOLEAN));
        assertEquals(BOOLEAN, commonType(BOOLEAN, BOOLEAN));
        assertEquals(NULL, commonType(NULL, NULL));
        assertEquals(INTEGER, commonType(INTEGER, KEYWORD));
        assertEquals(LONG, commonType(TEXT, LONG));
        assertNull(commonType(TEXT, KEYWORD));
        assertEquals(SHORT, commonType(SHORT, BYTE));
        assertEquals(FLOAT, commonType(BYTE, FLOAT));
        assertEquals(FLOAT, commonType(FLOAT, INTEGER));
        assertEquals(DOUBLE, commonType(DOUBLE, FLOAT));

        // numeric and intervals
        assertEquals(INTERVAL_YEAR_TO_MONTH, commonType(INTERVAL_YEAR_TO_MONTH, LONG));
        assertEquals(INTERVAL_HOUR_TO_MINUTE, commonType(INTEGER, INTERVAL_HOUR_TO_MINUTE));

        // dates/datetimes and intervals
        assertEquals(DATETIME, commonType(DATE, DATETIME));
        assertEquals(DATETIME, commonType(DATETIME, DATE));
        assertEquals(DATETIME, commonType(TIME, DATETIME));
        assertEquals(DATETIME, commonType(DATETIME, TIME));
        assertEquals(DATETIME, commonType(DATETIME, randomInterval()));
        assertEquals(DATETIME, commonType(randomInterval(), DATETIME));
        assertEquals(DATETIME, commonType(DATE, TIME));
        assertEquals(DATETIME, commonType(TIME, DATE));
        assertEquals(DATE, commonType(DATE, randomInterval()));
        assertEquals(DATE, commonType(randomInterval(), DATE));
        assertEquals(TIME, commonType(TIME, randomInterval()));
        assertEquals(TIME, commonType(randomInterval(), TIME));

        assertEquals(INTERVAL_YEAR_TO_MONTH, commonType(INTERVAL_YEAR_TO_MONTH, INTERVAL_MONTH));
        assertEquals(INTERVAL_HOUR_TO_SECOND, commonType(INTERVAL_HOUR_TO_MINUTE, INTERVAL_HOUR_TO_SECOND));
        assertNull(commonType(INTERVAL_SECOND, INTERVAL_YEAR));
    }

    public void testEsDataTypes() {
        for (DataType type : values()) {
            if (type != DATE) { // Doesn't have a corresponding type in ES
                assertEquals(type, fromTypeName(type.typeName));
            }
        }
    }

    public void testConversionToUnsupported() {
            Exception e = expectThrows(SqlIllegalArgumentException.class,
                () -> conversionFor(INTEGER, UNSUPPORTED));
            assertEquals("cannot convert from [integer] to [unsupported]", e.getMessage());
    }

    public void testStringToIp() {
        Conversion conversion = conversionFor(KEYWORD, IP);
        assertNull(conversion.convert(null));
        assertEquals("192.168.1.1", conversion.convert("192.168.1.1"));
        Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("10.1.1.300"));
        assertEquals("[10.1.1.300] is not a valid IPv4 or IPv6 address", e.getMessage());
    }

    public void testIpToString() {
        Source s = new Source(Location.EMPTY, "10.0.0.1");
        Conversion ipToString = conversionFor(IP, KEYWORD);
        assertEquals("10.0.0.1", ipToString.convert(new Literal(s, "10.0.0.1", IP)));
        Conversion stringToIp = conversionFor(KEYWORD, IP);
        assertEquals("10.0.0.1", ipToString.convert(stringToIp.convert(Literal.of(s, "10.0.0.1"))));
    }

    private DataType randomInterval() {
        return randomFrom(Stream.of(DataType.values()).filter(DataTypes::isInterval).collect(Collectors.toList()));
    }
}
