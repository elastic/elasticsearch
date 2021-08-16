/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.type;

import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Location;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.Converter;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.util.DateUtils;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.BYTE;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.FLOAT;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.IP;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.NULL;
import static org.elasticsearch.xpack.ql.type.DataTypes.SHORT;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSUPPORTED;
import static org.elasticsearch.xpack.ql.type.DateUtils.asDateTime;
import static org.elasticsearch.xpack.sql.type.SqlDataTypeConverter.commonType;
import static org.elasticsearch.xpack.sql.type.SqlDataTypeConverter.converterFor;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.DATE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_HOUR_TO_MINUTE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_HOUR_TO_SECOND;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_MONTH;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_SECOND;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_YEAR;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_YEAR_TO_MONTH;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.TIME;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.fromTypeName;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.types;
import static org.elasticsearch.xpack.sql.util.DateUtils.asDateOnly;
import static org.elasticsearch.xpack.sql.util.DateUtils.asDateTimeWithNanos;
import static org.elasticsearch.xpack.sql.util.DateUtils.asTimeOnly;

public class SqlDataTypeConverterTests extends ESTestCase {

    private static final ZoneId MINUS_5 = ZoneId.of("-05:00");

    public void testConversionToString() {
        DataType to = KEYWORD;
        {
            Converter conversion = converterFor(DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals("10.0", conversion.convert(10.0));
        }
        {
            Converter conversion = converterFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals("1973-11-29", conversion.convert(asDateOnly(123456789101L)));
            assertEquals("1966-02-02", conversion.convert(asDateOnly(-123456789101L)));
        }
        {
            Converter conversion = converterFor(TIME, to);
            assertNull(conversion.convert(null));
            assertEquals("00:02:03.456Z", conversion.convert(asTimeOnly(123456L)));
            assertEquals("21:33:09.101Z", conversion.convert(asTimeOnly(123456789101L)));
            assertEquals("23:57:56.544Z", conversion.convert(asTimeOnly(-123456L)));
            assertEquals("02:26:50.899Z", conversion.convert(asTimeOnly(-123456789101L)));
        }
        {
            Converter conversion = converterFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals("1973-11-29T21:33:09.101Z", conversion.convert(DateUtils.asDateTimeWithMillis(123456789101L)));
            assertEquals("1966-02-02T02:26:50.899Z", conversion.convert(DateUtils.asDateTimeWithMillis(-123456789101L)));
            assertEquals("2020-05-01T10:20:30.123456789Z", conversion.convert(asDateTimeWithNanos("2020-05-01T10:20:30.123456789Z")));
        }
        {
            Converter conversion = converterFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals("1973-11-29T16:33:09.101-05:00", conversion.convert(DateUtils.asDateTimeWithMillis(123456789101L, MINUS_5)));
            assertEquals("1966-02-01T21:26:50.899-05:00", conversion.convert(DateUtils.asDateTimeWithMillis(-123456789101L, MINUS_5)));
        }
    }

    /**
     * Test conversion to long.
     */
    public void testConversionToLong() {
        DataType to = LONG;
        {
            Converter conversion = converterFor(DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals(10L, conversion.convert(10.0));
            assertEquals(10L, conversion.convert(10.1));
            assertEquals(11L, conversion.convert(10.6));
            Exception e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert(Double.MAX_VALUE));
            assertEquals("[" + Double.MAX_VALUE + "] out of [long] range", e.getMessage());
        }
        {
            Converter conversion = converterFor(INTEGER, to);
            assertNull(conversion.convert(null));
            assertEquals(10L, conversion.convert(10));
            assertEquals(-134L, conversion.convert(-134));
        }
        {
            Converter conversion = converterFor(BOOLEAN, to);
            assertNull(conversion.convert(null));
            assertEquals(1L, conversion.convert(true));
            assertEquals(0L, conversion.convert(false));
        }
        {
            Converter conversion = converterFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals(123379200000L, conversion.convert(asDateOnly(123456789101L)));
            assertEquals(-123465600000L, conversion.convert(asDateOnly(-123456789101L)));
        }
        {
            Converter conversion = converterFor(TIME, to);
            assertNull(conversion.convert(null));
            assertEquals(123456L, conversion.convert(asTimeOnly(123456L)));
            assertEquals(77589101L, conversion.convert(asTimeOnly(123456789101L)));
            assertEquals(86276544L, conversion.convert(asTimeOnly(-123456L)));
            assertEquals(8810899L, conversion.convert(asTimeOnly(-123456789101L)));
        }
        {
            Converter conversion = converterFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals(123456789101L, conversion.convert(DateUtils.asDateTimeWithMillis(123456789101L)));
            assertEquals(-123456789101L, conversion.convert(DateUtils.asDateTimeWithMillis(-123456789101L)));
            // Nanos are ignored, only millis are used
            assertEquals(1588328430123L, conversion.convert(asDateTimeWithNanos("2020-05-01T10:20:30.123456789Z")));
        }
        {
            Converter conversion = converterFor(KEYWORD, to);
            assertNull(conversion.convert(null));
            assertEquals(1L, conversion.convert("1"));
            assertEquals(0L, conversion.convert("-0"));
            Exception e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert("0xff"));
            assertEquals("cannot cast [0xff] to [long]", e.getMessage());
        }
    }

    public void testConversionToDate() {
        DataType to = DATE;
        {
            Converter conversion = converterFor(DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals(date(10L), conversion.convert(10.0));
            assertEquals(date(10L), conversion.convert(10.1));
            assertEquals(date(11L), conversion.convert(10.6));
            Exception e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert(Double.MAX_VALUE));
            assertEquals("[" + Double.MAX_VALUE + "] out of [long] range", e.getMessage());
        }
        {
            Converter conversion = converterFor(INTEGER, to);
            assertNull(conversion.convert(null));
            assertEquals(date(10L), conversion.convert(10));
            assertEquals(date(-134L), conversion.convert(-134));
        }
        {
            Converter conversion = converterFor(BOOLEAN, to);
            assertNull(conversion.convert(null));
            assertEquals(date(1), conversion.convert(true));
            assertEquals(date(0), conversion.convert(false));
        }
        {
            assertNull(converterFor(TIME, to));
        }
        {
            Converter conversion = converterFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals(date(123456780000L), conversion.convert(DateUtils.asDateTimeWithMillis(123456789101L)));
            assertEquals(date(-123456789101L), conversion.convert(DateUtils.asDateTimeWithMillis(-123456789101L)));
        }
        {
            Converter conversion = converterFor(KEYWORD, to);
            assertNull(conversion.convert(null));

            assertEquals(date(1581292800000L), conversion.convert("2020-02-10T10:20"));
            assertEquals(date(-125908819200000L), conversion.convert("-2020-02-10T10:20:30.123"));
            assertEquals(date(1581292800000L), conversion.convert("2020-02-10T10:20:30.123456789"));

            assertEquals(date(1581292800000L), conversion.convert("2020-02-10 10:20"));
            assertEquals(date(-125908819200000L), conversion.convert("-2020-02-10 10:20:30.123"));
            assertEquals(date(1581292800000L), conversion.convert("2020-02-10 10:20:30.123456789"));

            assertEquals(date(1581292800000L), conversion.convert("2020-02-10T10:20+05:00"));
            assertEquals(date(-125908819200000L), conversion.convert("-2020-02-10T10:20:30.123-06:00"));
            assertEquals(date(1581292800000L), conversion.convert("2020-02-10T10:20:30.123456789+03:00"));

            assertEquals(date(1581292800000L), conversion.convert("2020-02-10 10:20+05:00"));
            assertEquals(date(-125908819200000L), conversion.convert("-2020-02-10 10:20:30.123-06:00"));
            assertEquals(date(1581292800000L), conversion.convert("2020-02-10 10:20:30.123456789+03:00"));

            assertEquals(date(11046514492800000L), conversion.convert("+352020-02-10 10:20:30.123456789+03:00"));

            // double check back and forth conversion
            ZonedDateTime zdt = org.elasticsearch.common.time.DateUtils.nowWithMillisResolution();
            Converter forward = converterFor(DATE, KEYWORD);
            Converter back = converterFor(KEYWORD, DATE);
            assertEquals(asDateOnly(zdt), back.convert(forward.convert(zdt)));
            Exception e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert("0xff"));
            assertEquals("cannot cast [0xff] to [date]: Text '0xff' could not be parsed at index 0", e.getMessage());
            e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert("2020-02-"));
            assertEquals("cannot cast [2020-02-] to [date]: Text '2020-02-' could not be parsed at index 8", e.getMessage());
            e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert("2020-"));
            assertEquals("cannot cast [2020-] to [date]: Text '2020-' could not be parsed at index 5", e.getMessage());
            e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert("-2020-02-"));
            assertEquals("cannot cast [-2020-02-] to [date]: Text '-2020-02-' could not be parsed at index 9", e.getMessage());
            e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert("-2020-"));
            assertEquals("cannot cast [-2020-] to [date]: Text '-2020-' could not be parsed at index 6", e.getMessage());
        }
    }

    public void testConversionToTime() {
        DataType to = TIME;
        {
            Converter conversion = converterFor(DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals(time(10L), conversion.convert(10.0));
            assertEquals(time(10L), conversion.convert(10.1));
            assertEquals(time(11L), conversion.convert(10.6));
            Exception e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert(Double.MAX_VALUE));
            assertEquals("[" + Double.MAX_VALUE + "] out of [long] range", e.getMessage());
        }
        {
            Converter conversion = converterFor(INTEGER, to);
            assertNull(conversion.convert(null));
            assertEquals(time(10L), conversion.convert(10));
            assertEquals(time(-134L), conversion.convert(-134));
        }
        {
            Converter conversion = converterFor(BOOLEAN, to);
            assertNull(conversion.convert(null));
            assertEquals(time(1), conversion.convert(true));
            assertEquals(time(0), conversion.convert(false));
        }
        {
            Converter conversion = converterFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals(time(123379200000L), conversion.convert(asDateOnly(123456789101L)));
            assertEquals(time(-123465600000L), conversion.convert(asDateOnly(-123456789101L)));
        }
        {
            Converter conversion = converterFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals(time(77589101L), conversion.convert(DateUtils.asDateTimeWithMillis(123456789101L)));
            assertEquals(time(8810899L), conversion.convert(DateUtils.asDateTimeWithMillis(-123456789101L)));
        }
        {
            Converter conversion = converterFor(KEYWORD, to);
            assertNull(conversion.convert(null));

            assertEquals(time(0L), conversion.convert("00:00:00Z"));
            assertEquals(time(1000L), conversion.convert("00:00:01Z"));
            assertEquals(time(1234L), conversion.convert("00:00:01.234Z"));
            assertEquals(time(63296789L).withOffsetSameInstant(ZoneOffset.ofHours(-5)), conversion.convert("12:34:56.789-05:00"));

            // double check back and forth conversion
            OffsetTime ot = org.elasticsearch.common.time.DateUtils.nowWithMillisResolution().toOffsetDateTime().toOffsetTime();
            Converter forward = converterFor(TIME, KEYWORD);
            Converter back = converterFor(KEYWORD, TIME);
            assertEquals(ot, back.convert(forward.convert(ot)));
            Exception e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert("0xff"));
            assertEquals("cannot cast [0xff] to [time]: Text '0xff' could not be parsed at index 0", e.getMessage());
        }
    }

    public void testConversionToDateTime() {
        DataType to = DATETIME;
        {
            Converter conversion = converterFor(DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals(asDateTime(10L), conversion.convert(10.0));
            assertEquals(asDateTime(10L), conversion.convert(10.1));
            assertEquals(asDateTime(11L), conversion.convert(10.6));
            Exception e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert(Double.MAX_VALUE));
            assertEquals("[" + Double.MAX_VALUE + "] out of [long] range", e.getMessage());
        }
        {
            Converter conversion = converterFor(INTEGER, to);
            assertNull(conversion.convert(null));
            assertEquals(asDateTime(10L), conversion.convert(10));
            assertEquals(asDateTime(-134L), conversion.convert(-134));
        }
        {
            Converter conversion = converterFor(BOOLEAN, to);
            assertNull(conversion.convert(null));
            assertEquals(asDateTime(1), conversion.convert(true));
            assertEquals(asDateTime(0), conversion.convert(false));
        }
        {
            Converter conversion = converterFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals(asDateTime(123379200000L), conversion.convert(asDateOnly(123456789101L)));
            assertEquals(asDateTime(-123465600000L), conversion.convert(asDateOnly(-123456789101L)));
        }
        {
            assertNull(converterFor(TIME, to));
        }
        {
            Converter conversion = converterFor(KEYWORD, to);
            assertNull(conversion.convert(null));

            assertEquals(asDateTime(0L), conversion.convert("1970-01-01"));
            assertEquals(asDateTime(1000L), conversion.convert("1970-01-01T00:00:01Z"));

            assertEquals(asDateTime(1483228800000L), conversion.convert("2017-01-01T00:00:00Z"));
            assertEquals(asDateTime(1483228800000L), conversion.convert("2017-01-01 00:00:00Z"));

            assertEquals(asDateTime(1483228800123L), conversion.convert("2017-01-01T00:00:00.123Z"));
            assertEquals(asDateTime(1483228800123L), conversion.convert("2017-01-01 00:00:00.123Z"));

            assertEquals(asDateTime(18000321L, MINUS_5), conversion.convert("1970-01-01T00:00:00.321-05:00"));
            assertEquals(asDateTime(18000321L, MINUS_5), conversion.convert("1970-01-01 00:00:00.321-05:00"));

            assertEquals(asDateTime(3849948162000321L, MINUS_5), conversion.convert("+123970-01-01T00:00:00.321-05:00"));
            assertEquals(asDateTime(3849948162000321L, MINUS_5), conversion.convert("+123970-01-01 00:00:00.321-05:00"));

            assertEquals(asDateTime(-818587277999679L, MINUS_5), conversion.convert("-23970-01-01T00:00:00.321-05:00"));
            assertEquals(asDateTime(-818587277999679L, MINUS_5), conversion.convert("-23970-01-01 00:00:00.321-05:00"));

            // double check back and forth conversion
            ZonedDateTime dt = org.elasticsearch.common.time.DateUtils.nowWithMillisResolution();
            Converter forward = converterFor(DATETIME, KEYWORD);
            Converter back = converterFor(KEYWORD, DATETIME);
            assertEquals(dt, back.convert(forward.convert(dt)));
            Exception e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert("0xff"));
            assertEquals("cannot cast [0xff] to [datetime]: Text '0xff' could not be parsed at index 0",
                    e.getMessage());
        }
    }

    public void testConversionToFloat() {
        DataType to = FLOAT;
        {
            Converter conversion = converterFor(DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals(10.0f, (float) conversion.convert(10.0d), 0.00001);
            assertEquals(10.1f, (float) conversion.convert(10.1d), 0.00001);
            assertEquals(10.6f, (float) conversion.convert(10.6d), 0.00001);
        }
        {
            Converter conversion = converterFor(INTEGER, to);
            assertNull(conversion.convert(null));
            assertEquals(10.0f, (float) conversion.convert(10), 0.00001);
            assertEquals(-134.0f, (float) conversion.convert(-134), 0.00001);
        }
        {
            Converter conversion = converterFor(BOOLEAN, to);
            assertNull(conversion.convert(null));
            assertEquals(1.0f, (float) conversion.convert(true), 0);
            assertEquals(0.0f, (float) conversion.convert(false), 0);
        }
        {
            Converter conversion = converterFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals(1.233792E11f, (float) conversion.convert(asDateOnly(123456789101L)), 0);
            assertEquals(-1.234656E11f, (float) conversion.convert(asDateOnly(-123456789101L)), 0);
        }
        {
            Converter conversion = converterFor(TIME, to);
            assertNull(conversion.convert(null));
            assertEquals(123456.0f, (float) conversion.convert(asTimeOnly(123456L)), 0);
            assertEquals(7.7589104E7f, (float) conversion.convert(asTimeOnly(123456789101L)), 0);
            assertEquals(8.6276544E7f, (float) conversion.convert(asTimeOnly(-123456L)), 0);
            assertEquals(8810899.0f, (float) conversion.convert(asTimeOnly(-123456789101L)), 0);
        }
        {
            Converter conversion = converterFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals(1.23456789101E11f, (float) conversion.convert(DateUtils.asDateTimeWithMillis(123456789101L)), 0);
            assertEquals(-1.23456789101E11f, (float) conversion.convert(DateUtils.asDateTimeWithMillis(-123456789101L)), 0);
            // Nanos are ignored, only millis are used
            assertEquals(1.5883284E12f, conversion.convert(asDateTimeWithNanos("2020-05-01T10:20:30.123456789Z")));
        }
        {
            Converter conversion = converterFor(KEYWORD, to);
            assertNull(conversion.convert(null));
            assertEquals(1.0f, (float) conversion.convert("1"), 0);
            assertEquals(0.0f, (float) conversion.convert("-0"), 0);
            assertEquals(12.776f, (float) conversion.convert("12.776"), 0.00001);
            Exception e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert("0xff"));
            assertEquals("cannot cast [0xff] to [float]", e.getMessage());
        }
    }

    public void testConversionToDouble() {
        DataType to = DOUBLE;
        {
            Converter conversion = converterFor(FLOAT, to);
            assertNull(conversion.convert(null));
            assertEquals(10.0, (double) conversion.convert(10.0f), 0.00001);
            assertEquals(10.1, (double) conversion.convert(10.1f), 0.00001);
            assertEquals(10.6, (double) conversion.convert(10.6f), 0.00001);
        }
        {
            Converter conversion = converterFor(INTEGER, to);
            assertNull(conversion.convert(null));
            assertEquals(10.0, (double) conversion.convert(10), 0.00001);
            assertEquals(-134.0, (double) conversion.convert(-134), 0.00001);
        }
        {
            Converter conversion = converterFor(BOOLEAN, to);
            assertNull(conversion.convert(null));
            assertEquals(1.0, (double) conversion.convert(true), 0);
            assertEquals(0.0, (double) conversion.convert(false), 0);
        }
        {
            Converter conversion = converterFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals(1.233792E11, (double) conversion.convert(asDateOnly(123456789101L)), 0);
            assertEquals(-1.234656E11, (double) conversion.convert(asDateOnly(-123456789101L)), 0);
        }
        {
            Converter conversion = converterFor(TIME, to);
            assertNull(conversion.convert(null));
            assertEquals(123456.0, (double) conversion.convert(asTimeOnly(123456L)), 0);
            assertEquals(7.7589101E7, (double) conversion.convert(asTimeOnly(123456789101L)), 0);
            assertEquals(8.6276544E7, (double) conversion.convert(asTimeOnly(-123456L)), 0);
            assertEquals(8810899.0, (double) conversion.convert(asTimeOnly(-123456789101L)), 0);
        }
        {
            Converter conversion = converterFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals(1.23456789101E11, (double) conversion.convert(DateUtils.asDateTimeWithMillis(123456789101L)), 0);
            assertEquals(-1.23456789101E11, (double) conversion.convert(DateUtils.asDateTimeWithMillis(-123456789101L)), 0);
            // Nanos are ignored, only millis are used
            assertEquals(1.588328430123E12, conversion.convert(asDateTimeWithNanos("2020-05-01T10:20:30.123456789Z")));
        }
        {
            Converter conversion = converterFor(KEYWORD, to);
            assertNull(conversion.convert(null));
            assertEquals(1.0, (double) conversion.convert("1"), 0);
            assertEquals(0.0, (double) conversion.convert("-0"), 0);
            assertEquals(12.776, (double) conversion.convert("12.776"), 0.00001);
            Exception e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert("0xff"));
            assertEquals("cannot cast [0xff] to [double]", e.getMessage());
        }
    }

    public void testConversionToBoolean() {
        DataType to = BOOLEAN;
        {
            Converter conversion = converterFor(FLOAT, to);
            assertNull(conversion.convert(null));
            assertEquals(true, conversion.convert(10.0f));
            assertEquals(true, conversion.convert(-10.0f));
            assertEquals(false, conversion.convert(0.0f));
        }
        {
            Converter conversion = converterFor(INTEGER, to);
            assertNull(conversion.convert(null));
            assertEquals(true, conversion.convert(10));
            assertEquals(true, conversion.convert(-10));
            assertEquals(false, conversion.convert(0));
        }
        {
            Converter conversion = converterFor(LONG, to);
            assertNull(conversion.convert(null));
            assertEquals(true, conversion.convert(10L));
            assertEquals(true, conversion.convert(-10L));
            assertEquals(false, conversion.convert(0L));
        }
        {
            Converter conversion = converterFor(DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals(true, conversion.convert(10.0d));
            assertEquals(true, conversion.convert(-10.0d));
            assertEquals(false, conversion.convert(0.0d));
        }
        {
            Converter conversion = converterFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals(true, conversion.convert(asDateOnly(123456789101L)));
            assertEquals(true, conversion.convert(asDateOnly(-123456789101L)));
            assertEquals(false, conversion.convert(asDateOnly(0L)));
        }
        {
            Converter conversion = converterFor(TIME, to);
            assertNull(conversion.convert(null));
            assertEquals(true, conversion.convert(asTimeOnly(123456789101L)));
            assertEquals(true, conversion.convert(asTimeOnly(-123456789101L)));
            assertEquals(false, conversion.convert(asTimeOnly(0L)));
        }
        {
            Converter conversion = converterFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals(true, conversion.convert(DateUtils.asDateTimeWithMillis(123456789101L)));
            assertEquals(true, conversion.convert(DateUtils.asDateTimeWithMillis(-123456789101L)));
            assertEquals(false, conversion.convert(DateUtils.asDateTimeWithMillis(0L)));
        }
        {
            Converter conversion = converterFor(KEYWORD, to);
            assertNull(conversion.convert(null));
            // We only handled upper and lower case true and false
            assertEquals(true, conversion.convert("true"));
            assertEquals(false, conversion.convert("false"));
            assertEquals(true, conversion.convert("True"));
            assertEquals(false, conversion.convert("fAlSe"));
            // Everything else should fail
            Exception e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert("10"));
            assertEquals("cannot cast [10] to [boolean]", e.getMessage());
            e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert("-1"));
            assertEquals("cannot cast [-1] to [boolean]", e.getMessage());
            e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert("0"));
            assertEquals("cannot cast [0] to [boolean]", e.getMessage());
            e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert("blah"));
            assertEquals("cannot cast [blah] to [boolean]", e.getMessage());
            e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert("Yes"));
            assertEquals("cannot cast [Yes] to [boolean]", e.getMessage());
            e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert("nO"));
            assertEquals("cannot cast [nO] to [boolean]", e.getMessage());
        }
    }

    public void testConversionToInt() {
        DataType to = INTEGER;
        {
            Converter conversion = converterFor(DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals(10, conversion.convert(10.0));
            assertEquals(10, conversion.convert(10.1));
            assertEquals(11, conversion.convert(10.6));
            Exception e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert(Long.MAX_VALUE));
            assertEquals("[" + Long.MAX_VALUE + "] out of [integer] range", e.getMessage());
        }
        {
            Converter conversion = converterFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals(0, conversion.convert(asDateOnly(12345678L)));
            assertEquals(86400000, conversion.convert(asDateOnly(123456789L)));
            assertEquals(172800000, conversion.convert(asDateOnly(223456789L)));
            assertEquals(-172800000, conversion.convert(asDateOnly(-123456789L)));
            Exception e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert(asDateOnly(Long.MAX_VALUE)));
            assertEquals("[9223372036828800000] out of [integer] range", e.getMessage());
        }
        {
            Converter conversion = converterFor(TIME, to);
            assertNull(conversion.convert(null));
            assertEquals(123456, conversion.convert(asTimeOnly(123456L)));
            assertEquals(77589101, conversion.convert(asTimeOnly(123456789101L)));
            assertEquals(86276544, conversion.convert(asTimeOnly(-123456L)));
            assertEquals(8810899, conversion.convert(asTimeOnly(-123456789101L)));
            assertEquals(25975807, conversion.convert(asTimeOnly(Long.MAX_VALUE)));
        }
        {
            Converter conversion = converterFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals(12345678, conversion.convert(DateUtils.asDateTimeWithMillis(12345678L)));
            assertEquals(223456789, conversion.convert(DateUtils.asDateTimeWithMillis(223456789L)));
            assertEquals(-123456789, conversion.convert(DateUtils.asDateTimeWithMillis(-123456789L)));
            // Nanos are ignored, only millis are used
            assertEquals(62123, conversion.convert(asDateTimeWithNanos("1970-01-01T00:01:02.123456789Z")));
            Exception e = expectThrows(
                QlIllegalArgumentException.class,
                () -> conversion.convert(DateUtils.asDateTimeWithMillis(Long.MAX_VALUE))
            );
            assertEquals("[" + Long.MAX_VALUE + "] out of [integer] range", e.getMessage());
        }
    }

    public void testConversionToShort() {
        DataType to = SHORT;
        {
            Converter conversion = converterFor(DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals((short) 10, conversion.convert(10.0));
            assertEquals((short) 10, conversion.convert(10.1));
            assertEquals((short) 11, conversion.convert(10.6));
            Exception e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert(Integer.MAX_VALUE));
            assertEquals("[" + Integer.MAX_VALUE + "] out of [short] range", e.getMessage());
        }
        {
            Converter conversion = converterFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals((short) 0, conversion.convert(asDateOnly(12345678L)));
            Exception e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert(asDateOnly(123456789L)));
            assertEquals("[86400000] out of [short] range", e.getMessage());
        }
        {
            Converter conversion = converterFor(TIME, to);
            assertNull(conversion.convert(null));
            assertEquals((short) 12345, conversion.convert(asTimeOnly(12345L)));
            Exception e1 = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert(asTimeOnly(-123456789L)));
            assertEquals("[49343211] out of [short] range", e1.getMessage());
            Exception e2 = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert(asTimeOnly(123456789L)));
            assertEquals("[37056789] out of [short] range", e2.getMessage());
        }
        {
            Converter conversion = converterFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals((short) 12345, conversion.convert(DateUtils.asDateTimeWithMillis(12345L)));
            assertEquals((short) -12345, conversion.convert(DateUtils.asDateTimeWithMillis(-12345L)));
            // Nanos are ignored, only millis are used
            assertEquals((short) 1123, conversion.convert(asDateTimeWithNanos("1970-01-01T00:00:01.123456789Z")));
            Exception e = expectThrows(
                QlIllegalArgumentException.class,
                () -> conversion.convert(DateUtils.asDateTimeWithMillis(Integer.MAX_VALUE))
            );
            assertEquals("[" + Integer.MAX_VALUE + "] out of [short] range", e.getMessage());
        }
    }

    public void testConversionToByte() {
        DataType to = BYTE;
        {
            Converter conversion = converterFor(DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals((byte) 10, conversion.convert(10.0));
            assertEquals((byte) 10, conversion.convert(10.1));
            assertEquals((byte) 11, conversion.convert(10.6));
            Exception e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert(Short.MAX_VALUE));
            assertEquals("[" + Short.MAX_VALUE + "] out of [byte] range", e.getMessage());
        }
        {
            Converter conversion = converterFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals((byte) 0, conversion.convert(asDateOnly(12345678L)));
            Exception e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert(asDateOnly(123456789L)));
            assertEquals("[86400000] out of [byte] range", e.getMessage());
        }
        {
            Converter conversion = converterFor(TIME, to);
            assertNull(conversion.convert(null));
            assertEquals((byte) 123, conversion.convert(asTimeOnly(123L)));
            Exception e1 = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert(asTimeOnly(-123L)));
            assertEquals("[86399877] out of [byte] range", e1.getMessage());
            Exception e2 = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert(asTimeOnly(123456789L)));
            assertEquals("[37056789] out of [byte] range", e2.getMessage());
        }
        {
            Converter conversion = converterFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals((byte) 123, conversion.convert(DateUtils.asDateTimeWithMillis(123L)));
            assertEquals((byte) -123, conversion.convert(DateUtils.asDateTimeWithMillis(-123L)));
            // Nanos are ignored, only millis are used
            assertEquals((byte) 123, conversion.convert(asDateTimeWithNanos("1970-01-01T00:00:00.123456789Z")));
            Exception e = expectThrows(
                QlIllegalArgumentException.class,
                () -> conversion.convert(DateUtils.asDateTimeWithMillis(Integer.MAX_VALUE))
            );
            assertEquals("[" + Integer.MAX_VALUE + "] out of [byte] range", e.getMessage());
        }
    }

    public void testConversionToNull() {
        Converter conversion = converterFor(DOUBLE, NULL);
        assertNull(conversion.convert(null));
        assertNull(conversion.convert(10.0));
    }

    public void testConversionFromNull() {
        Converter conversion = converterFor(NULL, INTEGER);
        assertNull(conversion.convert(null));
        assertNull(conversion.convert(10));
    }

    public void testConversionToIdentity() {
        Converter conversion = converterFor(INTEGER, INTEGER);
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
        assertEquals(SHORT, commonType(SHORT, BYTE));
        assertEquals(FLOAT, commonType(BYTE, FLOAT));
        assertEquals(FLOAT, commonType(FLOAT, INTEGER));
        assertEquals(DOUBLE, commonType(DOUBLE, FLOAT));

        // strings
        assertEquals(TEXT, commonType(TEXT, KEYWORD));
        assertEquals(TEXT, commonType(KEYWORD, TEXT));

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
        assertEquals(DATE, commonType(DATE, INTERVAL_YEAR));
        assertEquals(DATETIME, commonType(DATE, INTERVAL_HOUR_TO_MINUTE));
        assertEquals(TIME, commonType(TIME, randomInterval()));
        assertEquals(TIME, commonType(randomInterval(), TIME));

        assertEquals(INTERVAL_YEAR_TO_MONTH, commonType(INTERVAL_YEAR_TO_MONTH, INTERVAL_MONTH));
        assertEquals(INTERVAL_HOUR_TO_SECOND, commonType(INTERVAL_HOUR_TO_MINUTE, INTERVAL_HOUR_TO_SECOND));
        assertNull(commonType(INTERVAL_SECOND, INTERVAL_YEAR));
    }

    public void testEsDataTypes() {
        for (DataType type : types()) {
            if (type != DATE) { // Doesn't have a corresponding type in ES
                assertEquals(type, fromTypeName(type.typeName()));
            }
        }
    }

    public void testConversionToUnsupported() {
        assertNull(converterFor(INTEGER, UNSUPPORTED));
    }

    public void testStringToIp() {
        Converter conversion = converterFor(KEYWORD, IP);
        assertNull(conversion.convert(null));
        assertEquals("192.168.1.1", conversion.convert("192.168.1.1"));
        Exception e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert("10.1.1.300"));
        assertEquals("[10.1.1.300] is not a valid IPv4 or IPv6 address", e.getMessage());
    }

    public void testIpToString() {
        Source s = new Source(Location.EMPTY, "10.0.0.1");
        Converter ipToString = converterFor(IP, KEYWORD);
        assertEquals("10.0.0.1", ipToString.convert(new Literal(s, "10.0.0.1", IP)));
        Converter stringToIp = converterFor(KEYWORD, IP);
        assertEquals("10.0.0.1", ipToString.convert(stringToIp.convert(new Literal(s, "10.0.0.1", KEYWORD))));
    }

    private DataType randomInterval() {
        return randomFrom(SqlDataTypes.types().stream()
                .filter(SqlDataTypes::isInterval)
                .collect(toList()));
    }

    static ZonedDateTime date(long millisSinceEpoch) {
        return DateUtils.asDateOnly(millisSinceEpoch);
    }

    static OffsetTime time(long millisSinceEpoch) {
        return DateUtils.asTimeOnly(millisSinceEpoch);
    }
}
