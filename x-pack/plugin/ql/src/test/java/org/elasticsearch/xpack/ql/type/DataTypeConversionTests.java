/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.type;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Location;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZonedDateTime;

import static org.elasticsearch.xpack.ql.type.DataTypeConverter.commonType;
import static org.elasticsearch.xpack.ql.type.DataTypeConverter.converterFor;
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

public class DataTypeConversionTests extends ESTestCase {

    public void testConversionToString() {
        DataType to = KEYWORD;
        {
            Converter conversion = converterFor(DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals("10.0", conversion.convert(10.0));
        }
        {
            Converter conversion = converterFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals("1973-11-29T21:33:09.101Z", conversion.convert(asDateTime(123456789101L)));
            assertEquals("1966-02-02T02:26:50.899Z", conversion.convert(asDateTime(-123456789101L)));
            assertEquals("2020-05-01T10:20:30.123456789Z", conversion.convert(DateUtils.asDateTime("2020-05-01T10:20:30.123456789Z")));
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
            Converter conversion = converterFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals(123456789101L, conversion.convert(asDateTime(123456789101L)));
            assertEquals(-123456789101L, conversion.convert(asDateTime(-123456789101L)));
            // Nanos are ignored, only millis are used
            assertEquals(1588328430123L, conversion.convert(DateUtils.asDateTime("2020-05-01T10:20:30.123456789Z")));
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
            Converter conversion = converterFor(KEYWORD, to);
            assertNull(conversion.convert(null));

            assertEquals(asDateTime(0L), conversion.convert("1970-01-01"));
            assertEquals(asDateTime(1000L), conversion.convert("1970-01-01T00:00:01Z"));

            assertEquals(asDateTime(1483228800000L), conversion.convert("2017-01-01T00:00:00Z"));
            assertEquals(asDateTime(1483228800000L), conversion.convert("2017-01-01 00:00:00Z"));

            assertEquals(asDateTime(1483228800123L), conversion.convert("2017-01-01T00:00:00.123Z"));
            assertEquals(asDateTime(1483228800123L), conversion.convert("2017-01-01 00:00:00.123Z"));

            assertEquals(asDateTime(18000321L), conversion.convert("1970-01-01T00:00:00.321-05:00"));
            assertEquals(asDateTime(18000321L), conversion.convert("1970-01-01 00:00:00.321-05:00"));

            assertEquals(asDateTime(3849948162000321L), conversion.convert("+123970-01-01T00:00:00.321-05:00"));
            assertEquals(asDateTime(3849948162000321L), conversion.convert("+123970-01-01 00:00:00.321-05:00"));

            assertEquals(asDateTime(-818587277999679L), conversion.convert("-23970-01-01T00:00:00.321-05:00"));
            assertEquals(asDateTime(-818587277999679L), conversion.convert("-23970-01-01 00:00:00.321-05:00"));

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
            Converter conversion = converterFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals(1.23456789101E11f, (float) conversion.convert(asDateTime(123456789101L)), 0);
            assertEquals(-1.23456789101E11f, (float) conversion.convert(asDateTime(-123456789101L)), 0);
            // Nanos are ignored, only millis are used
            assertEquals(1.5883284E12f, conversion.convert(DateUtils.asDateTime("2020-05-01T10:20:30.123456789Z")));
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
            Converter conversion = converterFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals(1.23456789101E11, (double) conversion.convert(asDateTime(123456789101L)), 0);
            assertEquals(-1.23456789101E11, (double) conversion.convert(asDateTime(-123456789101L)), 0);
            // Nanos are ignored, only millis are used
            assertEquals(1.588328430123E12, conversion.convert(DateUtils.asDateTime("2020-05-01T10:20:30.123456789Z")));
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
            Converter conversion = converterFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals(true, conversion.convert(asDateTime(123456789101L)));
            assertEquals(true, conversion.convert(asDateTime(-123456789101L)));
            assertEquals(false, conversion.convert(asDateTime(0L)));
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
            Converter conversion = converterFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals(12345678, conversion.convert(asDateTime(12345678L)));
            assertEquals(223456789, conversion.convert(asDateTime(223456789L)));
            assertEquals(-123456789, conversion.convert(asDateTime(-123456789L)));
            // Nanos are ignored, only millis are used
            assertEquals(62123, conversion.convert(DateUtils.asDateTime("1970-01-01T00:01:02.123456789Z")));
            Exception e = expectThrows(QlIllegalArgumentException.class, () -> conversion.convert(asDateTime(Long.MAX_VALUE)));
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
            Converter conversion = converterFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals((short) 12345, conversion.convert(asDateTime(12345L)));
            assertEquals((short) -12345, conversion.convert(asDateTime(-12345L)));
            // Nanos are ignored, only millis are used
            assertEquals((short) 1123, conversion.convert(DateUtils.asDateTime("1970-01-01T00:00:01.123456789Z")));
            Exception e = expectThrows(QlIllegalArgumentException.class,
                () -> conversion.convert(asDateTime(Integer.MAX_VALUE)));
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
            Converter conversion = converterFor(DATETIME, to);
            assertNull(conversion.convert(null));
            assertEquals((byte) 123, conversion.convert(asDateTime(123L)));
            assertEquals((byte) -123, conversion.convert(asDateTime(-123L)));
            // Nanos are ignored, only millis are used
            assertEquals((byte) 123, conversion.convert(DateUtils.asDateTime("1970-01-01T00:00:00.123456789Z")));
            Exception e = expectThrows(QlIllegalArgumentException.class,
                () -> conversion.convert(asDateTime(Integer.MAX_VALUE)));
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
    }

    public void testEsDataTypes() {
        for (DataType type : DataTypes.types()) {
            assertEquals(type, DataTypes.fromTypeName(type.typeName()));
        }
    }

    public void testConversionToUnsupported() {
        Exception e = expectThrows(QlIllegalArgumentException.class,
                () -> DataTypeConverter.convert(Integer.valueOf(1), UNSUPPORTED));
        assertEquals("cannot convert from [1], type [integer] to [unsupported]", e.getMessage());
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
}
