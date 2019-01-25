/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.type.DataTypeConversion.Conversion;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import static org.elasticsearch.xpack.sql.tree.Location.EMPTY;
import static org.elasticsearch.xpack.sql.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.sql.type.DataType.BYTE;
import static org.elasticsearch.xpack.sql.type.DataType.DATE;
import static org.elasticsearch.xpack.sql.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.sql.type.DataType.FLOAT;
import static org.elasticsearch.xpack.sql.type.DataType.INTEGER;
import static org.elasticsearch.xpack.sql.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.sql.type.DataType.LONG;
import static org.elasticsearch.xpack.sql.type.DataType.NULL;
import static org.elasticsearch.xpack.sql.type.DataType.SHORT;
import static org.elasticsearch.xpack.sql.type.DataType.TEXT;
import static org.elasticsearch.xpack.sql.type.DataType.UNSUPPORTED;
import static org.elasticsearch.xpack.sql.type.DataTypeConversion.commonType;
import static org.elasticsearch.xpack.sql.type.DataTypeConversion.conversionFor;

public class DataTypeConversionTests extends ESTestCase {

    public void testConversionToString() {
        DataType to = KEYWORD;
        {
            DataTypeConversion.Conversion conversion = conversionFor(DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals("10.0", conversion.convert(10.0));
        }
        {
            DataTypeConversion.Conversion conversion = conversionFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals("1973-11-29T21:33:09.101Z", conversion.convert(new DateTime(123456789101L, DateTimeZone.UTC)));
            assertEquals("1966-02-02T02:26:50.899Z", conversion.convert(new DateTime(-123456789101L, DateTimeZone.UTC)));
        }
    }

    /**
     * Test conversion to long.
     */
    public void testConversionToLong() {
        DataType to = DataType.LONG;
        {
            Conversion conversion = DataTypeConversion.conversionFor(DataType.DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals(10L, conversion.convert(10.0));
            assertEquals(10L, conversion.convert(10.1));
            assertEquals(11L, conversion.convert(10.6));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert(Double.MAX_VALUE));
            assertEquals("[" + Double.MAX_VALUE + "] out of [Long] range", e.getMessage());
        }
        {
            Conversion conversion = DataTypeConversion.conversionFor(DataType.INTEGER, to);
            assertNull(conversion.convert(null));
            assertEquals(10L, conversion.convert(10));
            assertEquals(-134L, conversion.convert(-134));
        }
        {
            Conversion conversion = DataTypeConversion.conversionFor(DataType.BOOLEAN, to);
            assertNull(conversion.convert(null));
            assertEquals(1L, conversion.convert(true));
            assertEquals(0L, conversion.convert(false));
        }
        {
            Conversion conversion = conversionFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals(123456789101L, conversion.convert(new DateTime(123456789101L, DateTimeZone.UTC)));
            assertEquals(-123456789101L, conversion.convert(new DateTime(-123456789101L, DateTimeZone.UTC)));
        }
        {
            Conversion conversion = conversionFor(KEYWORD, to);
            assertNull(conversion.convert(null));
            assertEquals(1L, conversion.convert("1"));
            assertEquals(0L, conversion.convert("-0"));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("0xff"));
            assertEquals("cannot cast [0xff] to [Long]", e.getMessage());
        }
    }

    public void testConversionToDate() {
        DataType to = DataType.DATE;
        {
            Conversion conversion = DataTypeConversion.conversionFor(DataType.DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals(new DateTime(10L, DateTimeZone.UTC), conversion.convert(10.0));
            assertEquals(new DateTime(10L, DateTimeZone.UTC), conversion.convert(10.1));
            assertEquals(new DateTime(11L, DateTimeZone.UTC), conversion.convert(10.6));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert(Double.MAX_VALUE));
            assertEquals("[" + Double.MAX_VALUE + "] out of [Long] range", e.getMessage());
        }
        {
            Conversion conversion = DataTypeConversion.conversionFor(DataType.INTEGER, to);
            assertNull(conversion.convert(null));
            assertEquals(new DateTime(10L, DateTimeZone.UTC), conversion.convert(10));
            assertEquals(new DateTime(-134L, DateTimeZone.UTC), conversion.convert(-134));
        }
        {
            Conversion conversion = DataTypeConversion.conversionFor(DataType.BOOLEAN, to);
            assertNull(conversion.convert(null));
            assertEquals(new DateTime(1, DateTimeZone.UTC), conversion.convert(true));
            assertEquals(new DateTime(0, DateTimeZone.UTC), conversion.convert(false));
        }
        {
            Conversion conversion = conversionFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals(new DateTime(123456789101L, DateTimeZone.UTC), conversion.convert(new DateTime(123456789101L, DateTimeZone.UTC)));
            assertEquals(new DateTime(-123456789101L, DateTimeZone.UTC),
                conversion.convert(new DateTime(-123456789101L, DateTimeZone.UTC)));
        }
        {
            Conversion conversion = conversionFor(KEYWORD, to);
            assertNull(conversion.convert(null));

            assertEquals(new DateTime(1000L, DateTimeZone.UTC), conversion.convert("1970-01-01T00:00:01Z"));
            assertEquals(new DateTime(1483228800000L, DateTimeZone.UTC), conversion.convert("2017-01-01T00:00:00Z"));
            assertEquals(new DateTime(1483228800000L, DateTimeZone.UTC), conversion.convert("2017-01-01T00:00:00Z"));
            assertEquals(new DateTime(18000000L, DateTimeZone.UTC), conversion.convert("1970-01-01T00:00:00-05:00"));

            // double check back and forth conversion
            DateTime dt = DateTime.now(DateTimeZone.UTC);
            Conversion forward = DataTypeConversion.conversionFor(DataType.DATE, DataType.KEYWORD);
            Conversion back = DataTypeConversion.conversionFor(DataType.KEYWORD, DataType.DATE);
            assertEquals(dt, back.convert(forward.convert(dt)));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("0xff"));
            assertEquals("cannot cast [0xff] to [Date]:Invalid format: \"0xff\" is malformed at \"xff\"", e.getMessage());
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
            Conversion conversion = DataTypeConversion.conversionFor(DataType.INTEGER, DataType.DOUBLE);
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
            assertEquals(1.23456789101E11, (double) conversion.convert(new DateTime(123456789101L, DateTimeZone.UTC)), 0);
            assertEquals(-1.23456789101E11, (double) conversion.convert(new DateTime(-123456789101L, DateTimeZone.UTC)), 0);
        }
        {
            Conversion conversion = conversionFor(KEYWORD, to);
            assertNull(conversion.convert(null));
            assertEquals(1.0, (double) conversion.convert("1"), 0);
            assertEquals(0.0, (double) conversion.convert("-0"), 0);
            assertEquals(12.776, (double) conversion.convert("12.776"), 0.00001);
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("0xff"));
            assertEquals("cannot cast [0xff] to [Double]", e.getMessage());
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
            assertEquals(true, conversion.convert(new DateTime(123456789101L, DateTimeZone.UTC)));
            assertEquals(true, conversion.convert(new DateTime(-123456789101L, DateTimeZone.UTC)));
            assertEquals(false, conversion.convert(new DateTime(0L, DateTimeZone.UTC)));
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
            assertEquals("cannot cast [10] to [Boolean]", e.getMessage());
            e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("-1"));
            assertEquals("cannot cast [-1] to [Boolean]", e.getMessage());
            e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("0"));
            assertEquals("cannot cast [0] to [Boolean]", e.getMessage());
            e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("blah"));
            assertEquals("cannot cast [blah] to [Boolean]", e.getMessage());
            e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("Yes"));
            assertEquals("cannot cast [Yes] to [Boolean]", e.getMessage());
            e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("nO"));
            assertEquals("cannot cast [nO] to [Boolean]", e.getMessage());
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
            assertEquals("[" + Long.MAX_VALUE + "] out of [Int] range", e.getMessage());
        }
        {
            Conversion conversion = conversionFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals(12345678, conversion.convert(new DateTime(12345678L, DateTimeZone.UTC)));
            assertEquals(223456789, conversion.convert(new DateTime(223456789L, DateTimeZone.UTC)));
            assertEquals(-123456789, conversion.convert(new DateTime(-123456789L, DateTimeZone.UTC)));
            Exception e = expectThrows(SqlIllegalArgumentException.class,
                () -> conversion.convert(new DateTime(Long.MAX_VALUE, DateTimeZone.UTC)));
            assertEquals("[" + Long.MAX_VALUE + "] out of [Int] range", e.getMessage());
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
            assertEquals("[" + Integer.MAX_VALUE + "] out of [Short] range", e.getMessage());
        }
        {
            Conversion conversion = conversionFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals((short) 12345, conversion.convert(new DateTime(12345L, DateTimeZone.UTC)));
            assertEquals((short) -12345, conversion.convert(new DateTime(-12345L, DateTimeZone.UTC)));
            Exception e = expectThrows(SqlIllegalArgumentException.class,
                () -> conversion.convert(new DateTime(Integer.MAX_VALUE, DateTimeZone.UTC)));
            assertEquals("[" + Integer.MAX_VALUE + "] out of [Short] range", e.getMessage());
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
            assertEquals("[" + Short.MAX_VALUE + "] out of [Byte] range", e.getMessage());
        }
        {
            Conversion conversion = conversionFor(DATE, to);
            assertNull(conversion.convert(null));
            assertEquals((byte) 123, conversion.convert(new DateTime(123L, DateTimeZone.UTC)));
            assertEquals((byte) -123, conversion.convert(new DateTime(-123L, DateTimeZone.UTC)));
            Exception e = expectThrows(SqlIllegalArgumentException.class,
                () -> conversion.convert(new DateTime(Integer.MAX_VALUE, DateTimeZone.UTC)));
            assertEquals("[" + Integer.MAX_VALUE + "] out of [Byte] range", e.getMessage());
        }
    }

    public void testConversionToNull() {
        Conversion conversion = DataTypeConversion.conversionFor(DataType.DOUBLE, DataType.NULL);
        assertNull(conversion.convert(null));
        assertNull(conversion.convert(10.0));
    }

    public void testConversionFromNull() {
        Conversion conversion = DataTypeConversion.conversionFor(DataType.NULL, DataType.INTEGER);
        assertNull(conversion.convert(null));
        assertNull(conversion.convert(10));
    }

    public void testConversionToIdentity() {
        Conversion conversion = DataTypeConversion.conversionFor(DataType.INTEGER, DataType.INTEGER);
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
    }

    public void testEsDataTypes() {
        for (DataType type : DataType.values()) {
            assertEquals(type, DataType.fromEsType(type.esType));
        }
    }

    public void testConversionToUnsupported() {
        Exception e = expectThrows(SqlIllegalArgumentException.class,
            () -> conversionFor(INTEGER, UNSUPPORTED));
        assertEquals("cannot convert from [INTEGER] to [UNSUPPORTED]", e.getMessage());
    }

    public void testStringToIp() {
        Conversion conversion = DataTypeConversion.conversionFor(DataType.KEYWORD, DataType.IP);
        assertNull(conversion.convert(null));
        assertEquals("192.168.1.1", conversion.convert("192.168.1.1"));
        Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("10.1.1.300"));
        assertEquals("[10.1.1.300] is not a valid IPv4 or IPv6 address", e.getMessage());
    }

    public void testIpToString() {
        Conversion ipToString = DataTypeConversion.conversionFor(DataType.IP, DataType.KEYWORD);
        assertEquals("10.0.0.1", ipToString.convert(new Literal(EMPTY, "10.0.0.1", DataType.IP)));
        Conversion stringToIp = DataTypeConversion.conversionFor(DataType.KEYWORD, DataType.IP);
        assertEquals("10.0.0.1", ipToString.convert(stringToIp.convert(Literal.of(EMPTY, "10.0.0.1"))));
    }
}
