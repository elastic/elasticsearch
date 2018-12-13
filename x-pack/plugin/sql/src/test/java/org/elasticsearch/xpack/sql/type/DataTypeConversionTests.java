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
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.time.ZonedDateTime;

import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.dateTime;
import static org.elasticsearch.xpack.sql.tree.Location.EMPTY;
import static org.elasticsearch.xpack.sql.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.sql.type.DataType.BYTE;
import static org.elasticsearch.xpack.sql.type.DataType.DATE;
import static org.elasticsearch.xpack.sql.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.sql.type.DataType.FLOAT;
import static org.elasticsearch.xpack.sql.type.DataType.INTEGER;
import static org.elasticsearch.xpack.sql.type.DataType.IP;
import static org.elasticsearch.xpack.sql.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.sql.type.DataType.LONG;
import static org.elasticsearch.xpack.sql.type.DataType.NULL;
import static org.elasticsearch.xpack.sql.type.DataType.SHORT;
import static org.elasticsearch.xpack.sql.type.DataType.TEXT;
import static org.elasticsearch.xpack.sql.type.DataType.UNSUPPORTED;
import static org.elasticsearch.xpack.sql.type.DataType.fromTypeName;
import static org.elasticsearch.xpack.sql.type.DataType.values;
import static org.elasticsearch.xpack.sql.type.DataTypeConversion.commonType;
import static org.elasticsearch.xpack.sql.type.DataTypeConversion.conversionFor;


public class DataTypeConversionTests extends ESTestCase {
    public void testConversionToString() {
        Conversion conversion = conversionFor(DOUBLE, KEYWORD);
        assertNull(conversion.convert(null));
        assertEquals("10.0", conversion.convert(10.0));

        conversion = conversionFor(DATE, KEYWORD);
        assertNull(conversion.convert(null));
        assertEquals("1970-01-01T00:00:00.000Z", conversion.convert(dateTime(0)));
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
            assertEquals("[" + Double.MAX_VALUE + "] out of [Long] range", e.getMessage());
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
        Conversion conversion = conversionFor(KEYWORD, to);
        assertNull(conversion.convert(null));
        assertEquals(1L, conversion.convert("1"));
        assertEquals(0L, conversion.convert("-0"));
        Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("0xff"));
        assertEquals("cannot cast [0xff] to [Long]", e.getMessage());
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/35683")
    public void testConversionToDate() {
        DataType to = DATE;
        {
            Conversion conversion = conversionFor(DOUBLE, to);
            assertNull(conversion.convert(null));
            assertEquals(dateTime(10L), conversion.convert(10.0));
            assertEquals(dateTime(10L), conversion.convert(10.1));
            assertEquals(dateTime(11L), conversion.convert(10.6));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert(Double.MAX_VALUE));
            assertEquals("[" + Double.MAX_VALUE + "] out of [Long] range", e.getMessage());
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
        Conversion conversion = conversionFor(KEYWORD, to);
        assertNull(conversion.convert(null));

        assertEquals(dateTime(1000L), conversion.convert("1970-01-01T00:00:01Z"));
        assertEquals(dateTime(1483228800000L), conversion.convert("2017-01-01T00:00:00Z"));
        assertEquals(dateTime(18000000L), conversion.convert("1970-01-01T00:00:00-05:00"));
        
        // double check back and forth conversion
        ZonedDateTime dt = ZonedDateTime.now(DateUtils.UTC);
        Conversion forward = conversionFor(DATE, KEYWORD);
        Conversion back = conversionFor(KEYWORD, DATE);
        assertEquals(dt, back.convert(forward.convert(dt)));
        Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("0xff"));
        assertEquals("cannot cast [0xff] to [Date]:Invalid format: \"0xff\" is malformed at \"xff\"", e.getMessage());
    }

    public void testConversionToDouble() {
        {
            Conversion conversion = conversionFor(FLOAT, DOUBLE);
            assertNull(conversion.convert(null));
            assertEquals(10.0, (double) conversion.convert(10.0f), 0.00001);
            assertEquals(10.1, (double) conversion.convert(10.1f), 0.00001);
            assertEquals(10.6, (double) conversion.convert(10.6f), 0.00001);
        }
        {
            Conversion conversion = conversionFor(INTEGER, DOUBLE);
            assertNull(conversion.convert(null));
            assertEquals(10.0, (double) conversion.convert(10), 0.00001);
            assertEquals(-134.0, (double) conversion.convert(-134), 0.00001);
        }
        {
            Conversion conversion = conversionFor(BOOLEAN, DOUBLE);
            assertNull(conversion.convert(null));
            assertEquals(1.0, (double) conversion.convert(true), 0);
            assertEquals(0.0, (double) conversion.convert(false), 0);
        }
        {
            Conversion conversion = conversionFor(KEYWORD, DOUBLE);
            assertNull(conversion.convert(null));
            assertEquals(1.0, (double) conversion.convert("1"), 0);
            assertEquals(0.0, (double) conversion.convert("-0"), 0);
            assertEquals(12.776, (double) conversion.convert("12.776"), 0.00001);
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("0xff"));
            assertEquals("cannot cast [0xff] to [Double]", e.getMessage());
        }
    }

    public void testConversionToBoolean() {
        {
            Conversion conversion = conversionFor(FLOAT, BOOLEAN);
            assertNull(conversion.convert(null));
            assertEquals(true, conversion.convert(10.0f));
            assertEquals(true, conversion.convert(-10.0f));
            assertEquals(false, conversion.convert(0.0f));
        }
        {
            Conversion conversion = conversionFor(INTEGER, BOOLEAN);
            assertNull(conversion.convert(null));
            assertEquals(true, conversion.convert(10));
            assertEquals(true, conversion.convert(-10));
            assertEquals(false, conversion.convert(0));
        }
        {
            Conversion conversion = conversionFor(LONG, BOOLEAN);
            assertNull(conversion.convert(null));
            assertEquals(true, conversion.convert(10L));
            assertEquals(true, conversion.convert(-10L));
            assertEquals(false, conversion.convert(0L));
        }
        {
            Conversion conversion = conversionFor(DOUBLE, BOOLEAN);
            assertNull(conversion.convert(null));
            assertEquals(true, conversion.convert(10.0d));
            assertEquals(true, conversion.convert(-10.0d));
            assertEquals(false, conversion.convert(0.0d));
        }
        {
            Conversion conversion = conversionFor(KEYWORD, BOOLEAN);
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
        {
            Conversion conversion = conversionFor(DOUBLE, INTEGER);
            assertNull(conversion.convert(null));
            assertEquals(10, conversion.convert(10.0));
            assertEquals(10, conversion.convert(10.1));
            assertEquals(11, conversion.convert(10.6));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert(Long.MAX_VALUE));
            assertEquals("[" + Long.MAX_VALUE + "] out of [Int] range", e.getMessage());
        }
    }

    public void testConversionToShort() {
        {
            Conversion conversion = conversionFor(DOUBLE, SHORT);
            assertNull(conversion.convert(null));
            assertEquals((short) 10, conversion.convert(10.0));
            assertEquals((short) 10, conversion.convert(10.1));
            assertEquals((short) 11, conversion.convert(10.6));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert(Integer.MAX_VALUE));
            assertEquals("[" + Integer.MAX_VALUE + "] out of [Short] range", e.getMessage());
        }
    }

    public void testConversionToByte() {
        {
            Conversion conversion = conversionFor(DOUBLE, BYTE);
            assertNull(conversion.convert(null));
            assertEquals((byte) 10, conversion.convert(10.0));
            assertEquals((byte) 10, conversion.convert(10.1));
            assertEquals((byte) 11, conversion.convert(10.6));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert(Short.MAX_VALUE));
            assertEquals("[" + Short.MAX_VALUE + "] out of [Byte] range", e.getMessage());
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
        assertEquals(null, commonType(TEXT, KEYWORD));
        assertEquals(SHORT, commonType(SHORT, BYTE));
        assertEquals(FLOAT, commonType(BYTE, FLOAT));
        assertEquals(FLOAT, commonType(FLOAT, INTEGER));
        assertEquals(DOUBLE, commonType(DOUBLE, FLOAT));
    }

    public void testEsDataTypes() {
        for (DataType type : values()) {
            assertEquals(type, fromTypeName(type.esType));
        }
    }

    public void testConversionToUnsupported() {
            Exception e = expectThrows(SqlIllegalArgumentException.class,
                () -> conversionFor(INTEGER, UNSUPPORTED));
            assertEquals("cannot convert from [INTEGER] to [UNSUPPORTED]", e.getMessage());
    }

    public void testStringToIp() {
        Conversion conversion = conversionFor(KEYWORD, IP);
        assertNull(conversion.convert(null));
        assertEquals("192.168.1.1", conversion.convert("192.168.1.1"));
        Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("10.1.1.300"));
        assertEquals("[10.1.1.300] is not a valid IPv4 or IPv6 address", e.getMessage());
    }

    public void testIpToString() {
        Conversion ipToString = conversionFor(IP, KEYWORD);
        assertEquals("10.0.0.1", ipToString.convert(new Literal(EMPTY, "10.0.0.1", IP)));
        Conversion stringToIp = conversionFor(KEYWORD, IP);
        assertEquals("10.0.0.1", ipToString.convert(stringToIp.convert(Literal.of(EMPTY, "10.0.0.1"))));
    }
}
