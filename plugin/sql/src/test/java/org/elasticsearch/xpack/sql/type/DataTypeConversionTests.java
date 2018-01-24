/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.type.DataTypeConversion.Conversion;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class DataTypeConversionTests extends ESTestCase {
    public void testConversionToString() {
        Conversion conversion = DataTypeConversion.conversionFor(DataType.DOUBLE, DataType.KEYWORD);
        assertNull(conversion.convert(null));
        assertEquals("10.0", conversion.convert(10.0));

        conversion = DataTypeConversion.conversionFor(DataType.DATE, DataType.KEYWORD);
        assertNull(conversion.convert(null));
        assertEquals("1970-01-01T00:00:00.000Z", conversion.convert(new DateTime(0, DateTimeZone.UTC)));
    }

    /**
     * Test conversion to a date or long. These are almost the same.
     */
    public void testConversionToLongOrDate() {
        DataType to = randomBoolean() ? DataType.LONG : DataType.DATE;
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
            assertEquals(1, conversion.convert(true));
            assertEquals(0, conversion.convert(false));
        }
        Conversion conversion = DataTypeConversion.conversionFor(DataType.KEYWORD, to);
        assertNull(conversion.convert(null));
        if (to == DataType.LONG) {
            assertEquals(1L, conversion.convert("1"));
            assertEquals(0L, conversion.convert("-0"));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("0xff"));
            assertEquals("cannot cast [0xff] to [Long]", e.getMessage());
        } else {
            // TODO we'd like to be able to optionally parse millis here I think....
            assertEquals(1000L, conversion.convert("1970-01-01T00:00:01Z"));
            assertEquals(1483228800000L, conversion.convert("2017-01-01T00:00:00Z"));
            assertEquals(18000000L, conversion.convert("1970-01-01T00:00:00-05:00"));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("0xff"));
            assertEquals("cannot cast [0xff] to [Date]:Invalid format: \"0xff\" is malformed at \"xff\"", e.getMessage());
        }
    }

    public void testConversionToDouble() {
        {
            Conversion conversion = DataTypeConversion.conversionFor(DataType.FLOAT, DataType.DOUBLE);
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
            Conversion conversion = DataTypeConversion.conversionFor(DataType.BOOLEAN, DataType.DOUBLE);
            assertNull(conversion.convert(null));
            assertEquals(1.0, (double) conversion.convert(true), 0);
            assertEquals(0.0, (double) conversion.convert(false), 0);
        }
        {
            Conversion conversion = DataTypeConversion.conversionFor(DataType.KEYWORD, DataType.DOUBLE);
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
            Conversion conversion = DataTypeConversion.conversionFor(DataType.FLOAT, DataType.BOOLEAN);
            assertNull(conversion.convert(null));
            assertEquals(true, conversion.convert(10.0f));
            assertEquals(true, conversion.convert(-10.0f));
            assertEquals(false, conversion.convert(0.0f));
        }
        {
            Conversion conversion = DataTypeConversion.conversionFor(DataType.INTEGER, DataType.BOOLEAN);
            assertNull(conversion.convert(null));
            assertEquals(true, conversion.convert(10));
            assertEquals(true, conversion.convert(-10));
            assertEquals(false, conversion.convert(0));
        }
        {
            Conversion conversion = DataTypeConversion.conversionFor(DataType.DOUBLE, DataType.BOOLEAN);
            assertNull(conversion.convert(null));
            assertEquals(true, conversion.convert(10.0));
            assertEquals(true, conversion.convert(-10.0));
            assertEquals(false, conversion.convert(0.0));
        }
        {
            Conversion conversion = DataTypeConversion.conversionFor(DataType.KEYWORD, DataType.BOOLEAN);
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
            Conversion conversion = DataTypeConversion.conversionFor(DataType.DOUBLE, DataType.INTEGER);
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
            Conversion conversion = DataTypeConversion.conversionFor(DataType.DOUBLE, DataType.SHORT);
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
            Conversion conversion = DataTypeConversion.conversionFor(DataType.DOUBLE, DataType.BYTE);
            assertNull(conversion.convert(null));
            assertEquals((byte) 10, conversion.convert(10.0));
            assertEquals((byte) 10, conversion.convert(10.1));
            assertEquals((byte) 11, conversion.convert(10.6));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert(Short.MAX_VALUE));
            assertEquals("[" + Short.MAX_VALUE + "] out of [Byte] range", e.getMessage());
        }
    }
}
