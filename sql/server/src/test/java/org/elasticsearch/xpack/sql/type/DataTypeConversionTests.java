/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.type.DataTypeConversion.Conversion;

public class DataTypeConversionTests extends ESTestCase {
    public void testConversionToString() {
        Conversion conversion = DataTypeConversion.conversionFor(new DoubleType(true), KeywordType.DEFAULT);
        assertNull(conversion.convert(null));
        assertEquals("10.0", conversion.convert(10.0));

        conversion = DataTypeConversion.conversionFor(new DateType(true), KeywordType.DEFAULT);
        assertNull(conversion.convert(null));
        assertEquals("1970-01-01T00:00:00Z", conversion.convert(0));
    }

    /**
     * Test conversion to a date or long. These are almost the same. 
     */
    public void testConversionToLongOrDate() {
        DataType to = randomBoolean() ? new LongType(true) : new DateType(true);
        {
            Conversion conversion = DataTypeConversion.conversionFor(new DoubleType(true), to);
            assertNull(conversion.convert(null));
            assertEquals(10L, conversion.convert(10.0));
            assertEquals(10L, conversion.convert(10.1));
            assertEquals(11L, conversion.convert(10.6));
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert(Double.MAX_VALUE));
            assertEquals("[" + Double.MAX_VALUE + "] out of [Long] range", e.getMessage());
        }
        {
            Conversion conversion = DataTypeConversion.conversionFor(new IntegerType(true), to);
            assertNull(conversion.convert(null));
            assertEquals(10L, conversion.convert(10));
            assertEquals(-134L, conversion.convert(-134));
        }
        {
            Conversion conversion = DataTypeConversion.conversionFor(new BooleanType(true), to);
            assertNull(conversion.convert(null));
            assertEquals(1, conversion.convert(true));
            assertEquals(0, conversion.convert(false));
        }
        Conversion conversion = DataTypeConversion.conversionFor(KeywordType.DEFAULT, to);
        assertNull(conversion.convert(null));
        if (to instanceof LongType) {
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
            assertEquals("cannot cast [0xff] to [Date]: Invalid format: \"0xff\" is malformed at \"xff\"", e.getMessage());
        }
    }

    public void testConversionToDouble() {
        {
            Conversion conversion = DataTypeConversion.conversionFor(new FloatType(true), new DoubleType(true));
            assertNull(conversion.convert(null));
            assertEquals(10.0, (double) conversion.convert(10.0f), 0.00001);
            assertEquals(10.1, (double) conversion.convert(10.1f), 0.00001);
            assertEquals(10.6, (double) conversion.convert(10.6f), 0.00001);
        }
        {
            Conversion conversion = DataTypeConversion.conversionFor(new IntegerType(true), new DoubleType(true));
            assertNull(conversion.convert(null));
            assertEquals(10.0, (double) conversion.convert(10), 0.00001);
            assertEquals(-134.0, (double) conversion.convert(-134), 0.00001);
        }
        {
            Conversion conversion = DataTypeConversion.conversionFor(new BooleanType(true), new DoubleType(true));
            assertNull(conversion.convert(null));
            assertEquals(1.0, (double) conversion.convert(true), 0);
            assertEquals(0.0, (double) conversion.convert(false), 0);
        }
        {
            Conversion conversion = DataTypeConversion.conversionFor(KeywordType.DEFAULT, new DoubleType(true));
            assertNull(conversion.convert(null));
            assertEquals(1.0, (double) conversion.convert("1"), 0);
            assertEquals(0.0, (double) conversion.convert("-0"), 0);
            assertEquals(12.776, (double) conversion.convert("12.776"), 0.00001);
            Exception e = expectThrows(SqlIllegalArgumentException.class, () -> conversion.convert("0xff"));
            assertEquals("cannot cast [0xff] to [Double]", e.getMessage());
        }
    }
}
