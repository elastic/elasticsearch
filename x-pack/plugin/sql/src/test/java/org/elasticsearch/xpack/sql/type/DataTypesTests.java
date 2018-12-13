/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.test.ESTestCase;

import java.util.EnumSet;

import static org.elasticsearch.xpack.sql.type.DataType.DATE;
import static org.elasticsearch.xpack.sql.type.DataType.FLOAT;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_DAY;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_DAY_TO_HOUR;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_DAY_TO_MINUTE;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_DAY_TO_SECOND;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_HOUR;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_HOUR_TO_MINUTE;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_HOUR_TO_SECOND;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_MINUTE;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_MINUTE_TO_SECOND;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_MONTH;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_SECOND;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_YEAR;
import static org.elasticsearch.xpack.sql.type.DataType.INTERVAL_YEAR_TO_MONTH;
import static org.elasticsearch.xpack.sql.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.sql.type.DataType.LONG;
import static org.elasticsearch.xpack.sql.type.DataTypes.compatibleInterval;
import static org.elasticsearch.xpack.sql.type.DataTypes.isInterval;
import static org.elasticsearch.xpack.sql.type.DataTypes.metaSqlDataType;
import static org.elasticsearch.xpack.sql.type.DataTypes.metaSqlDateTimeSub;
import static org.elasticsearch.xpack.sql.type.DataTypes.metaSqlMaximumScale;
import static org.elasticsearch.xpack.sql.type.DataTypes.metaSqlMinimumScale;
import static org.elasticsearch.xpack.sql.type.DataTypes.metaSqlRadix;

public class DataTypesTests extends ESTestCase {

    public void testMetaDataType() {
        assertEquals(Integer.valueOf(9), metaSqlDataType(DATE));
        DataType t = randomDataTypeNoDate();
        assertEquals(t.sqlType.getVendorTypeNumber(), metaSqlDataType(t));
    }

    public void testMetaDateTypeSub() {
        assertEquals(Integer.valueOf(3), metaSqlDateTimeSub(DATE));
        assertEquals(Integer.valueOf(0), metaSqlDateTimeSub(randomDataTypeNoDate()));
    }

    public void testMetaMinimumScale() {
        assertEquals(Short.valueOf((short) 3), metaSqlMinimumScale(DATE));
        assertEquals(Short.valueOf((short) 0), metaSqlMinimumScale(LONG));
        assertEquals(Short.valueOf((short) 0), metaSqlMinimumScale(FLOAT));
        assertNull(metaSqlMinimumScale(KEYWORD));
    }

    public void testMetaMaximumScale() {
        assertEquals(Short.valueOf((short) 3), metaSqlMaximumScale(DATE));
        assertEquals(Short.valueOf((short) 0), metaSqlMaximumScale(LONG));
        assertEquals(Short.valueOf((short) FLOAT.defaultPrecision), metaSqlMaximumScale(FLOAT));
        assertNull(metaSqlMaximumScale(KEYWORD));
    }

    public void testMetaRadix() {
        assertNull(metaSqlRadix(DATE));
        assertNull(metaSqlRadix(KEYWORD));
        assertEquals(Integer.valueOf(10), metaSqlRadix(LONG));
        assertEquals(Integer.valueOf(2), metaSqlRadix(FLOAT));
    }


    // type checks
    public void testIsInterval() throws Exception {
        for (DataType dataType : EnumSet.range(INTERVAL_YEAR, INTERVAL_MINUTE_TO_SECOND)) {
            assertTrue(isInterval(dataType));
        }
    }

    public void testIntervalCompatibilityYearMonth() throws Exception {
        assertEquals(INTERVAL_YEAR_TO_MONTH, compatibleInterval(INTERVAL_YEAR, INTERVAL_MONTH));
        assertEquals(INTERVAL_YEAR_TO_MONTH, compatibleInterval(INTERVAL_YEAR, INTERVAL_YEAR_TO_MONTH));
        assertEquals(INTERVAL_YEAR_TO_MONTH, compatibleInterval(INTERVAL_MONTH, INTERVAL_YEAR));
        assertEquals(INTERVAL_YEAR_TO_MONTH, compatibleInterval(INTERVAL_MONTH, INTERVAL_YEAR_TO_MONTH));
    }

    public void testIntervalCompatibilityDayTime() throws Exception {
        assertEquals(INTERVAL_DAY_TO_HOUR, compatibleInterval(INTERVAL_DAY, INTERVAL_HOUR));
        assertEquals(INTERVAL_DAY_TO_HOUR, compatibleInterval(INTERVAL_DAY_TO_HOUR, INTERVAL_HOUR));
        assertEquals(INTERVAL_DAY_TO_MINUTE, compatibleInterval(INTERVAL_DAY, INTERVAL_MINUTE));
        assertEquals(INTERVAL_DAY_TO_MINUTE, compatibleInterval(INTERVAL_DAY_TO_HOUR, INTERVAL_HOUR_TO_MINUTE));
        assertEquals(INTERVAL_DAY_TO_MINUTE, compatibleInterval(INTERVAL_MINUTE, INTERVAL_DAY_TO_HOUR));
        assertEquals(INTERVAL_DAY_TO_MINUTE, compatibleInterval(INTERVAL_DAY, INTERVAL_DAY_TO_MINUTE));
        assertEquals(INTERVAL_DAY_TO_SECOND, compatibleInterval(INTERVAL_DAY, INTERVAL_SECOND));
        assertEquals(INTERVAL_DAY_TO_SECOND, compatibleInterval(INTERVAL_MINUTE, INTERVAL_DAY_TO_SECOND));

        assertEquals(INTERVAL_HOUR_TO_MINUTE, compatibleInterval(INTERVAL_MINUTE, INTERVAL_HOUR));
        assertEquals(INTERVAL_HOUR_TO_SECOND, compatibleInterval(INTERVAL_SECOND, INTERVAL_HOUR));
        assertEquals(INTERVAL_HOUR_TO_SECOND, compatibleInterval(INTERVAL_SECOND, INTERVAL_HOUR_TO_MINUTE));
        assertEquals(INTERVAL_HOUR_TO_SECOND, compatibleInterval(INTERVAL_SECOND, INTERVAL_HOUR_TO_MINUTE));

        assertEquals(INTERVAL_MINUTE_TO_SECOND, compatibleInterval(INTERVAL_SECOND, INTERVAL_MINUTE));
    }

    public void testIncompatibleInterval() throws Exception {
        assertNull(compatibleInterval(INTERVAL_YEAR, INTERVAL_SECOND));
        assertNull(compatibleInterval(INTERVAL_YEAR, INTERVAL_DAY_TO_HOUR));
        assertNull(compatibleInterval(INTERVAL_HOUR, INTERVAL_MONTH));
        assertNull(compatibleInterval(INTERVAL_MINUTE_TO_SECOND, INTERVAL_MONTH));
    }

    private DataType randomDataTypeNoDate() {
        return randomValueOtherThan(DataType.DATE, () -> randomFrom(DataType.values()));
    }
}