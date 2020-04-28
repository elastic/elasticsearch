/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.FLOAT;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.sql.expression.literal.interval.Intervals.compatibleInterval;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.DATE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_DAY;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_DAY_TO_HOUR;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_DAY_TO_MINUTE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_DAY_TO_SECOND;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_HOUR;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_HOUR_TO_MINUTE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_HOUR_TO_SECOND;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_MINUTE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_MINUTE_TO_SECOND;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_MONTH;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_SECOND;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_YEAR;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_YEAR_TO_MONTH;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.TIME;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.defaultPrecision;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.isInterval;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.metaSqlDataType;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.metaSqlDateTimeSub;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.metaSqlMaximumScale;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.metaSqlMinimumScale;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.metaSqlRadix;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.sqlType;

public class SqlDataTypesTests extends ESTestCase {

    public void testMetadataType() {
        assertEquals(Integer.valueOf(91), metaSqlDataType(DATE));
        assertEquals(Integer.valueOf(92), metaSqlDataType(TIME));
        assertEquals(Integer.valueOf(9), metaSqlDataType(DATETIME));
        DataType t = randomDataTypeNoDateTime();
        assertEquals(sqlType(t).getVendorTypeNumber(), metaSqlDataType(t));
    }

    public void testMetaDateTypeSub() {
        assertEquals(Integer.valueOf(1), metaSqlDateTimeSub(DATE));
        assertEquals(Integer.valueOf(2), metaSqlDateTimeSub(TIME));
        assertEquals(Integer.valueOf(3), metaSqlDateTimeSub(DATETIME));
        assertEquals(Integer.valueOf(0), metaSqlDateTimeSub(randomDataTypeNoDateTime()));
    }

    public void testMetaMinimumScale() {
        assertNull(metaSqlMinimumScale(DATE));
        assertEquals(Short.valueOf((short) 3), metaSqlMinimumScale(TIME));
        assertEquals(Short.valueOf((short) 3), metaSqlMinimumScale(DATETIME));
        assertEquals(Short.valueOf((short) 0), metaSqlMinimumScale(LONG));
        assertEquals(Short.valueOf((short) defaultPrecision(FLOAT)), metaSqlMaximumScale(FLOAT));
        assertNull(metaSqlMinimumScale(KEYWORD));
    }

    public void testMetaMaximumScale() {
        assertNull(metaSqlMinimumScale(DATE));
        assertEquals(Short.valueOf((short) 3), metaSqlMinimumScale(TIME));
        assertEquals(Short.valueOf((short) 3), metaSqlMaximumScale(DATETIME));
        assertEquals(Short.valueOf((short) 0), metaSqlMaximumScale(LONG));
        assertEquals(Short.valueOf((short) defaultPrecision(FLOAT)), metaSqlMaximumScale(FLOAT));
        assertNull(metaSqlMaximumScale(KEYWORD));
    }

    public void testMetaRadix() {
        assertNull(metaSqlRadix(DATETIME));
        assertNull(metaSqlRadix(KEYWORD));
        assertEquals(Integer.valueOf(10), metaSqlRadix(LONG));
        assertEquals(Integer.valueOf(2), metaSqlRadix(FLOAT));
    }


    // type checks
    public void testIsInterval() {
        for (DataType dataType : asList(INTERVAL_YEAR,
                INTERVAL_MONTH,
                INTERVAL_DAY,
                INTERVAL_HOUR,
                INTERVAL_MINUTE,
                INTERVAL_SECOND,
                INTERVAL_YEAR_TO_MONTH,
                INTERVAL_DAY_TO_HOUR,
                INTERVAL_DAY_TO_MINUTE,
                INTERVAL_DAY_TO_SECOND,
                INTERVAL_HOUR_TO_MINUTE,
                INTERVAL_HOUR_TO_SECOND,
                INTERVAL_MINUTE_TO_SECOND)) {
            assertTrue(dataType + " is not an interval", isInterval(dataType));
        }
    }

    public void testIntervalCompatibilityYearMonth() {
        assertEquals(INTERVAL_YEAR_TO_MONTH, compatibleInterval(INTERVAL_YEAR, INTERVAL_MONTH));
        assertEquals(INTERVAL_YEAR_TO_MONTH, compatibleInterval(INTERVAL_YEAR, INTERVAL_YEAR_TO_MONTH));
        assertEquals(INTERVAL_YEAR_TO_MONTH, compatibleInterval(INTERVAL_MONTH, INTERVAL_YEAR));
        assertEquals(INTERVAL_YEAR_TO_MONTH, compatibleInterval(INTERVAL_MONTH, INTERVAL_YEAR_TO_MONTH));
    }

    public void testIntervalCompatibilityDayTime() {
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

    public void testIncompatibleInterval() {
        assertNull(compatibleInterval(INTERVAL_YEAR, INTERVAL_SECOND));
        assertNull(compatibleInterval(INTERVAL_YEAR, INTERVAL_DAY_TO_HOUR));
        assertNull(compatibleInterval(INTERVAL_HOUR, INTERVAL_MONTH));
        assertNull(compatibleInterval(INTERVAL_MINUTE_TO_SECOND, INTERVAL_MONTH));
    }

    public void testEsToDataType() {
        List<String> types = new ArrayList<>(Arrays.asList("null", "boolean", "bool",
                "byte", "tinyint",
                "short", "smallint",
                "integer",
                "long", "bigint",
                "double", "real",
                "half_float", "scaled_float", "float",
                "decimal", "numeric",
                "keyword", "text", "varchar",
                "date", "datetime", "timestamp",
                "binary", "varbinary",
                "ip",
                "interval_year", "interval_month", "interval_year_to_month",
                "interval_day", "interval_hour", "interval_minute", "interval_second",
                "interval_day_to_hour", "interval_day_to_minute", "interval_day_to_second",
                "interval_hour_to_minute", "interval_hour_to_second",
                "interval_minute_to_second"));

        types.addAll(SqlDataTypes.types().stream()
                .filter(DataTypes::isPrimitive)
                .map(DataType::typeName)
               .collect(toList()));
        String type = randomFrom(types.toArray(new String[0]));
        DataType dataType = SqlDataTypes.fromSqlOrEsType(type);
        assertNotNull("cound not find " + type, dataType);
    }

    private DataType randomDataTypeNoDateTime() {
        return randomValueOtherThanMany(SqlDataTypes::isDateOrTimeBased, () -> randomFrom(SqlDataTypes.types()));
    }
}
